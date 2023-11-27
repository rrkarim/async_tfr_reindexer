import io
import os
import gzip
import asyncio
import xxhash
import dataclasses
import collections
import libpoolparser
from functools import partial

from cpp_async_threaded import cpp_threaded_worker
from extendable_stream import BaseAsyncExtendableStream
from typing import Mapping, AsyncGenerator, Optional, AsyncIterable, List, Any, Set

import aiohttp
from gcloud.aio.storage import Storage

# TODO: test uvloop


def hash_with_seed(input_string, seed=0):
    input_bytes = input_string.encode("utf-8")
    hash_int = xxhash.xxh64(input_bytes, seed=seed).intdigest()
    return hash_int


@dataclasses.dataclass
class WaitTimeCounter:
    wait_time: float = 0


def _thread_worker(x):
    return cpp_threaded_worker() if x is None else x


def _check_file_in_output_path(file_name: str, output_file_lists: Set[str]) -> bool:
    return file_name in output_file_lists


async def async_file_name_generator(
    client,
    bucket_name: str,
    prefix_path: str,
    output_prefix_path: str,
    mode_value: int,
    mode_frac: int,
):
    bucket = client.get_bucket(bucket_name)
    # TODO: match_glob could be cleaner

    output_blobs = await bucket.list_blobs(prefix=output_prefix_path)
    output_blobs = set([blob[len(output_prefix_path) + 1 :] for blob in output_blobs])

    blobs = await bucket.list_blobs(prefix=prefix_path)
    for file_name in [blob[len(prefix_path) + 1 :] for blob in blobs]:
        if hash_with_seed(
            file_name, seed=13
        ) % mode_value == mode_frac and not _check_file_in_output_path(
            file_name, output_blobs
        ):
            # check if file already exists in the output directory
            yield file_name


class AsyncSortedExtendableStream(BaseAsyncExtendableStream):
    def __init__(self, semaphore: asyncio.Semaphore):
        super(AsyncSortedExtendableStream, self).__init__()
        self._records = collections.deque()
        self._loop = asyncio.get_running_loop()
        self._semaphore = semaphore

    def _async_iterator_put_impl(self, rec):
        self._records.append(rec)

    async def _async_iterator_next_impl(self):
        self._semaphore.release()
        return self._records.popleft()

    def _empty_impl(self):
        return len(self._records) == 0

    # Small additional hack to estimate wait time

    async def __anext__(self):
        wait_time = 0
        # Don't count "fake" time when we already have records
        if not len(self._records):
            start = self._loop.time()
            rec = await super(AsyncSortedExtendableStream, self).__anext__()
            wait_time = self._loop.time() - start
        else:
            rec = await super(AsyncSortedExtendableStream, self).__anext__()
        return wait_time, rec


async def async_parser(
    parser,
    bucket_name: str,
    prefix_path: str,
    output_prefix: str,
    mode_value: int = 1,
    mode_frac: int = 0,
    wait_thread: Optional[cpp_threaded_worker] = None,
) -> AsyncGenerator[Any, None]:
    wait_thread = _thread_worker(wait_thread)

    semaphore = asyncio.Semaphore(5)
    records_stream = AsyncSortedExtendableStream(semaphore)

    handlers_mp = {}

    def decompress(compressed_data):
        with io.BytesIO(compressed_data) as bytes_io:
            with gzip.open(bytes_io, "rb") as f:
                decompressed_data = f.read()
            return decompressed_data

    def compress(decompressed_data):
        with io.BytesIO() as bytes_io:
            with gzip.GzipFile(fileobj=bytes_io, mode="wb") as f:
                f.write(decompressed_data)
            compressed_data = bytes_io.getvalue()
        return compressed_data

    async def _job_assigner() -> None:
        try:
            async with aiohttp.ClientSession() as session:
                client = Storage(session=session)
                assign_thread = _thread_worker(None)
                index = 0
                async for file_name in async_file_name_generator(
                    client,
                    bucket_name,
                    prefix_path,
                    output_prefix,
                    mode_value,
                    mode_frac,
                ):
                    output = await client.download(
                        bucket_name, os.path.join(prefix_path, file_name), timeout=150
                    )

                    # TODO: we better use c++ threads here -- too lazy
                    output = await loop.run_in_executor(
                        None, partial(decompress, output)
                    )

                    handlers_mp[index] = (
                        libpoolparser.BatchedStringHandle(output),
                        file_name,
                    )
                    job_assigner = libpoolparser.TFRParserJobAssigner(
                        parser,
                        handlers_mp[index][0],
                        index,
                    )
                    index += 1
                    await assign_thread(job_assigner)
                parser.stop()
        except BaseException as ex:
            records_stream.throw(ex)
            raise

    async def _records_yielder():
        nonlocal semaphore
        nonlocal handlers_mp
        async with aiohttp.ClientSession() as session:
            client = Storage(session=session)
            try:
                while True:
                    waiter = libpoolparser.TFRParserWaiter(parser)
                    await wait_thread(waiter)
                    cur_rec_count = waiter.getRecordsCount()
                    if cur_rec_count < 0:
                        break
                    for _ in range(cur_rec_count):
                        record_index = parser.next()
                        records_stream.put(record_index)
                        file_name = handlers_mp[record_index][1]
                        compressed_data = await loop.run_in_executor(
                            None,
                            partial(
                                compress, handlers_mp[record_index][0].getOutString()
                            ),
                        )
                        _ = await client.upload(
                            bucket_name,
                            os.path.join(output_prefix, file_name),
                            compressed_data,
                        )
                        # TODO: assert for status
                        del handlers_mp[record_index]
                        await semaphore.acquire()
                records_stream.stop()
            except BaseException as ex:
                records_stream.throw(ex)
                raise

    loop = asyncio.get_running_loop()

    # check assert above!
    assigner = loop.create_task(_job_assigner())
    yielder = loop.create_task(_records_yielder())

    async for wait_time, record in records_stream:
        yield record

    await asyncio.gather(assigner, yielder)
