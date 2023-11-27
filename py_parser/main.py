import os
from tqdm.asyncio import tqdm
import asyncio
import aiohttp
import argparse

from google.cloud import storage
from gcloud.aio.storage import Storage

import libpoolparser
from parser import async_parser, hash_with_seed

parser = argparse.ArgumentParser(description="")
parser.add_argument(
    "--bucket-name", type=str, default="sc-ranking-prod-compiled-pipelines-us-central1"
)
parser.add_argument(
    "--prefix-path",
    type=str,
    default="tfrecords/megalith-v2-cold-start-1hr-video/span-1698694200/ver-1/Split-train/",
)
parser.add_argument(
    "--output-prefix",
    type=str,
    default="tfrecords/megalith-v2-cold-start-1hr-video/span-1698694200/ver-1/Split-train-transformed-v2",
)
parser.add_argument("--mode-value", type=int, default=1)
parser.add_argument("--mode-frac", type=int, default=0)
parser.add_argument("--num-parsers", type=int, default=5)
# with no batching no point of having yield > parser threads
parser.add_argument("--yield-size", type=int, default=10)
# post_id mapping vocab
parser.add_argument("--gcs-forward-vocab-path", type=str)
parser.add_argument("--gcs-backward-vocab-path", type=str)
args = parser.parse_args()


async def _get_filenames_len(bucket_name, prefix_path, mode_value, mode_frac):
    async with aiohttp.ClientSession() as session:
        client = Storage(session=session)
        bucket = client.get_bucket(bucket_name)
        # TODO: match_glob could be cleaner
        blobs = await bucket.list_blobs(prefix=prefix_path)
        return len(
            [
                True
                for blob in blobs
                if hash_with_seed(blob[len(prefix_path) :], seed=13) % mode_value
                == mode_frac
            ]
        )


def _list_blobs(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    return blobs


def _prepare_vocab_local_files(gcs_vocab_path: str, local_path: str):
    gcs_vocab_path = gcs_vocab_path[5:]
    gcs_dir_splits = gcs_vocab_path.split("/")
    bucket_name, prefix = gcs_dir_splits[0], "/".join(gcs_dir_splits[1:])
    blobs = _list_blobs(bucket_name, prefix)
    os.makedirs(local_path, exist_ok=True)
    for blob in blobs:
        local_file_path = os.path.join(local_path, blob.name.split("/")[-1])
        if os.path.exists(local_file_path) or "user_id" in local_file_path:
            print(f"{local_file_path} already exists")
            continue
        blob.download_to_filename(local_file_path)


def dummy_parser(
    gcs_forward_vocab_path: str,
    gcs_backward_vocab_path: str,
    num_parsers: int = 5,
    yield_size: int = 10,
):
    # sync code
    forward_local_dir, backward_local_dir = "forward_vocabs", "backward_vocabs"

    _prepare_vocab_local_files(gcs_forward_vocab_path, forward_local_dir)
    _prepare_vocab_local_files(gcs_backward_vocab_path, backward_local_dir)

    parser = libpoolparser.TFRPythonParser(
        libpoolparser.ThreadPoolHandle(num_parsers),
        [
            str(os.path.join(forward_local_dir, _))
            for _ in os.listdir(forward_local_dir)
        ],
        [
            str(os.path.join(backward_local_dir, _))
            for _ in os.listdir(backward_local_dir)
        ],
        yield_size,
    )
    parser.start()
    return parser


async def _run_parser():
    parser = dummy_parser(
        gcs_forward_vocab_path=args.gcs_forward_vocab_path,
        gcs_backward_vocab_path=args.gcs_backward_vocab_path,
        num_parsers=args.num_parsers,
        yield_size=args.yield_size,
    )
    iterator = async_parser(
        parser=parser,
        bucket_name=args.bucket_name,
        prefix_path=args.prefix_path,
        output_prefix=args.output_prefix,
        mode_value=args.mode_value,
        mode_frac=args.mode_frac,
    )

    filenames_len = await _get_filenames_len(
        args.bucket_name, args.prefix_path, args.mode_value, args.mode_frac
    )
    records = [r async for r in tqdm(iterator, total=filenames_len)]
    for r in records:
        pass


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(_run_parser())
    finally:
        loop.close()
