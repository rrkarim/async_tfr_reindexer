import asyncio
import socket
import contextlib
import dataclasses
import struct
import weakref
from typing import Optional, Dict


def _finalize_callback(
    server_sock: socket.socket,
    worker_sock: socket.socket,
    transport,
    loop: asyncio.BaseEventLoop,
):
    if loop.is_running():
        with contextlib.closing(transport):
            with contextlib.closing(server_sock):
                worker_sock.shutdown(socket.SHUT_RDWR)
            with contextlib.closing(worker_sock):
                worker_sock.shutdown(socket.SHUT_RDWR)


class SocketSignalBusError(Exception):
    pass


class SocketSignalBus:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        read_size: int = 65536,
    ):
        """
        This code is very similar to what happens inside uvloop when processing ThreadPoolExecutor
        """
        self._reader: asyncio.StreamReader = None
        self._initialized = False
        self._lock = asyncio.Lock(loop=loop)
        self.worker_sock, self.server_sock = socket.socketpair(
            family=socket.AF_UNIX, type=socket.SOCK_STREAM
        )

        self._workers_to_futures = {}
        self._previous_tail = b""
        self._worker = None
        self._ex = None
        self._read_size = read_size
        self._loop = loop

    def _raise_if_dead(self):
        if self._ex is not None:
            raise self._ex

    async def _background_reader(self):
        tail: Optional[bytes] = None
        while True:
            # Read by large blobs to turn on several futures on single loop tick
            data = await self._reader.read(self._read_size)
            data_view = memoryview(data)  # to avoid bytecopy
            start = 0
            if tail is not None:
                yield tail + bytes(data_view[: 4 - len(tail)])
                start = 4 - len(tail)

            yield data_view[start:]
            remainer = (len(data) - start) % 4
            tail = data[-remainer:] if remainer > 0 else None

    async def _background_worker(self):
        try:
            unpacker = struct.Struct("=i")
            async for data_blob in self._background_reader():
                for i in range(0, len(data_blob), 4):
                    worker_id = unpacker.unpack(data_blob[i : i + 4])[0]
                    future = self._workers_to_futures.pop(worker_id & 0x7FFFFFFF)
                    if not future.done():
                        if worker_id & 1 << 31:
                            # Error regime
                            future.set_exception(SocketSignalBusError())
                        else:
                            future.set_result(1)
        except BaseException as ex:
            self._ex = ex
            try:
                for f in self._workers_to_futures.values():
                    if not f.done():
                        f.set_exception(ex)
            finally:
                self._workers_to_futures.clear()
            raise

    async def get_callback(
        self, worker_id: int, loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> asyncio.Future:
        self._raise_if_dead()
        if not self._initialized:
            await self._init()
        loop = asyncio.get_running_loop() if loop is None else loop
        future = loop.create_future()
        if worker_id in self._workers_to_futures:
            msg = f"Tried to push {worker_id} to signal bus, but previous future has not been yielded"
            raise RuntimeError(msg)
        self._workers_to_futures[worker_id] = future
        return future

    async def _init(self):
        async with self._lock:
            if not self._initialized:
                self._reader, transport = await asyncio.open_unix_connection(
                    sock=self.server_sock
                )

                loop = asyncio.get_running_loop()
                self._worker = loop.create_task(self._background_worker())

                weakref.finalize(
                    self,
                    _finalize_callback,
                    server_sock=self.server_sock,
                    worker_sock=self.worker_sock,
                    transport=transport,
                    loop=loop,
                )

                self._initialized = True


_LOOP_TO_BUS: Dict[asyncio.AbstractEventLoop, SocketSignalBus] = {}
_INSTANCE_REGISTRY = weakref.WeakKeyDictionary()
INSTANCE_ID = 0


"""
We don't use base class here because sometimes (e.g. in TensorPipe) sending and receiving
instances may be very different entities, even in different processes
"""


def _get_signal_bus(
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> SocketSignalBus:
    loop = asyncio.get_running_loop() if loop is None else loop
    global _LOOP_TO_BUS
    if loop not in _LOOP_TO_BUS:
        _LOOP_TO_BUS[loop] = SocketSignalBus(loop=loop)
    return _LOOP_TO_BUS[loop]


def _get_signal_bus_registry_id(self: object) -> int:
    global _INSTANCE_REGISTRY
    if self not in _INSTANCE_REGISTRY:
        global INSTANCE_ID
        if INSTANCE_ID & 1 << 31:
            raise ValueError(f"Reached max bus instance id {INSTANCE_ID}")
        _INSTANCE_REGISTRY[self] = INSTANCE_ID
        INSTANCE_ID += 1
    return _INSTANCE_REGISTRY[self]


def _get_signal_bus_socket(loop: Optional[asyncio.AbstractEventLoop] = None):
    return _get_signal_bus(loop=loop).worker_sock


@dataclasses.dataclass
class SignalBusRegisterDescriptor:
    future: asyncio.Future
    sock: socket.socket
    instance_id: int


async def get_signal_bus_future(
    self, loop: Optional[asyncio.AbstractEventLoop] = None
) -> SignalBusRegisterDescriptor:
    bus_instance_id = _get_signal_bus_registry_id(self=self)
    loop = asyncio.get_running_loop() if loop is None else loop
    signal_bus = _get_signal_bus(loop=loop)

    future = await signal_bus.get_callback(worker_id=bus_instance_id, loop=loop)

    return SignalBusRegisterDescriptor(
        future=future,
        sock=_get_signal_bus_socket(loop=loop),
        instance_id=bus_instance_id,
    )


__all__ = ["get_signal_bus_future"]
