import functools
import inspect
from typing import Tuple, Optional, Any, Dict

import libpoolparser
from socket_signal_bus import get_signal_bus_future, SocketSignalBusError


class AlreadyExecutingError(Exception):
    pass


@functools.lru_cache(maxsize=None)
def _cached_getmodule(fn):
    return inspect.getmodule(fn)


class cpp_threaded_worker:
    def __init__(
        self, name: Optional[str] = None, thread_pools: Optional[Dict[Any, Any]] = None
    ):
        self._executing = False
        self._name = name

        # Currently, thread handles and run_functor_in_cpp are per-library calls
        self._thread_handle = {} if thread_pools is None else thread_pools.copy()

    @property
    def is_executing(self):
        return self._executing

    def _get_callers(self, fn) -> Tuple[Any, Any]:
        # TODO: investigate how to make one shard library for functors from other libraries
        mod = _cached_getmodule(fn)
        if mod not in self._thread_handle:
            self._thread_handle[mod] = getattr(mod, "ThreadPoolHandle")()
        return self._thread_handle[mod], getattr(mod, "run_functor_in_cpp")

    async def __call__(self, fn):
        if self._executing:
            raise AlreadyExecutingError
        self._executing = True

        thread_pool, runner = self._get_callers(type(fn))
        res = await get_signal_bus_future(self=self)
        runner(thread_pool, fn, res.sock.fileno(), res.instance_id)
        try:
            await res.future
        except SocketSignalBusError:
            msg = libpoolparser.get_thread_bridge_error_message(res.instance_id)
            raise RuntimeError(msg)
        finally:
            self._executing = False
