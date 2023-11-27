import asyncio
import contextlib


class StoppedExtendableStream(Exception):
    pass


class FinishedExtendableStream(Exception):
    pass


async def _wait_and_cancel_pending(fs, return_when=asyncio.FIRST_COMPLETED):
    _, pending = await asyncio.wait(fs, return_when=return_when)
    for f in pending:
        f.cancel()


class BaseAsyncExtendableStream:
    """
    This Stream interface allow us to put new items to stream, stop stream, cancel and wait
    Also, this stream provides additional abstraction level: it accepts awaitable futures, not the data itself

    Stop means that we tell streams that there will be no data. It's some kind of EOF
    Cancel means that we terminate stream and never read data from it.
    """

    def __init__(self):
        super(BaseAsyncExtendableStream, self).__init__()
        self._stop_flag = False
        self._finish_flag = False
        self._ex = None
        self._anext_waiter = None

        # Controlling events
        self._available_event = asyncio.Event()
        self._terminated_event = asyncio.Event()
        self._stopped_event = asyncio.Event()

    #####################################################
    #                    USER API                       #
    #####################################################

    def __aiter__(self):
        return self

    #####################################################
    #                   SYSTEM API                      #
    #####################################################

    def put(self, *data):
        """
        Put new data to stream.
        """
        self._raise_if_terminated()
        self._raise_if_finished()
        self._raise_if_stopped()
        with self._throw_on_error_ctx():
            self._async_iterator_put_impl(*data)
            self._available_event.set()

    # Stream interrupt methods. All can be called only once

    def stop(self) -> None:
        """
        Stop extending the stream - forbid the put() call. This action is irreversible
        """
        self._raise_if_terminated()
        self._raise_if_finished()
        self._raise_if_stopped()
        self._stop_flag = True
        self._stopped_event.set()

    def throw(self, ex: BaseException) -> None:
        """
        Throw an exception to the stream. Forbid put() and __anext__().
        This action is irreversible and the exception cannot be changed.
        Can be called on stopped stream to forbid yielding rest values and immediately exit
        """
        self._raise_if_terminated()
        self._raise_if_finished()
        self._ex = ex
        self._stop_flag = True
        self._terminated_event.set()

    def cancel(self) -> None:
        """
        Cancel the stream. Short-hand for throwing CancelledError
        """
        self.throw(asyncio.CancelledError())

    # INFO methods. Work always

    def done(self):
        return self.finished() or self.terminated()

    def finished(self):
        return self._finish_flag

    def terminated(self):
        return self._ex is not None

    def stopped(self):
        return self._stop_flag

    def get_exception(self):
        return self._ex

    #####################################################
    #                  IMPLEMENTATION                   #
    #####################################################

    @contextlib.contextmanager
    def _throw_on_error_ctx(self):
        try:
            yield
        except BaseException as ex:
            self.throw(ex)
            raise

    def _async_iterator_put_impl(self, *data):
        raise NotImplementedError

    async def _async_iterator_next_impl(self):
        raise NotImplementedError

    def _empty_impl(self) -> bool:
        raise NotImplementedError

    def __empty_impl(self) -> bool:
        with self._throw_on_error_ctx():
            return self._empty_impl()

    def _raise_if_stopped(self):
        if self._stop_flag:
            raise StoppedExtendableStream("{} has already been stopped".format(self))

    def _raise_if_terminated(self):
        if self._ex is not None:
            raise self._ex

    def _raise_if_finished(self):
        if self._finish_flag:
            raise FinishedExtendableStream("{} has already finished".format(self))

    def _finish_if_needed(self):
        if self._stop_flag and self.__empty_impl():
            self._finish_flag = True
            raise StopAsyncIteration

    # NOTHROW: this function does not need in a throw context shield
    # we use __empty_impl which is already shielded
    # all other code here is safe from user implementation
    async def _anext_wait(self):
        # immediately try to die
        self._raise_if_terminated()
        self._finish_if_needed()
        to_wait = [
            asyncio.ensure_future(self._terminated_event.wait()),
            asyncio.ensure_future(self._available_event.wait()),
        ]
        # The only case when we need to awake on stop is stop event + empty. We must raise then
        if not self._stop_flag:
            to_wait.append(asyncio.ensure_future(self._stopped_event.wait()))
        await _wait_and_cancel_pending(to_wait)
        # If we have died - immediately raise
        self._raise_if_terminated()
        self._finish_if_needed()

        # We have not terminated and did not have stopped - this means that
        # available event occured
        if self._available_event.is_set():
            return

        if len(to_wait) == 3:
            return await self._anext_wait()

        # I've found too much corner cases for this *** class and need some shield from the new ones
        raise RuntimeError(
            "SOMETHING VERY WEIRD OCCURED IN EXTENDABLE STREAM {}".format(self)
        )

    async def __anext__(self):
        await self._anext_wait()
        # If we entered here:
        # 1. We have not been terminated
        # 2. We have available futures
        self._anext_waiter = asyncio.ensure_future(self._async_iterator_next_impl())
        terminated = asyncio.ensure_future(self._terminated_event.wait())
        await _wait_and_cancel_pending([self._anext_waiter, terminated])
        try:
            self._raise_if_terminated()
        except:
            raise
        else:
            with self._throw_on_error_ctx():
                res = await self._anext_waiter
        finally:
            self._anext_waiter = None

        # If this was the last available element, we have to reset the event to wait new one
        if self.__empty_impl():
            self._available_event.clear()

        return res
