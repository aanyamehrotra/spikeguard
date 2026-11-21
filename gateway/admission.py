"""FIFO admission for a single asyncio worker."""
import asyncio
from collections import deque
from contextlib import asynccontextmanager


class Shed(Exception):
    pass


class Admission:
    def __init__(self, concurrency: int, queue_limit: int, queue_wait: float):
        self.concurrency = concurrency
        self.queue_limit = queue_limit
        self.queue_wait = queue_wait
        self.active = 0
        self._waiting: deque[asyncio.Event] = deque()

    @property
    def depth(self) -> int:
        return len(self._waiting)

    async def _acquire(self) -> None:
        if self.active < self.concurrency:
            self.active += 1
            return
        if self.depth >= self.queue_limit:
            raise Shed('queue_full')

        turn = asyncio.Event()
        self._waiting.append(turn)
        try:
            try:
                async with asyncio.timeout(self.queue_wait):
                    await turn.wait()
            except TimeoutError:
                raise Shed('queue_wait_expired') from None
        except BaseException:
            # A release may already have transferred the slot before cancellation.
            if turn.is_set():
                self._release()
            else:
                self._waiting.remove(turn)
            raise

    def _release(self) -> None:
        if self._waiting:
            # Transfer the slot directly so new arrivals cannot bypass the queue.
            self._waiting.popleft().set()
        else:
            self.active -= 1

    @asynccontextmanager
    async def slot(self):
        await self._acquire()
        try:
            yield
        finally:
            self._release()
