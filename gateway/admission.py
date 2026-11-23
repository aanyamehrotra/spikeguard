"""Bounded admission for a single asyncio worker."""
import asyncio
from collections import deque
from contextlib import asynccontextmanager

TIERS = ('free', 'paid')
TIER_HEADER = 'X-SpikeGuard-Tier'


class Shed(Exception):
    pass


class Admission:
    def __init__(
        self, concurrency: int, queue_limit: int, queue_wait: float,
        paid_slots: int = 0, paid_queue_slots: int = 0,
    ):
        self.concurrency = concurrency
        self.maximum = concurrency
        self.queue_limit = queue_limit
        self.queue_wait = queue_wait
        self.paid_slots = paid_slots
        self.paid_queue_slots = paid_queue_slots
        self._active = dict.fromkeys(TIERS, 0)
        self._waiting = {tier: deque() for tier in TIERS}

    @property
    def active(self) -> int:
        return sum(self._active.values())

    @property
    def depth(self) -> int:
        return sum(len(queue) for queue in self._waiting.values())

    def active_for(self, tier: str) -> int:
        return self._active[tier]

    def depth_for(self, tier: str) -> int:
        return len(self._waiting[tier])

    def _can_start(self, tier: str) -> bool:
        if tier == 'paid':
            return self.active < self.concurrency or (
                self._active['paid'] < self.paid_slots and self.active < self.maximum
            )
        return self.active < self.concurrency and self._active['free'] < self.concurrency - self.paid_slots

    def saturated(self, tier: str) -> bool:
        return not self._can_start(tier)

    def resize(self, concurrency: int) -> None:
        self.concurrency = concurrency
        self._dispatch()

    async def _acquire(self, tier: str) -> None:
        if self._can_start(tier):
            self._active[tier] += 1
            return
        queue = self._waiting[tier]
        if self.depth >= self.queue_limit or (
            tier == 'free' and len(queue) >= self.queue_limit - self.paid_queue_slots
        ):
            raise Shed('queue_full')

        turn = asyncio.Event()
        queue.append(turn)
        try:
            try:
                async with asyncio.timeout(self.queue_wait):
                    await turn.wait()
            except TimeoutError:
                raise Shed('queue_wait_expired') from None
        except BaseException:
            # A release may already have transferred the slot before cancellation.
            if turn.is_set():
                self._release(tier)
            else:
                queue.remove(turn)
            raise

    def _dispatch(self) -> None:
        for tier in ('paid', 'free'):
            queue = self._waiting[tier]
            while queue and self._can_start(tier):
                # Reserve the slot before waking the waiter.
                self._active[tier] += 1
                queue.popleft().set()

    def _release(self, tier: str) -> None:
        self._active[tier] -= 1
        self._dispatch()

    @asynccontextmanager
    async def slot(self, tier: str = 'free'):
        await self._acquire(tier)
        try:
            yield
        finally:
            self._release(tier)
