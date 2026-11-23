from contextlib import AsyncExitStack
import unittest

from gateway.adaptive import AdaptiveConcurrency, FirstContent, MAX_EVENT_LINE_BYTES
from gateway.admission import Admission, Shed


class AdaptiveTests(unittest.IsolatedAsyncioTestCase):
    def test_slow_samples_reduce_and_saturated_healthy_samples_recover(self):
        admission = Admission(4, 0, 1)
        controller = AdaptiveConcurrency(admission, minimum=1, target=1.5, recovery_samples=2)
        controller.record(0, True, 1.5)
        controller.record(0, True, None)
        self.assertEqual(admission.concurrency, 2)
        controller.record(1, False, 0.1)
        controller.record(1, True, 0.1)
        self.assertEqual(admission.concurrency, 2)
        controller.record(1, True, 0.1)
        self.assertEqual(admission.concurrency, 3)
        controller.record(2, True, None)
        controller.record(3, True, None)
        self.assertEqual(admission.concurrency, 1)

    async def test_contraction_preserves_paid_reservation_while_free_work_drains(self):
        admission = Admission(4, 0, 1, paid_slots=1)
        controller = AdaptiveConcurrency(admission, minimum=2, target=1.5, recovery_samples=2)
        async with AsyncExitStack() as stack:
            for _ in range(3):
                await stack.enter_async_context(admission.slot('free'))
            controller.record(0, True, None)
            self.assertEqual((admission.concurrency, admission.active), (2, 3))
            async with admission.slot('paid'):
                self.assertEqual(admission.active, 4)
                with self.assertRaises(Shed):
                    async with admission.slot('free'):
                        self.fail('free request exceeded the reduced limit')
        self.assertEqual(admission.active, 0)

    def test_probe_ignores_role_and_oversized_lines_and_handles_split_crlf(self):
        probe = FirstContent()
        self.assertFalse(probe.feed(b'data: {"choices":[{"delta":{"role":"assistant"}}]}\r\n'))
        self.assertFalse(probe.feed(b'x' * (MAX_EVENT_LINE_BYTES + 1)))
        self.assertFalse(probe.feed(b'\r\ndata: {"choices":[{"delta":{"con'))
        self.assertFalse(probe.feed(b'tent":"summary"}}]}\r'))
        self.assertTrue(probe.feed(b'\n\r\n'))
