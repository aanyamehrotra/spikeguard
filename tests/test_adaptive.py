from contextlib import AsyncExitStack
import unittest

from gateway.adaptive import AdaptiveConcurrency
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

    def test_request_feedback_is_used_once_and_attributes_changes(self):
        changes = []
        admission = Admission(4, 0, 1)
        controller = AdaptiveConcurrency(admission, 1, 1.5, 1, lambda *change: changes.append(change))
        controller.feedback(True)(None, failure='deadline')
        feedback = controller.feedback(True)
        feedback(0.1)
        feedback(None)
        self.assertEqual(admission.concurrency, 3)
        self.assertEqual(changes, [('decrease', 'deadline'), ('increase', 'healthy')])
