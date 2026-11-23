from argparse import Namespace
import unittest
from unittest.mock import patch

from capacity import assess, summarize_rates, summarize_trial, sweep


def row(tier='free', offset=1, outcome='completed', ttft=0.25):
    return dict(phase='steady', tier=tier, scheduled_s=offset, outcome=outcome, ttft_s=ttft)


class CapacityTests(unittest.TestCase):
    def test_warmup_is_excluded_by_arrival_and_steady_bounds_are_half_open(self):
        rows = [row(offset=0.9, outcome='timeout'), row(), row(offset=2, outcome='timeout')]
        result = summarize_trial(rows, warmup=1, duration=1, paid_rate=0, min_samples=1)
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['aggregate']['offered'], 1)
        self.assertEqual(result['by_tier']['paid']['offered'], 0)

    def test_thresholds_are_strict_except_minimum_completion(self):
        summary = dict(offered=100, p95_ttft_s=2, completion_fraction=0.99, error_fraction=0.01)
        result = assess(summary, min_samples=1)
        self.assertEqual(result['checks'], dict(ttft=False, errors=False, completion=True))
        self.assertEqual(result['status'], 'fail')

    def test_aggregate_success_cannot_hide_a_failing_or_unmeasured_paid_tier(self):
        free = [row()] * 100
        paid = [row('paid', outcome='shed', ttft=None)]
        result = summarize_trial(free + paid, warmup=0, duration=2, paid_rate=1, min_samples=1)
        self.assertEqual(result['aggregate']['status'], 'pass')
        self.assertEqual(result['by_tier']['paid']['status'], 'fail')
        self.assertEqual(result['status'], 'fail')
        result = summarize_trial(free, warmup=0, duration=2, paid_rate=1, min_samples=1)
        self.assertEqual(result['status'], 'insufficient_samples')

    def test_capacity_requires_every_repeat_to_pass(self):
        trials = [dict(free_rate_rps=rate, status=status) for rate, status in (
            (1, 'pass'), (1, 'pass'), (2, 'pass'), (2, 'fail'), (3, 'insufficient_samples'),
        )]
        result = summarize_rates(trials, (1, 2, 3), paid_rate=0.5)
        self.assertEqual(result['highest_passing_rate']['total_rate_rps'], 1.5)
        self.assertEqual([point['all_repeats_pass'] for point in result['rates']], [True, False, False])


class SweepTests(unittest.IsolatedAsyncioTestCase):
    async def test_sweep_rotates_targets_and_pairs_seeds(self):
        args = Namespace(rates=(1,), repeats=2, seed=42, timeout=2, cooldown=0,
                         warmup=0, duration=2, paid_rate=0, min_samples=1, include_requests=False)
        targets = {'direct': 'http://localhost:8080', 'gateway': 'http://localhost:8081'}
        with patch('capacity.replay', return_value=[row()]) as replay, patch('capacity.print', create=True):
            result = await sweep(args, targets, tickets=[])
        order = [(call.args[0].url, call.args[0].seed) for call in replay.await_args_list]
        self.assertEqual(order, [
            (targets['direct'], 42), (targets['gateway'], 42),
            (targets['gateway'], 43), (targets['direct'], 43),
        ])
        self.assertEqual([trial['execution_order'] for trial in result['direct']['trials']], [1, 4])
        self.assertEqual([trial['execution_order'] for trial in result['gateway']['trials']], [2, 3])
