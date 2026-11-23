import unittest

from capacity import assess, summarize_rates, summarize_trial


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
