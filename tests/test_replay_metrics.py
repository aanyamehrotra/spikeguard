import unittest

from replay_metrics import ReplayMetrics


class ReplayMetricsTests(unittest.TestCase):
    def test_rate_series_exist_before_requests_arrive(self):
        metrics = ReplayMetrics(('spike',))
        self.assertEqual(
            metrics.registry.get_sample_value(
                'spikeguard_replay_finished_total', {'phase': 'spike', 'outcome': 'shed'},
            ),
            0,
        )

    def test_completed_requests_alone_contribute_to_ttft(self):
        metrics = ReplayMetrics(('spike',))
        metrics.record({'phase': 'spike', 'outcome': 'completed', 'ttft_s': 0.5})
        metrics.record({'phase': 'spike', 'outcome': 'timeout', 'ttft_s': 4.0})
        metrics.record({'phase': 'spike', 'outcome': 'shed', 'ttft_s': None})

        labels = {'phase': 'spike'}
        count = metrics.registry.get_sample_value('spikeguard_replay_ttft_seconds_count', labels)
        total = metrics.registry.get_sample_value('spikeguard_replay_ttft_seconds_sum', labels)
        self.assertEqual((count, total), (1, 0.5))
        for outcome in ('completed', 'timeout', 'shed'):
            self.assertEqual(
                metrics.registry.get_sample_value(
                    'spikeguard_replay_finished_total', {**labels, 'outcome': outcome},
                ),
                1,
            )
