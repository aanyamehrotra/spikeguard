"""Prometheus instrumentation for one replay process."""
from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server

from gateway.admission import TIERS

OUTCOMES = (
    'completed', 'shed', 'http_error', 'timeout', 'transport_error',
    'protocol_error', 'incomplete',
)


class ReplayMetrics:
    def __init__(self, phases):
        self.registry = CollectorRegistry()
        self.offered, self.finished, self.ttft = self._metrics('spikeguard_replay', ('phase',))
        self.tier_offered, self.tier_finished, self.tier_ttft = self._metrics(
            'spikeguard_replay_tier', ('phase', 'tier'),
        )
        for phase in phases:
            self.offered.labels(phase)
            self.ttft.labels(phase)
            for outcome in OUTCOMES:
                self.finished.labels(phase, outcome)
            for tier in TIERS:
                self.tier_offered.labels(phase, tier)
                self.tier_ttft.labels(phase, tier)
                for outcome in OUTCOMES:
                    self.tier_finished.labels(phase, tier, outcome)

    def _metrics(self, prefix, labels):
        offered = Counter(
            f'{prefix}_offered', 'Scheduled arrivals attempted by the replay client.', labels, registry=self.registry,
        )
        finished = Counter(
            f'{prefix}_finished', 'Terminal outcomes grouped by arrival phase.', (*labels, 'outcome'), registry=self.registry,
        )
        ttft = Histogram(
            f'{prefix}_ttft_seconds',
            'Time from dispatch to first assistant content for completed requests.',
            labels, registry=self.registry,
            buckets=(0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.5, 3, 4, 6, 10, 15, 30),
        )
        return offered, finished, ttft

    def start(self, port):
        return start_http_server(port, addr='127.0.0.1', registry=self.registry)

    def record_offer(self, phase, tier):
        self.offered.labels(phase).inc()
        self.tier_offered.labels(phase, tier).inc()

    def record(self, row):
        phase = row['phase']
        tier = row.get('tier', 'free')
        outcome = row['outcome']
        if outcome == 'completed':
            self.ttft.labels(phase).observe(row['ttft_s'])
            self.tier_ttft.labels(phase, tier).observe(row['ttft_s'])
        self.finished.labels(phase, outcome).inc()
        self.tier_finished.labels(phase, tier, outcome).inc()
