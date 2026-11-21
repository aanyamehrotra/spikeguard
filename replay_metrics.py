"""Prometheus instrumentation for one replay process."""
from prometheus_client import CollectorRegistry, Counter, Histogram, start_http_server

OUTCOMES = (
    'completed', 'shed', 'http_error', 'timeout', 'transport_error',
    'protocol_error', 'incomplete',
)


class ReplayMetrics:
    def __init__(self, phases):
        self.registry = CollectorRegistry()
        self.offered = Counter(
            'spikeguard_replay_offered', 'Requests dispatched by the replay client.',
            ('phase',), registry=self.registry,
        )
        self.finished = Counter(
            'spikeguard_replay_finished', 'Terminal outcomes grouped by arrival phase.',
            ('phase', 'outcome'), registry=self.registry,
        )
        self.ttft = Histogram(
            'spikeguard_replay_ttft_seconds',
            'Time from dispatch to first assistant content for completed requests.',
            ('phase',), registry=self.registry,
            buckets=(0.1, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.5, 3, 4, 6, 10, 15, 30),
        )
        for phase in phases:
            self.offered.labels(phase)
            self.ttft.labels(phase)
            for outcome in OUTCOMES:
                self.finished.labels(phase, outcome)

    def start(self, port):
        return start_http_server(port, addr='127.0.0.1', registry=self.registry)

    def record(self, row):
        phase = row['phase']
        outcome = row['outcome']
        if outcome == 'completed':
            self.ttft.labels(phase).observe(row['ttft_s'])
        self.finished.labels(phase, outcome).inc()
