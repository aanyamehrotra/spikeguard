"""Prometheus instrumentation for a single gateway worker."""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from .adaptive import ADJUSTMENT_REASONS
from .admission import SHED_REASONS, TIERS

PROXY_OUTCOMES = (
    'completed', 'shed', 'deadline_exceeded', 'upstream_http_error', 'upstream_transport_error',
    'disconnected', 'incomplete_stream', 'protocol_error', 'internal_error',
)


class GatewayMetrics:
    def __init__(self, admission):
        registry = self.registry = CollectorRegistry()
        Gauge('spikeguard_gateway_concurrency_limit', 'Current upstream concurrency limit.', registry=registry).set_function(
            lambda: admission.concurrency,
        )
        Gauge('spikeguard_gateway_active', 'Requests holding an upstream slot.', registry=registry).set_function(
            lambda: admission.active,
        )
        Gauge('spikeguard_gateway_queue_depth', 'Requests waiting for a slot.', registry=registry).set_function(
            lambda: admission.depth,
        )
        self.shed = Counter('spikeguard_gateway_shed', 'Requests rejected by admission.', ('reason',), registry=registry)
        tier_active = Gauge('spikeguard_gateway_tier_active', 'Upstream slots by tier.', ('tier',), registry=registry)
        tier_depth = Gauge('spikeguard_gateway_tier_queue_depth', 'Waiting requests by tier.', ('tier',), registry=registry)
        self.tier_shed = Counter(
            'spikeguard_gateway_tier_shed', 'Admission rejections by tier.', ('tier', 'reason'), registry=registry,
        )
        self.outcomes = Counter(
            'spikeguard_gateway_proxy_outcomes', 'Proxy outcomes after reading the request body.',
            ('tier', 'outcome'), registry=registry,
        )
        self.queue_wait = Histogram(
            'spikeguard_gateway_queue_wait_seconds', 'Admission wait for requests granted an upstream slot.',
            ('tier',), registry=registry, buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5),
        )
        self.upstream_ttft = Histogram(
            'spikeguard_gateway_upstream_ttft_seconds', 'Time from upstream dispatch to first assistant content.',
            ('tier',), registry=registry, buckets=(.025, .05, .1, .25, .5, .75, 1, 1.25, 1.5, 1.75, 2, 2.5, 4, 8, 15, 30),
        )
        self.adjustments = Counter(
            'spikeguard_gateway_adaptive_adjustments', 'Applied changes to the adaptive concurrency target.',
            ('direction', 'reason'), registry=registry,
        )
        for direction, reason in ADJUSTMENT_REASONS:
            self.adjustments.labels(direction, reason)
        for reason in SHED_REASONS:
            self.shed.labels(reason)
        for tier in TIERS:
            tier_active.labels(tier).set_function(lambda tier=tier: admission.active_for(tier))
            tier_depth.labels(tier).set_function(lambda tier=tier: admission.depth_for(tier))
            self.queue_wait.labels(tier)
            self.upstream_ttft.labels(tier)
            for outcome in PROXY_OUTCOMES:
                self.outcomes.labels(tier, outcome)
            for reason in SHED_REASONS:
                self.tier_shed.labels(tier, reason)

    def record_shed(self, tier, reason):
        self.shed.labels(reason).inc()
        self.tier_shed.labels(tier, reason).inc()

    def record_adjustment(self, direction, reason):
        self.adjustments.labels(direction, reason).inc()
