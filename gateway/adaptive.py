"""Adjust the admission limit using upstream time to first content."""
ADJUSTMENT_REASONS = (
    ('increase', 'healthy'),
    ('decrease', 'slow_ttft'),
    ('decrease', 'upstream_error'),
    ('decrease', 'deadline'),
    ('decrease', 'incomplete_stream'),
)


class AdaptiveConcurrency:
    def __init__(self, admission, minimum, target, recovery_samples, on_adjustment=None):
        self.admission = admission
        self.minimum = minimum
        self.maximum = admission.concurrency
        self.target = target
        self.recovery_samples = recovery_samples
        self.generation = 0
        self.healthy_streak = 0
        self.on_adjustment = on_adjustment

    def feedback(self, saturated):
        generation = self.generation
        recorded = False

        def observe(ttft, *, failure='upstream_error'):
            nonlocal recorded
            if not recorded:
                recorded = True
                self.record(generation, saturated, ttft, failure=failure)

        return observe

    def record(self, generation, saturated, ttft, *, failure='upstream_error'):
        if generation != self.generation:
            return
        limit = self.admission.concurrency
        if ttft is None or ttft >= self.target:
            self.healthy_streak = 0
            new_limit = max(self.minimum, limit // 2)
            direction = 'decrease'
            reason = failure if ttft is None else 'slow_ttft'
        elif saturated and limit < self.maximum:
            self.healthy_streak += 1
            if self.healthy_streak < self.recovery_samples:
                return
            new_limit = limit + 1
            direction, reason = 'increase', 'healthy'
        else:
            return
        if new_limit != limit:
            self.generation += 1
            self.healthy_streak = 0
            self.admission.resize(new_limit)
            if self.on_adjustment is not None:
                self.on_adjustment(direction, reason)
