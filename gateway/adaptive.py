"""Adjust the admission limit using upstream time to first content."""
import json

MAX_EVENT_LINE_BYTES = 256 * 1024


class AdaptiveConcurrency:
    def __init__(self, admission, minimum, target, recovery_samples):
        self.admission = admission
        self.minimum = minimum
        self.maximum = admission.concurrency
        self.target = target
        self.recovery_samples = recovery_samples
        self.generation = 0
        self.healthy_streak = 0

    def record(self, generation, saturated, ttft):
        if generation != self.generation:
            return
        limit = self.admission.concurrency
        if ttft is None or ttft >= self.target:
            self.healthy_streak = 0
            new_limit = max(self.minimum, limit // 2)
        elif saturated and limit < self.maximum:
            self.healthy_streak += 1
            if self.healthy_streak < self.recovery_samples:
                return
            new_limit = limit + 1
        else:
            return
        if new_limit != limit:
            self.generation += 1
            self.healthy_streak = 0
            self.admission.resize(new_limit)


class FirstContent:
    """Probe OpenAI-style SSE data lines without changing forwarded bytes."""

    def __init__(self):
        self.pending = bytearray()
        self.discard_line = False

    def feed(self, chunk: bytes) -> bool:
        parts = chunk.split(b'\n')
        for part in parts[:-1]:
            self._append(part)
            if not self.discard_line and self._has_content(self.pending):
                return True
            self.pending.clear()
            self.discard_line = False
        self._append(parts[-1])
        return False

    def _append(self, part):
        if len(self.pending) + len(part) > MAX_EVENT_LINE_BYTES:
            self.pending.clear()
            self.discard_line = True
        if not self.discard_line:
            self.pending.extend(part)

    @staticmethod
    def _has_content(line):
        if not line.startswith(b'data:'):
            return False
        try:
            data = json.loads(line[5:])
            for choice in data.get('choices', []):
                content = choice.get('delta', {}).get('content')
                if isinstance(content, str) and content:
                    return True
        except (ValueError, TypeError, AttributeError, RecursionError):
            pass
        return False
