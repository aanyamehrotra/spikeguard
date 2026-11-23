"""Incremental Server-Sent Events framing for OpenAI-compatible streams."""
from codecs import BOM_UTF8
import json
import re

MAX_EVENT_BYTES = 256 * 1024
LINE_END = re.compile(br'[\r\n]')
FIRST_CONTENT = 'first_content'
DONE = 'done'


class SSELimitError(ValueError):
    """An event or one of its lines exceeded the decoder's memory limit."""


class ChatStreamError(ValueError):
    """A chat stream reported an error or contained a malformed event."""


class SSEParser:
    def __init__(self, max_event_bytes=MAX_EVENT_BYTES):
        self.max_event_bytes = max_event_bytes
        self._line = bytearray()
        self._data = bytearray()
        self._event_type = b'message'
        self._first_line = True
        self._skip_lf = False

    def feed(self, chunk: bytes):
        """Yield (event type, data) pairs only at completed event boundaries."""
        offset = 0
        for match in LINE_END.finditer(chunk):
            index = match.start()
            newline = chunk[index]
            if self._skip_lf and offset == index and newline == 10:
                self._skip_lf = False
                offset = index + 1
                continue
            self._append_line(chunk, offset, index)
            offset = index + 1
            self._skip_lf = newline == 13
            event = self._finish_line()
            if event is not None:
                yield event
        if offset < len(chunk):
            self._skip_lf = False
            self._append_line(chunk, offset, len(chunk))

    def _append_line(self, chunk, start, end):
        if len(self._line) + end - start > self.max_event_bytes:
            raise SSELimitError('SSE line exceeds the byte limit')
        self._line.extend(chunk[start:end])

    def _finish_line(self):
        line = bytes(self._line)
        self._line.clear()
        if self._first_line:
            line = line.removeprefix(BOM_UTF8)
            self._first_line = False
        if not line:
            event = (self._event_type, bytes(self._data)[:-1]) if self._data else None
            self._data.clear()
            self._event_type = b'message'
            return event

        field, _, value = line.partition(b':')
        value = value.removeprefix(b' ')
        if field == b'data':
            if len(self._data) + len(value) + 1 > self.max_event_bytes:
                raise SSELimitError('SSE event data exceeds the byte limit')
            self._data.extend(value)
            self._data.append(10)
        elif field == b'event':
            self._event_type = value or b'message'
        return None


class ChatStream:
    """Observe the meaningful markers in an OpenAI-style chat SSE stream."""

    def __init__(self):
        self.parser = SSEParser()
        self.has_content = False
        self.done = False

    def feed(self, chunk: bytes):
        if self.done:
            return
        for event_type, data in self.parser.feed(chunk):
            if event_type == b'error':
                raise ChatStreamError('upstream sent an SSE error event')
            data = data.strip()
            if data == b'[DONE]':
                self.done = True
                yield DONE
                return
            if not data:
                continue
            try:
                message = json.loads(data)
            except (ValueError, RecursionError) as exc:
                raise ChatStreamError('malformed chat event') from exc
            try:
                if message.get('error') is not None:
                    raise ChatStreamError('upstream sent an error envelope')
                for choice in message.get('choices', []):
                    content = choice.get('delta', {}).get('content')
                    if not self.has_content and isinstance(content, str) and content:
                        self.has_content = True
                        yield FIRST_CONTENT
            except (TypeError, AttributeError) as exc:
                raise ChatStreamError('malformed chat event') from exc
