import unittest

from gateway.sse import ChatStream, ChatStreamError, DONE, FIRST_CONTENT, SSELimitError, SSEParser


class SSETests(unittest.TestCase):
    def test_mixed_line_endings_multiline_and_bom_at_every_two_chunk_split(self):
        wire = (
            b'\xef\xbb\xbf: ping\r\nevent: chat\rdata: {"one":\r\ndata: 1}\n\r\n'
            b'event: ignored\n\ndata: next\r\rdata: [DONE]\n'
        )
        expected = [(b'chat', b'{"one":\n1}'), (b'message', b'next')]
        for index in range(len(wire) + 1):
            with self.subTest(index=index):
                parser = SSEParser()
                events = [*parser.feed(wire[:index]), *parser.feed(wire[index:])]
                self.assertEqual(events, expected)

    def test_rejects_an_oversized_line_or_accumulated_event(self):
        parser = SSEParser(max_event_bytes=10)
        list(parser.feed(b'abcdef'))
        with self.assertRaises(SSELimitError):
            list(parser.feed(b'ghijk'))
        parser = SSEParser(max_event_bytes=10)
        with self.assertRaises(SSELimitError):
            list(parser.feed(b'data:12345\ndata:12345\n\n'))

    def test_only_framed_assistant_content_and_done_are_markers(self):
        stream = ChatStream()
        self.assertEqual(list(stream.feed(b'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n')), [])
        self.assertEqual(list(stream.feed(b'data: {"choices":[{"delta":{"content":"hello"}}]}\n')), [])
        self.assertEqual(list(stream.feed(b'\ndata: [DONE]\r\n')), [FIRST_CONTENT])
        self.assertFalse(stream.done)
        self.assertEqual(list(stream.feed(b'\r\n')), [DONE])
        self.assertTrue(stream.done)

    def test_detects_error_events_and_envelopes(self):
        for wire in (b'event: error\ndata: failure\n\n', b'data: {"error":{"message":"failure"}}\n\n'):
            with self.subTest(wire=wire), self.assertRaises(ChatStreamError):
                list(ChatStream().feed(wire))
