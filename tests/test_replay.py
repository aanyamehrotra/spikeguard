import asyncio
import unittest

import httpx

from gateway.admission import SHED_HEADER, TIER_HEADER
from replay import measurement_metadata, request, schedule, schedule_by_tier, summarize, summarize_by_tier

TICKET = {'text': 'The October renewal was charged twice.', 'max_tokens': 32}
ROLE_EVENT = 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
CONTENT_EVENT = 'data: {"choices":[{"delta":{"content":"Duplicate renewal"}}]}\n\n'
END_EVENT = 'data: [DONE]\n\n'


class ChunkedStream(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body

    async def __aiter__(self):
        for offset in range(0, len(self.body), 5):
            yield self.body[offset:offset + 5]


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    def test_schedule_preserves_phase_boundaries(self):
        tasks = list(schedule((1, 4, 1), (2, 2, 2), [TICKET], 42))
        self.assertEqual(len(tasks), 12)
        self.assertEqual(tasks[2][:2], ('spike', 2))
        self.assertEqual(tasks[10][:2], ('recovery', 4))

    def test_paid_schedule_adds_independent_arrivals_without_changing_free_traffic(self):
        rates = (1, 4, 1)
        durations = (2, 2, 2)
        tickets = [TICKET, {**TICKET, 'text': 'Unexpected login prompt'}]
        traffic = list(schedule_by_tier(rates, (0, 1, 0), durations, tickets, 42))
        self.assertEqual([task for task, tier in traffic if tier == 'free'], list(schedule(rates, durations, tickets, 42)))
        self.assertEqual([(phase, offset) for (phase, offset, _), tier in traffic if tier == 'paid'],
                         [('spike', 2), ('spike', 3)])
        self.assertEqual([task[1] for task, _ in traffic], sorted(task[1] for task, _ in traffic))

    async def run_request(self, handler, timeout=1, tier='free'):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await request(
                client, 'http://127.0.0.1', ('baseline', 0, TICKET),
                asyncio.get_running_loop().time(), timeout, tier,
            )

    async def test_role_event_does_not_count_as_content(self):
        row = await self.run_request(lambda _: httpx.Response(200, text=ROLE_EVENT + END_EVENT))
        self.assertIsNone(row['ttft_s'])
        self.assertEqual(row['outcome'], 'incomplete')

    async def test_stream_requires_content_and_terminator(self):
        for suffix, outcome in (('', 'incomplete'), (END_EVENT, 'completed')):
            with self.subTest(outcome=outcome):
                row = await self.run_request(
                    lambda _: httpx.Response(200, text=ROLE_EVENT + CONTENT_EVENT + suffix)
                )
                self.assertIsNotNone(row['ttft_s'])
                self.assertGreaterEqual(row['ttft_s'], 0)
                self.assertEqual(row['outcome'], outcome)

    async def test_truncated_done_event_is_not_a_completion(self):
        for truncated in ('data: [DONE]', 'data: [DONE]\n'):
            with self.subTest(truncated=truncated):
                row = await self.run_request(lambda _: httpx.Response(200, text=CONTENT_EVENT + truncated))
                self.assertEqual(row['outcome'], 'incomplete')
                self.assertIsNotNone(row['ttft_s'])

    async def test_completed_stream_stays_completed_if_cleanup_fails_or_times_out(self):
        loop = asyncio.get_running_loop()
        for stall in (False, True):
            with self.subTest(stall=stall):
                closing_at = None

                class FailingCloseStream(ChunkedStream):
                    async def aclose(self):
                        nonlocal closing_at
                        closing_at = loop.time()
                        if stall:
                            await asyncio.Event().wait()
                        raise httpx.CloseError('failed to close response stream')

                body = (CONTENT_EVENT + END_EVENT).encode('utf-8')
                transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=FailingCloseStream(body)))
                async with httpx.AsyncClient(transport=transport) as client:
                    origin = loop.time()
                    result = await request(client, 'http://127.0.0.1', ('baseline', 0, TICKET), origin, 0.2)
                self.assertEqual(result['outcome'], 'completed')
                self.assertIsNotNone(closing_at)
                self.assertLessEqual(origin + result['dispatch_lag_s'] + result['elapsed_s'], closing_at)

    async def test_multiline_sse_over_split_http_chunks(self):
        body = (
            ': keep-alive\r\n\r\n'
            'data: {"choices": [\r\n'
            'data: {"delta": {"content": "Résumé"}}]}\r\n\r\n'
            'data: [DONE]\r\n\r\n'
        ).encode('utf-8')
        row = await self.run_request(lambda _: httpx.Response(200, stream=ChunkedStream(body)))
        self.assertEqual(row['outcome'], 'completed')

    async def test_error_frame_overrides_partial_content(self):
        for error in ('event: error\ndata: stream failed\n\n', 'data: {"error":{"message":"stream failed"}}\n\n'):
            with self.subTest(error=error):
                row = await self.run_request(lambda _: httpx.Response(200, text=CONTENT_EVENT + error + END_EVENT))
                self.assertEqual(row['outcome'], 'protocol_error')
                self.assertIsNotNone(row['ttft_s'])

    async def test_shedding_is_reported_separately(self):
        shed = await self.run_request(lambda _: httpx.Response(
            429, headers={'Retry-After': '1', SHED_HEADER: 'queue_full'},
        ))
        error = await self.run_request(lambda _: httpx.Response(
            429, headers={'Retry-After': '1'}, json={'error': {'code': 'rate_limit'}},
        ))
        stats = summarize([shed, error], (2, 2, 2))['baseline']
        self.assertEqual(shed['retry_after'], '1')
        self.assertEqual(shed['shed_reason'], 'queue_full')
        self.assertEqual(error['outcome'], 'http_error')
        self.assertEqual(stats['shed_fraction'], 0.5)
        self.assertEqual(stats['error_fraction'], 0.5)
        self.assertIsNone(stats['p95_ttft_s'])

    async def test_legacy_gateway_shed_requires_code_and_retry_after(self):
        body = {'error': {'code': 'queue_wait_expired'}}
        legacy = await self.run_request(lambda _: httpx.Response(429, headers={'Retry-After': '1'}, json=body))
        missing_retry = await self.run_request(lambda _: httpx.Response(429, json=body))
        self.assertEqual(legacy['outcome'], 'shed')
        self.assertEqual(legacy['shed_reason'], 'queue_wait_expired')
        self.assertEqual(missing_retry['outcome'], 'http_error')

    async def test_paid_requests_send_tier_and_report_outcomes_separately(self):
        def handler(request):
            self.assertEqual(request.headers[TIER_HEADER], 'paid')
            return httpx.Response(200, text=CONTENT_EVENT + END_EVENT)

        paid = await self.run_request(handler, tier='paid')
        free = await self.run_request(lambda _: httpx.Response(
            429, headers={'Retry-After': '1', SHED_HEADER: 'queue_full'},
        ))
        summaries = summarize_by_tier([free, paid], (2, 2, 2))
        self.assertEqual(summaries['paid']['baseline']['completion_fraction'], 1)
        self.assertEqual(summaries['free']['baseline']['shed_fraction'], 1)
        self.assertIsNone(summaries['paid']['spike']['completion_fraction'])

    async def test_deadline_includes_time_before_headers(self):
        async def slow_response(_):
            await asyncio.sleep(5)
            return httpx.Response(200)

        row = await self.run_request(slow_response, timeout=0.02)
        self.assertEqual(row['outcome'], 'timeout')

    async def test_arrival_past_deadline_never_dispatches(self):
        def handler(_):
            self.fail('An expired arrival reached the server')

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            row = await request(
                client, 'http://127.0.0.1', ('baseline', 0, TICKET),
                asyncio.get_running_loop().time() - 1, timeout=0.1,
            )
        self.assertEqual(row['outcome'], 'timeout')
        self.assertIsNone(row['status'])
        self.assertGreaterEqual(row['dispatch_lag_s'], 1)

    def test_summary_exposes_dispatch_completion_and_shed_latency(self):
        rows = [
            dict(phase='baseline', outcome='completed', ttft_s=0.2, elapsed_s=0.5, dispatch_lag_s=0.1),
            dict(phase='baseline', outcome='shed', ttft_s=None, elapsed_s=0.1, dispatch_lag_s=0.2),
            dict(phase='baseline', outcome='timeout', ttft_s=None, elapsed_s=0, dispatch_lag_s=0.3),
        ]
        summary = summarize(rows, (2, 2, 2))['baseline']
        self.assertEqual(summary['offered_per_second'], 1.5)
        self.assertEqual(summary['p95_dispatch_lag_s'], 0.3)
        self.assertEqual(summary['p95_completion_latency_s'], 0.5)
        self.assertEqual(summary['p95_shed_latency_s'], 0.1)
        self.assertEqual(summary['p95_ttft_s'], 0.2)

    def test_workload_fingerprint_is_stable_and_order_sensitive(self):
        reordered = {'max_tokens': 32, 'text': TICKET['text']}
        other = {**TICKET, 'text': 'Reset a password'}
        original = measurement_metadata([TICKET, other])
        self.assertEqual(original, measurement_metadata([reordered, other]))
        self.assertNotEqual(original['tickets_sha256'], measurement_metadata([other, TICKET])['tickets_sha256'])


if __name__ == '__main__':
    unittest.main()
