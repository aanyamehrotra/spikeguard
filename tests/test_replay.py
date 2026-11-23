import asyncio
import unittest

import httpx

from gateway.admission import TIER_HEADER
from replay import request, schedule, schedule_by_tier, summarize, summarize_by_tier

TICKET = {'text': 'The October renewal was charged twice.', 'max_tokens': 32}
ROLE_EVENT = 'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
CONTENT_EVENT = 'data: {"choices":[{"delta":{"content":"Duplicate renewal"}}]}\n\n'
END_EVENT = 'data: [DONE]\n\n'


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

    async def test_shedding_is_reported_separately(self):
        shed = await self.run_request(lambda _: httpx.Response(429, headers={'Retry-After': '1'}))
        error = await self.run_request(lambda _: httpx.Response(429))
        stats = summarize([shed, error], (2, 2, 2))['baseline']
        self.assertEqual(shed['retry_after'], '1')
        self.assertEqual(stats['shed_fraction'], 0.5)
        self.assertEqual(stats['error_fraction'], 0.5)
        self.assertIsNone(stats['p95_ttft_s'])

    async def test_paid_requests_send_tier_and_report_outcomes_separately(self):
        def handler(request):
            self.assertEqual(request.headers[TIER_HEADER], 'paid')
            return httpx.Response(200, text=CONTENT_EVENT + END_EVENT)

        paid = await self.run_request(handler, tier='paid')
        free = await self.run_request(lambda _: httpx.Response(429, headers={'Retry-After': '1'}))
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


if __name__ == '__main__':
    unittest.main()
