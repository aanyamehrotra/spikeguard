import asyncio
import unittest

import httpx

from replay import request, schedule, summarize

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

    async def run_request(self, handler, timeout=1):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await request(
                client, 'http://127.0.0.1', ('baseline', 0, TICKET),
                asyncio.get_running_loop().time(), timeout,
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
                self.assertGreater(row['ttft_s'], 0)
                self.assertEqual(row['outcome'], outcome)

    async def test_shedding_is_reported_separately(self):
        shed = await self.run_request(lambda _: httpx.Response(429, headers={'Retry-After': '1'}))
        error = await self.run_request(lambda _: httpx.Response(429))
        stats = summarize([shed, error], (2, 2, 2))['baseline']
        self.assertEqual(shed['retry_after'], '1')
        self.assertEqual(stats['shed_fraction'], 0.5)
        self.assertEqual(stats['error_fraction'], 0.5)
        self.assertIsNone(stats['p95_ttft_s'])

    async def test_deadline_includes_time_before_headers(self):
        async def slow_response(_):
            await asyncio.sleep(5)
            return httpx.Response(200)

        row = await self.run_request(slow_response, timeout=0.02)
        self.assertEqual(row['outcome'], 'timeout')


if __name__ == '__main__':
    unittest.main()
