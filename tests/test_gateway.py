import asyncio
from contextlib import AsyncExitStack
import unittest
from unittest.mock import patch

import httpx

from gateway.admission import Admission, SHED_HEADER, TIER_HEADER
from gateway.app import CHAT, MAX_BODY_BYTES, Settings, create_app

CHUNK = b'data: {"choices":[{"delta":{"content":"summary"}}]}\n\n'
DONE = b'data: [DONE]\n\n'


class HeldStream(httpx.AsyncByteStream):
    def __init__(self):
        self.finish = asyncio.Event()
        self.closed = asyncio.Event()

    async def __aiter__(self):
        yield CHUNK
        await self.finish.wait()
        yield DONE

    async def aclose(self):
        # Real sockets may need an asynchronous close checkpoint.
        await asyncio.sleep(0)
        self.closed.set()


class Exchange:
    """A downstream HTTP connection whose disconnect can be triggered explicitly."""

    def __init__(self, app, identifier, tier):
        self.events = asyncio.Queue()
        self.events.put_nowait({'type': 'http.request', 'body': identifier, 'more_body': False})
        self.messages = []
        self.first_chunk = asyncio.Event()
        headers = [(b'content-type', b'application/json')]
        if tier is not None:
            headers.append((TIER_HEADER.lower().encode(), tier.encode()))
        self.task = asyncio.create_task(app({
            'type': 'http', 'asgi': {'version': '3.0', 'spec_version': '2.4'},
            'method': 'POST', 'path': CHAT, 'root_path': '', 'scheme': 'http',
            'query_string': b'', 'headers': headers,
        }, self.events.get, self.send))

    async def send(self, message):
        self.messages.append(message)
        if message['type'] == 'http.response.body' and message.get('more_body'):
            self.first_chunk.set()

    def disconnect(self):
        self.events.put_nowait({'type': 'http.disconnect'})

    async def response(self):
        await self.task
        start = self.messages[0]
        body = b''.join(message.get('body', b'') for message in self.messages[1:])
        return httpx.Response(start['status'], headers=start['headers'], content=body)


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_handoff_passes_slot_to_next_waiter(self):
        admission = Admission(1, 2, 3)

        async def acquire():
            async with admission.slot():
                return admission.active

        async with admission.slot():
            abandoned = asyncio.create_task(acquire())
            next_waiter = asyncio.create_task(acquire())
            await asyncio.sleep(0)
            self.assertEqual(admission.depth, 2)
        # The first waiter owns the transferred slot, but has not resumed yet.
        abandoned.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await abandoned
        self.assertEqual(await asyncio.wait_for(next_waiter, 1), 1)
        self.assertEqual((admission.active, admission.depth), (0, 0))


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = await self.enterAsyncContext(AsyncExitStack())
        self.exchanges = []
        self.addAsyncCleanup(self.stop_exchanges)

    async def stop_exchanges(self):
        for exchange in self.exchanges:
            exchange.disconnect()
        await asyncio.gather(*(exchange.task for exchange in self.exchanges), return_exceptions=True)

    async def setup_gateway(self, upstream, concurrency=1, **settings):
        self.app = create_app(
            Settings(concurrency=concurrency, **settings), transport=httpx.MockTransport(upstream),
        )
        await self.stack.enter_async_context(self.app.router.lifespan_context(self.app))
        self.client = await self.stack.enter_async_context(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url='http://gateway',
        ))

    def exchange(self, identifier=b'{}', tier=None):
        exchange = Exchange(self.app, identifier, tier)
        self.exchanges.append(exchange)
        return exchange

    async def wait_for_depth(self, depth):
        async with asyncio.timeout(2):
            while True:
                response = await self.client.get('/metrics')
                if f'spikeguard_gateway_queue_depth {depth}.0\n' in response.text:
                    return
                await asyncio.sleep(0)

    async def assert_idle(self):
        metrics = (await self.client.get('/metrics')).text
        self.assertIn('spikeguard_gateway_active 0.0\n', metrics)
        self.assertIn('spikeguard_gateway_queue_depth 0.0\n', metrics)

    async def test_streams_immediately_and_preserves_fifo_under_overload(self):
        held = HeldStream()
        order = []

        async def upstream(request):
            identifier = await request.aread()
            order.append(identifier)
            if identifier == b'1':
                return httpx.Response(200, stream=held, headers={'content-type': 'text/event-stream'})
            return httpx.Response(200, content=CHUNK + DONE)

        await self.setup_gateway(upstream, queue_limit=2, queue_wait=3)
        active = self.exchange(b'1')
        await asyncio.wait_for(active.first_chunk.wait(), 2)
        self.assertFalse(active.task.done())
        second = self.exchange(b'2')
        await self.wait_for_depth(1)
        third = self.exchange(b'3')
        await self.wait_for_depth(2)
        rejected = await asyncio.wait_for(self.exchange(b'4').response(), 1)
        self.assertEqual(rejected.status_code, 429)
        self.assertEqual(rejected.headers['retry-after'], '1')
        self.assertEqual(rejected.headers[SHED_HEADER], 'queue_full')
        self.assertEqual(rejected.json()['error']['code'], 'queue_full')
        self.assertEqual(order, [b'1'])
        held.finish.set()
        for exchange in (active, second, third):
            response = await asyncio.wait_for(exchange.response(), 2)
            self.assertEqual(response.content, CHUNK + DONE)
        self.assertEqual(order, [b'1', b'2', b'3'])
        await self.assert_idle()

    async def test_queue_wait_expires_without_consuming_a_slot(self):
        held = HeldStream()

        async def upstream(_):
            return httpx.Response(200, stream=held)

        await self.setup_gateway(upstream, queue_limit=1, queue_wait=0.05)
        active = self.exchange()
        await asyncio.wait_for(active.first_chunk.wait(), 2)
        expired = await asyncio.wait_for(self.exchange().response(), 1)
        self.assertEqual(expired.status_code, 429)
        self.assertEqual(expired.json()['error']['code'], 'queue_wait_expired')
        self.assertEqual(expired.headers[SHED_HEADER], 'queue_wait_expired')
        await self.wait_for_depth(0)
        held.finish.set()
        await asyncio.wait_for(active.response(), 2)
        await self.assert_idle()

    async def test_paid_lane_remains_available_through_free_overload(self):
        held = {b'free': HeldStream(), b'paid': HeldStream()}
        order = []

        async def upstream(request):
            self.assertNotIn(TIER_HEADER, request.headers)
            identifier = await request.aread()
            order.append(identifier)
            if identifier in held:
                return httpx.Response(200, stream=held[identifier])
            return httpx.Response(200, content=CHUNK + DONE)

        await self.setup_gateway(
            upstream, concurrency=2, queue_limit=2, queue_wait=3, paid_slots=1, paid_queue_slots=1,
        )
        free = self.exchange(b'free')
        await asyncio.wait_for(free.first_chunk.wait(), 2)
        free_waiter = self.exchange(b'free-waiter')
        await self.wait_for_depth(1)
        rejected = await asyncio.wait_for(self.exchange(b'free-rejected').response(), 1)
        self.assertEqual((rejected.status_code, rejected.headers['retry-after']), (429, '1'))
        paid = self.exchange(b'paid', 'paid')
        await asyncio.wait_for(paid.first_chunk.wait(), 2)
        paid_waiter = self.exchange(b'paid-waiter', 'paid')
        await self.wait_for_depth(2)
        metrics = (await self.client.get('/metrics')).text
        self.assertIn('spikeguard_gateway_tier_queue_depth{tier="paid"} 1.0', metrics)
        self.assertIn('spikeguard_gateway_tier_shed_total{reason="queue_full",tier="free"} 1.0', metrics)
        held[b'free'].finish.set()
        for exchange in (free, paid_waiter, free_waiter):
            self.assertEqual((await asyncio.wait_for(exchange.response(), 2)).status_code, 200)
        held[b'paid'].finish.set()
        await asyncio.wait_for(paid.response(), 2)
        self.assertEqual(order, [b'free', b'paid', b'paid-waiter', b'free-waiter'])
        await self.assert_idle()

    async def test_invalid_tier_is_rejected_before_forwarding(self):
        async def upstream(_):
            self.fail('invalid tier reached the upstream')

        await self.setup_gateway(upstream)
        response = await asyncio.wait_for(self.exchange(tier='invalid').response(), 1)
        self.assertEqual((response.status_code, response.json()['error']['code']), (400, 'invalid_tier'))

    async def test_disconnect_removes_waiter_and_closes_active_upstream(self):
        held = HeldStream()
        calls = []

        async def upstream(request):
            identifier = await request.aread()
            calls.append(identifier)
            if identifier == b'1':
                return httpx.Response(200, stream=held)
            self.assertTrue(held.closed.is_set())
            return httpx.Response(200, content=CHUNK + DONE)

        await self.setup_gateway(upstream, queue_limit=1, queue_wait=3)
        active = self.exchange(b'1')
        await asyncio.wait_for(active.first_chunk.wait(), 2)
        abandoned = self.exchange(b'2')
        await self.wait_for_depth(1)
        abandoned.disconnect()
        await asyncio.wait_for(abandoned.task, 2)
        replacement = self.exchange(b'3')
        await self.wait_for_depth(1)
        active.disconnect()
        await asyncio.wait_for(active.task, 2)
        self.assertTrue(held.closed.is_set())
        self.assertEqual((await replacement.response()).status_code, 200)
        self.assertEqual(calls, [b'1', b'3'])
        await self.assert_idle()

    async def test_deadline_before_headers_returns_504_and_releases_capacity(self):
        async def upstream(_):
            await asyncio.Event().wait()

        await self.setup_gateway(upstream, deadline=0.06)
        result = await asyncio.wait_for(self.exchange().response(), 2)
        self.assertEqual(result.status_code, 504)
        await self.assert_idle()

    async def test_deadline_after_headers_closes_without_done(self):
        held = HeldStream()

        async def upstream(_):
            return httpx.Response(200, stream=held)

        await self.setup_gateway(upstream, deadline=0.06)
        result = await asyncio.wait_for(self.exchange().response(), 2)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.content, CHUNK)
        self.assertTrue(held.closed.is_set())
        await self.assert_idle()

    async def test_queued_deadline_expires_while_active_stream_is_closing(self):
        resume_close = asyncio.Event()
        closing = asyncio.Event()

        class SlowClose(HeldStream):
            async def aclose(self):
                closing.set()
                await resume_close.wait()
                await super().aclose()

        held = SlowClose()

        async def upstream(request):
            self.assertEqual(await request.aread(), b'active')
            return httpx.Response(200, stream=held, headers={'content-type': 'text/event-stream'})

        await self.setup_gateway(upstream, deadline=0.2, queue_limit=1, queue_wait=3)
        active = self.exchange(b'active')
        try:
            await asyncio.wait_for(active.first_chunk.wait(), 2)
            queued = self.exchange(b'queued')
            await self.wait_for_depth(1)
            await asyncio.wait_for(closing.wait(), 2)
            response = await asyncio.wait_for(queued.response(), 2)
            self.assertEqual((response.status_code, response.json()['error']['code']), (504, 'deadline_exceeded'))
            metrics = (await self.client.get('/metrics')).text
            self.assertIn('spikeguard_gateway_active 1.0\n', metrics)
            self.assertIn('spikeguard_gateway_queue_depth 0.0\n', metrics)
        finally:
            resume_close.set()
        self.assertEqual((await asyncio.wait_for(active.response(), 2)).content, CHUNK)
        self.assertTrue(held.closed.is_set())
        await self.assert_idle()

    async def test_request_body_limit_rejects_before_forwarding(self):
        async def upstream(_):
            self.fail('oversized request reached the upstream')

        await self.setup_gateway(upstream)
        response = await self.client.post(CHAT, content=b'x' * (MAX_BODY_BYTES + 1))
        self.assertEqual(response.status_code, 413)
        await self.assert_idle()

    async def test_upstream_transport_failure_is_502(self):
        async def upstream(request):
            raise httpx.ConnectError('unavailable', request=request)

        await self.setup_gateway(upstream)
        response = await asyncio.wait_for(self.exchange().response(), 2)
        self.assertEqual((response.status_code, response.json()['error']['code']), (502, 'upstream_unavailable'))
        metrics = (await self.client.get('/metrics')).text
        self.assertIn('spikeguard_gateway_proxy_outcomes_total{outcome="upstream_transport_error",tier="free"} 1.0', metrics)
        await self.assert_idle()

    async def test_upstream_429_is_reported_as_http_error_without_an_admission_marker(self):
        async def upstream(_):
            return httpx.Response(429, headers={'Retry-After': '3', SHED_HEADER: 'queue_full'})

        await self.setup_gateway(upstream)
        response = await asyncio.wait_for(self.exchange().response(), 2)
        self.assertEqual(response.status_code, 429)
        self.assertNotIn(SHED_HEADER, response.headers)
        metrics = (await self.client.get('/metrics')).text
        self.assertIn('spikeguard_gateway_proxy_outcomes_total{outcome="upstream_http_error",tier="free"} 1.0', metrics)
        self.assertIn('spikeguard_gateway_shed_total{reason="queue_full"} 0.0', metrics)

    async def test_gateway_observes_event_markers_without_changing_response_bytes(self):
        sse_bodies = {
            b'complete': CHUNK + DONE,
            b'incomplete': CHUNK + b'data: [DONE]\n',
            b'bad': CHUNK + b'event: error\ndata: failed\n\n',
        }

        async def upstream(request):
            return httpx.Response(
                200, content=sse_bodies[await request.aread()], headers={'content-type': 'text/event-stream'},
            )

        await self.setup_gateway(upstream)
        for identifier, expected in sse_bodies.items():
            response = await asyncio.wait_for(self.exchange(identifier).response(), 2)
            self.assertEqual(response.content, expected)
        metrics = (await self.client.get('/metrics')).text
        for outcome in ('completed', 'incomplete_stream', 'protocol_error'):
            self.assertIn(f'spikeguard_gateway_proxy_outcomes_total{{outcome="{outcome}",tier="free"}} 1.0', metrics)
        self.assertIn('spikeguard_gateway_queue_wait_seconds_count{tier="free"} 3.0', metrics)
        self.assertIn('spikeguard_gateway_upstream_ttft_seconds_count{tier="free"} 3.0', metrics)

    async def test_readiness_checks_headers_without_reading_a_stalled_upstream_body(self):
        held = HeldStream()

        async def upstream(request):
            self.assertEqual(request.url.path, '/health')
            return httpx.Response(200, stream=held)

        await self.setup_gateway(upstream)
        response = await asyncio.wait_for(self.client.get('/ready'), 1)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(held.closed.is_set())

    async def test_readiness_times_out_waiting_for_upstream_headers(self):
        async def upstream(_):
            await asyncio.Event().wait()

        await self.setup_gateway(upstream)
        with patch('gateway.app.READY_TIMEOUT_S', 0.03):
            response = await asyncio.wait_for(self.client.get('/ready'), 1)
        self.assertEqual((response.status_code, response.json()['error']['code']), (503, 'upstream_not_ready'))

    async def test_adaptive_limit_responds_to_deadline_and_healthy_stream(self):
        async def upstream(request):
            if await request.aread() == b'slow':
                await asyncio.Event().wait()
            return httpx.Response(200, content=CHUNK + DONE, headers={'content-type': 'text/event-stream'})

        await self.setup_gateway(upstream, concurrency=2, adaptive=True, adaptive_recovery_samples=1, deadline=0.2)
        response = await asyncio.wait_for(self.exchange(b'slow').response(), 2)
        self.assertEqual(response.status_code, 504)
        metrics = (await self.client.get('/metrics')).text
        self.assertIn('spikeguard_gateway_concurrency_limit 1.0\n', metrics)
        self.assertIn('spikeguard_gateway_adaptive_adjustments_total{direction="decrease",reason="deadline"} 1.0', metrics)
        response = await asyncio.wait_for(self.exchange().response(), 2)
        self.assertEqual(response.content, CHUNK + DONE)
        metrics = (await self.client.get('/metrics')).text
        self.assertIn('spikeguard_gateway_concurrency_limit 2.0\n', metrics)
        self.assertIn('spikeguard_gateway_adaptive_adjustments_total{direction="increase",reason="healthy"} 1.0', metrics)


if __name__ == '__main__':
    unittest.main()
