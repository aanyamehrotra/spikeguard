import asyncio
import unittest

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import httpx

from gateway.admission import TIER_HEADER
from gateway.app import CHAT, Settings, create_app
from scripts.demo import serve


class SocketReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_paid_slot_and_disconnect_cleanup_over_real_http(self):
        model = FastAPI()
        held = asyncio.Event()
        closed = asyncio.Event()
        content = b'data: {"choices":[{"delta":{"content":"summary"}}]}\n\n'
        done = b'data: [DONE]\n\n'

        @model.post(CHAT)
        async def completion(request: Request):
            payload = await request.json()

            async def events():
                yield content
                if payload.get('hold'):
                    try:
                        await held.wait()
                    finally:
                        closed.set()
                yield done

            return StreamingResponse(events(), media_type='text/event-stream')

        async with serve(model) as upstream:
            gateway = create_app(Settings(upstream=upstream, paid_slots=1, queue_limit=0))
            async with serve(gateway) as url, httpx.AsyncClient(base_url=url, trust_env=False) as client:
                async with asyncio.timeout(5):
                    async with client.stream('POST', CHAT, json={'hold': True}) as first:
                        self.assertEqual(first.status_code, 200)
                        chunks = first.aiter_bytes(chunk_size=len(content))
                        self.assertEqual(await anext(chunks), content)

                        free = await client.post(CHAT, json={})
                        self.assertEqual((free.status_code, free.headers.get('retry-after')), (429, '1'))

                        paid = await client.post(CHAT, headers={TIER_HEADER: 'paid'}, json={})
                        self.assertEqual((paid.status_code, paid.content), (200, content + done))
                        self.assertFalse(closed.is_set())

                    await closed.wait()
                    while 'spikeguard_gateway_active 0.0\n' not in (await client.get('/metrics')).text:
                        await asyncio.sleep(0.01)
                    restored = await client.post(CHAT, json={})
                    self.assertEqual((restored.status_code, restored.content), (200, content + done))

                    metrics = (await client.get('/metrics')).text
                    self.assertIn('spikeguard_gateway_active 0.0\n', metrics)
                    self.assertIn('spikeguard_gateway_queue_depth 0.0\n', metrics)
