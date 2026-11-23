"""Deterministic two-slot SSE upstream for a local pipeline smoke test."""
import asyncio

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()
slots = asyncio.Semaphore(2)


@app.get('/health')
async def health():
    return {'status': 'ok'}


@app.post('/v1/chat/completions')
async def complete():
    async def stream():
        async with slots:
            await asyncio.sleep(0.04)
            yield b'data: {"choices":[{"delta":{"content":"Fixture summary"}}]}\n\n'
            await asyncio.sleep(0.10)
            yield b'data: [DONE]\n\n'

    return StreamingResponse(stream(), media_type='text/event-stream')
