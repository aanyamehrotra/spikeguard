"""Local streaming gateway for llama.cpp."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import math
import os

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
import httpx
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.requests import ClientDisconnect

from .admission import Admission, Shed

CHAT = '/v1/chat/completions'
MAX_BODY_BYTES = 256 * 1024


@dataclass(frozen=True)
class Settings:
    upstream: str = 'http://127.0.0.1:8080'
    concurrency: int = 2
    queue_limit: int = 2
    queue_wait: float = 0.25
    deadline: float = 25.0

    def __post_init__(self):
        if self.concurrency < 1 or self.queue_limit < 0:
            raise ValueError('concurrency must be positive and queue limit must be nonnegative')
        if not all(math.isfinite(value) and value > 0 for value in (self.queue_wait, self.deadline)):
            raise ValueError('queue wait and request deadline must be positive and finite')

    @classmethod
    def from_env(cls):
        return cls(
            upstream=os.getenv('SPIKEGUARD_UPSTREAM', cls.upstream).rstrip('/'),
            concurrency=int(os.getenv('SPIKEGUARD_CONCURRENCY', cls.concurrency)),
            queue_limit=int(os.getenv('SPIKEGUARD_QUEUE_LIMIT', cls.queue_limit)),
            queue_wait=float(os.getenv('SPIKEGUARD_QUEUE_WAIT_S', cls.queue_wait)),
            deadline=float(os.getenv('SPIKEGUARD_DEADLINE_S', cls.deadline)),
        )


def error(status, code, headers=None):
    return JSONResponse({'error': {'code': code}}, status_code=status, headers=headers)


async def watch_disconnect(receive, cancel_scope):
    while (await receive())['type'] != 'http.disconnect':
        pass
    cancel_scope.cancel()


class ProxyResponse(Response):
    """Keep the deadline, disconnect monitor and upstream lease alive through the body."""

    def __init__(self, client, admission, shed_counter, payload, deadline):
        super().__init__()
        self.client = client
        self.admission = admission
        self.shed_counter = shed_counter
        self.payload = payload
        self.deadline = deadline

    async def __call__(self, scope, receive, send):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(watch_disconnect, receive, tasks.cancel_scope)
            try:
                await self.forward(scope, receive, send)
            except OSError:
                # ASGI servers may report a closed downstream from send().
                pass
            finally:
                tasks.cancel_scope.cancel()

    async def forward(self, scope, receive, send):
        headers_sent = False
        try:
            async with asyncio.timeout_at(self.deadline):
                async with self.admission.slot():
                    upstream = await self.client.send(self.client.build_request(
                        'POST', CHAT, content=self.payload,
                        headers={'content-type': 'application/json', 'accept-encoding': 'identity'},
                    ), stream=True)
                    try:
                        headers = [
                            (b'content-type', upstream.headers.get(
                                'content-type', 'application/octet-stream',
                            ).encode('latin-1')),
                            (b'cache-control', b'no-store'),
                        ]
                        headers_sent = True
                        await send({
                            'type': 'http.response.start', 'status': upstream.status_code,
                            'headers': headers,
                        })
                        async for chunk in upstream.aiter_bytes():
                            await send({'type': 'http.response.body', 'body': chunk, 'more_body': True})
                    finally:
                        # Finish closing the upstream before returning the slot.
                        with anyio.CancelScope(shield=True):
                            await upstream.aclose()
        except Shed as exc:
            reason = str(exc)
            self.shed_counter.labels(reason).inc()
            await error(429, reason, {'Retry-After': '1'})(scope, receive, send)
            return
        except (TimeoutError, httpx.HTTPError) as exc:
            if not headers_sent:
                if isinstance(exc, TimeoutError):
                    response = error(504, 'deadline_exceeded')
                else:
                    response = error(502, 'upstream_unavailable')
                await response(scope, receive, send)
                return
            # The status is committed. End the partial stream without synthesizing [DONE].
        await send({'type': 'http.response.body', 'body': b'', 'more_body': False})


def create_app(settings=None, *, transport=None):
    settings = settings or Settings.from_env()
    admission = Admission(settings.concurrency, settings.queue_limit, settings.queue_wait)
    registry = CollectorRegistry()
    Gauge('spikeguard_gateway_active', 'Requests holding an upstream slot.', registry=registry).set_function(
        lambda: admission.active,
    )
    Gauge('spikeguard_gateway_queue_depth', 'Requests waiting for a slot.', registry=registry).set_function(
        lambda: admission.depth,
    )
    shed_counter = Counter('spikeguard_gateway_shed', 'Requests rejected by admission.', ('reason',), registry=registry)
    for reason in ('queue_full', 'queue_wait_expired'):
        shed_counter.labels(reason)

    @asynccontextmanager
    async def lifespan(app):
        async with httpx.AsyncClient(
            base_url=settings.upstream, timeout=None, trust_env=False, transport=transport,
        ) as client:
            app.state.client = client
            yield

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get('/health')
    async def health():
        return {'status': 'ok'}

    @app.get('/ready')
    async def ready(request: Request):
        try:
            response = await request.app.state.client.get('/health', timeout=1.0)
            if response.is_success:
                return {'status': 'ok'}
        except httpx.HTTPError:
            pass
        return error(503, 'upstream_not_ready')

    @app.get('/metrics')
    async def metrics():
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.post(CHAT)
    async def chat(request: Request):
        deadline = asyncio.get_running_loop().time() + settings.deadline
        body = bytearray()
        try:
            async with asyncio.timeout_at(deadline):
                async for chunk in request.stream():
                    if len(body) + len(chunk) > MAX_BODY_BYTES:
                        return error(413, 'request_too_large')
                    body.extend(chunk)
        except TimeoutError:
            return error(504, 'deadline_exceeded')
        except ClientDisconnect:
            return Response(status_code=499)
        return ProxyResponse(request.app.state.client, admission, shed_counter, bytes(body), deadline)

    return app


app = create_app()
