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
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.requests import ClientDisconnect

from .adaptive import AdaptiveConcurrency
from .admission import Admission, SHED_HEADER, Shed, TIER_HEADER, TIERS
from .metrics import GatewayMetrics
from .sse import ChatStream, ChatStreamError, FIRST_CONTENT, SSELimitError

CHAT = '/v1/chat/completions'
MAX_BODY_BYTES = 256 * 1024
READY_TIMEOUT_S = 1.0


@dataclass(frozen=True)
class Settings:
    upstream: str = 'http://127.0.0.1:8080'
    concurrency: int = 2
    queue_limit: int = 2
    queue_wait: float = 0.25
    deadline: float = 25.0
    paid_slots: int = 0
    paid_queue_slots: int = 0
    adaptive: bool = False
    adaptive_min: int | None = None
    adaptive_target: float = 1.5
    adaptive_recovery_samples: int = 8

    def __post_init__(self):
        if self.concurrency < 1 or self.queue_limit < 0:
            raise ValueError('concurrency must be positive and queue limit must be nonnegative')
        if not all(math.isfinite(value) and value > 0 for value in (self.queue_wait, self.deadline)):
            raise ValueError('queue wait and request deadline must be positive and finite')
        if not 0 <= self.paid_slots < self.concurrency:
            raise ValueError('paid slots must leave at least one free upstream slot')
        if not 0 <= self.paid_queue_slots <= self.queue_limit:
            raise ValueError('paid queue slots must fit within the queue limit')
        if not self.paid_slots < self.adaptive_floor <= self.concurrency:
            raise ValueError('adaptive minimum must leave room for both lanes and fit within concurrency')
        if not math.isfinite(self.adaptive_target) or self.adaptive_target <= 0 or self.adaptive_recovery_samples < 1:
            raise ValueError('adaptive target and recovery sample count must be positive and finite')

    @property
    def priority_enabled(self):
        return bool(self.paid_slots or self.paid_queue_slots)

    @property
    def adaptive_floor(self):
        return self.paid_slots + 1 if self.adaptive_min is None else self.adaptive_min

    @classmethod
    def from_env(cls):
        adaptive = os.getenv('SPIKEGUARD_ADAPTIVE', '0')
        if adaptive not in ('0', '1'):
            raise ValueError('SPIKEGUARD_ADAPTIVE must be 0 or 1')
        adaptive_min = os.getenv('SPIKEGUARD_ADAPTIVE_MIN')
        return cls(
            upstream=os.getenv('SPIKEGUARD_UPSTREAM', cls.upstream).rstrip('/'),
            concurrency=int(os.getenv('SPIKEGUARD_CONCURRENCY', cls.concurrency)),
            queue_limit=int(os.getenv('SPIKEGUARD_QUEUE_LIMIT', cls.queue_limit)),
            queue_wait=float(os.getenv('SPIKEGUARD_QUEUE_WAIT_S', cls.queue_wait)),
            deadline=float(os.getenv('SPIKEGUARD_DEADLINE_S', cls.deadline)),
            paid_slots=int(os.getenv('SPIKEGUARD_PAID_SLOTS', cls.paid_slots)),
            paid_queue_slots=int(os.getenv('SPIKEGUARD_PAID_QUEUE_SLOTS', cls.paid_queue_slots)),
            adaptive=adaptive == '1',
            adaptive_min=int(adaptive_min) if adaptive_min else None,
            adaptive_target=float(os.getenv('SPIKEGUARD_ADAPTIVE_TARGET_S', cls.adaptive_target)),
            adaptive_recovery_samples=int(os.getenv('SPIKEGUARD_ADAPTIVE_RECOVERY_SAMPLES', cls.adaptive_recovery_samples)),
        )


def error(status, code, headers=None):
    return JSONResponse({'error': {'code': code}}, status_code=status, headers=headers)


async def watch_disconnect(receive, cancel_scope):
    while (await receive())['type'] != 'http.disconnect':
        pass
    cancel_scope.cancel()


class ProxyResponse(Response):
    """Keep the deadline, disconnect monitor and upstream lease alive through the body."""

    def __init__(self, client, admission, adaptive, metrics, payload, deadline, tier):
        super().__init__()
        self.client = client
        self.admission = admission
        self.adaptive = adaptive
        self.metrics = metrics
        self.payload = payload
        self.deadline = deadline
        self.tier = tier
        self.completed_stream = False

    async def __call__(self, scope, receive, send):
        outcome = 'internal_error'
        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(watch_disconnect, receive, tasks.cancel_scope)
                try:
                    outcome = await self.forward(scope, receive, send)
                except OSError:
                    # ASGI servers may report a closed downstream from send().
                    outcome = 'disconnected'
                except asyncio.CancelledError:
                    outcome = 'disconnected'
                    raise
                finally:
                    tasks.cancel_scope.cancel()
        finally:
            self.metrics.outcomes.labels(self.tier, 'completed' if self.completed_stream else outcome).inc()

    async def forward(self, scope, receive, send):
        headers_sent = False
        loop = asyncio.get_running_loop()
        queued_at = loop.time()
        try:
            with anyio.fail_after(max(0, self.deadline - queued_at)) as budget:
                async with self.admission.slot(self.tier):
                    started = loop.time()
                    self.metrics.queue_wait.labels(self.tier).observe(started - queued_at)
                    if started >= self.deadline:
                        raise TimeoutError
                    feedback = self.adaptive.feedback(self.admission.saturated(self.tier)) if self.adaptive else None
                    upstream = None
                    try:
                        upstream = await self.client.send(self.client.build_request(
                            'POST', CHAT, content=self.payload,
                            headers={'content-type': 'application/json', 'accept-encoding': 'identity'},
                        ), stream=True)
                        content_type = upstream.headers.get('content-type', 'application/octet-stream')
                        is_sse = content_type.split(';', 1)[0].strip().lower() == 'text/event-stream'
                        observe_stream = is_sse and upstream.is_success
                        if feedback is not None and (upstream.status_code == 429 or upstream.status_code >= 500):
                            feedback(None)
                        if not observe_stream:
                            feedback = None
                        headers = [
                            (b'content-type', content_type.encode('latin-1')),
                            (b'cache-control', b'no-store'),
                        ]
                        headers_sent = True
                        await send({
                            'type': 'http.response.start', 'status': upstream.status_code,
                            'headers': headers,
                        })
                        outcome = await self.relay_body(upstream, observe_stream, started, feedback, send)
                    except httpx.HTTPError:
                        if feedback is not None:
                            feedback(None)
                        raise
                    except asyncio.CancelledError:
                        if feedback is not None and budget.cancel_called:
                            feedback(None, failure='deadline')
                        raise
                    finally:
                        # Finish closing the upstream before returning the slot.
                        if upstream is not None:
                            with anyio.CancelScope(shield=True):
                                await upstream.aclose()
        except Shed as exc:
            reason = str(exc)
            self.metrics.record_shed(self.tier, reason)
            await error(429, reason, {'Retry-After': '1', SHED_HEADER: reason})(scope, receive, send)
            return 'shed'
        except (TimeoutError, httpx.HTTPError) as exc:
            timed_out = isinstance(exc, TimeoutError)
            outcome = 'deadline_exceeded' if timed_out else 'upstream_transport_error'
            if not headers_sent:
                if timed_out:
                    response = error(504, 'deadline_exceeded')
                else:
                    response = error(502, 'upstream_unavailable')
                await response(scope, receive, send)
                return outcome
            # The status is committed. End the partial stream without synthesizing [DONE].
        await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
        return outcome

    async def relay_body(self, upstream, observe_stream, started, feedback, send):
        loop = asyncio.get_running_loop()
        stream = ChatStream() if observe_stream else None
        outcome = 'completed' if upstream.is_success else 'upstream_http_error'
        async for chunk in upstream.aiter_bytes():
            if stream is not None:
                try:
                    for marker in stream.feed(chunk):
                        if marker == FIRST_CONTENT:
                            ttft = loop.time() - started
                            self.metrics.upstream_ttft.labels(self.tier).observe(ttft)
                            if feedback is not None:
                                feedback(ttft)
                except (ChatStreamError, SSELimitError):
                    outcome = 'protocol_error'
                    stream = None
                    if feedback is not None:
                        feedback(None, failure='incomplete_stream')
            await send({'type': 'http.response.body', 'body': chunk, 'more_body': True})
            if stream is not None and stream.done:
                self.completed_stream = stream.has_content
        if stream is not None and not self.completed_stream:
            if feedback is not None:
                feedback(None, failure='incomplete_stream')
            return 'incomplete_stream'
        return outcome


def create_app(settings=None, *, transport=None):
    settings = settings or Settings.from_env()
    admission = Admission(
        settings.concurrency, settings.queue_limit, settings.queue_wait,
        settings.paid_slots, settings.paid_queue_slots,
    )
    telemetry = GatewayMetrics(admission)
    adaptive = None
    if settings.adaptive:
        adaptive = AdaptiveConcurrency(
            admission, settings.adaptive_floor, settings.adaptive_target, settings.adaptive_recovery_samples,
            on_adjustment=telemetry.record_adjustment,
        )
    probe_lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        # Match the pool to the admission ceiling and leave one connection for readiness.
        pool_size = settings.concurrency + 1
        async with httpx.AsyncClient(
            base_url=settings.upstream, timeout=None, trust_env=False, transport=transport,
            limits=httpx.Limits(max_connections=pool_size, max_keepalive_connections=pool_size),
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
            async with asyncio.timeout(READY_TIMEOUT_S), probe_lock:
                async with request.app.state.client.stream('GET', '/health') as response:
                    if response.is_success:
                        return {'status': 'ok'}
        except (TimeoutError, httpx.HTTPError):
            pass
        return error(503, 'upstream_not_ready')

    @app.get('/metrics')
    async def metrics():
        return Response(generate_latest(telemetry.registry), media_type=CONTENT_TYPE_LATEST)

    @app.post(CHAT)
    async def chat(request: Request):
        tier = request.headers.get(TIER_HEADER, 'free').strip().lower()
        if tier not in TIERS:
            return error(400, 'invalid_tier')
        if not settings.priority_enabled:
            tier = 'free'
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
        return ProxyResponse(
            request.app.state.client, admission, adaptive, telemetry, bytes(body), deadline, tier,
        )

    return app


app = create_app()
