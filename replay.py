"""Replay synthetic support tickets against a local streaming chat endpoint."""
import argparse
import asyncio
from collections import Counter
import hashlib
import heapq
import json
import math
from pathlib import Path
import platform
import random
import statistics
import time

import httpx

from gateway.admission import SHED_HEADER, SHED_REASONS, TIER_HEADER, TIERS
from gateway.sse import ChatStream, ChatStreamError, DONE, FIRST_CONTENT, SSELimitError
from replay_metrics import ReplayMetrics

PHASES = ('baseline', 'spike', 'recovery')
MAX_LEGACY_SHED_BODY_BYTES = 4096


def measurement_metadata(tickets):
    workload = json.dumps(tickets, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return dict(
        ticket_count=len(tickets),
        tickets_sha256=hashlib.sha256(workload.encode('utf-8')).hexdigest(),
        client=dict(python=platform.python_version(), httpx=httpx.__version__,
                    system=platform.system(), machine=platform.machine()),
    )


def is_loopback_endpoint(value):
    try:
        url = httpx.URL(value)
    except httpx.InvalidURL:
        return False
    return (
        url.scheme in {'http', 'https'} and url.host in {'localhost', '127.0.0.1', '::1'}
        and not (url.query or url.fragment or url.userinfo)
    )


def schedule(rates, durations, tickets, seed, *, phases=PHASES):
    rng = random.Random(seed)
    start = 0
    for phase, rate, duration in zip(phases, rates, durations, strict=True):
        for index in range(math.ceil(rate * duration)):
            yield phase, start + index / rate, rng.choice(tickets)
        start += duration


def schedule_by_tier(rates, paid_rates, durations, tickets, seed, *, phases=PHASES):
    free = schedule(rates, durations, tickets, seed, phases=phases)
    paid = schedule(paid_rates, durations, tickets, f'{seed}:paid', phases=phases)
    return heapq.merge(
        ((task, 'free') for task in free), ((task, 'paid') for task in paid),
        key=lambda arrival: arrival[0][1],
    )


async def request(client, target, task, origin, timeout, tier='free'):
    phase, offset, ticket = task
    loop = asyncio.get_running_loop()
    started = loop.time()
    scheduled_at = origin + offset
    deadline = scheduled_at + timeout
    result = dict(phase=phase, tier=tier, scheduled_s=offset, input_chars=len(ticket['text']),
                  max_tokens=ticket['max_tokens'], dispatch_lag_s=max(0, started - scheduled_at),
                  ttft_s=None, status=None, outcome='incomplete')
    if started >= deadline:
        return dict(result, outcome='timeout', elapsed_s=0)
    payload = {
        'model': 'spikeguard', 'stream': True, 'temperature': 0,
        'max_tokens': ticket['max_tokens'],
        'messages': [
            {'role': 'system', 'content': 'Summarize the support ticket. Include the issue, impact, and next step.'},
            {'role': 'user', 'content': ticket['text']},
        ],
    }
    try:
        async with asyncio.timeout_at(deadline):
            headers = {TIER_HEADER: tier} if tier == 'paid' else None
            async with client.stream('POST', target, json=payload, headers=headers) as response:
                result['status'] = response.status_code
                if response.status_code == 429:
                    responded_at = loop.time()
                    result.update(outcome='http_error', retry_after=response.headers.get('retry-after'))
                    reason = await shed_reason(response)
                    if reason is not None:
                        result.update(outcome='shed', shed_reason=reason, elapsed_s=responded_at - started)
                elif not response.is_success:
                    result['outcome'] = 'http_error'
                else:
                    await read_events(response, result, started, deadline)
    except TimeoutError:
        if result['outcome'] == 'incomplete':
            result['outcome'] = 'timeout'
    except httpx.HTTPError:
        if result['outcome'] == 'incomplete':
            result['outcome'] = 'transport_error'
    except (ChatStreamError, SSELimitError):
        result['outcome'] = 'protocol_error'
    result.setdefault('elapsed_s', loop.time() - started)
    return result


async def shed_reason(response):
    if not response.headers.get('retry-after'):
        return None
    reason = response.headers.get(SHED_HEADER)
    if reason is not None:
        return reason if reason in SHED_REASONS else None

    # Earlier SpikeGuard releases identify admission rejections in a small JSON body.
    content_type = response.headers.get('content-type', '').split(';', 1)[0].strip().lower()
    if content_type != 'application/json':
        return None
    body = bytearray()
    async for chunk in response.aiter_bytes():
        if len(body) + len(chunk) > MAX_LEGACY_SHED_BODY_BYTES:
            return None
        body.extend(chunk)
    try:
        reason = json.loads(body).get('error', {}).get('code')
    except (ValueError, AttributeError, RecursionError):
        return None
    return reason if reason in SHED_REASONS else None


async def read_events(response, result, started, deadline):
    loop = asyncio.get_running_loop()
    stream = ChatStream()
    async for chunk in response.aiter_bytes():
        for marker in stream.feed(chunk):
            if marker == FIRST_CONTENT:
                result['ttft_s'] = loop.time() - started
            elif marker == DONE:
                if result['ttft_s'] is not None:
                    finished = loop.time()
                    if finished >= deadline:
                        raise TimeoutError
                    result.update(outcome='completed', elapsed_s=finished - started)
                return


def p95(values):
    values = sorted(values)
    if values:
        return values[math.ceil(0.95 * len(values)) - 1]
    return None


def summarize(rows, durations, *, phases=PHASES):
    summaries = {}
    for phase, duration in zip(phases, durations, strict=True):
        samples = [row for row in rows if row['phase'] == phase]
        outcomes = Counter(row['outcome'] for row in samples)
        latencies = [row['ttft_s'] for row in samples if row['outcome'] == 'completed']
        offered = len(samples)
        completed = len(latencies)
        shed = outcomes['shed']
        summaries[phase] = dict(
            offered=offered, outcomes=dict(outcomes),
            offered_per_second=offered / duration,
            completion_fraction=completed / offered if offered else None,
            completed_per_offered_second=completed / duration,
            shed_fraction=shed / offered if offered else None,
            error_fraction=(offered - completed - shed) / offered if offered else None,
            median_ttft_s=statistics.median(latencies) if latencies else None,
            p95_ttft_s=p95(latencies),
            p95_dispatch_lag_s=p95(row['dispatch_lag_s'] for row in samples if 'dispatch_lag_s' in row),
            p95_completion_latency_s=p95(
                row['elapsed_s'] for row in samples if row['outcome'] == 'completed' and 'elapsed_s' in row
            ),
            p95_shed_latency_s=p95(
                row['elapsed_s'] for row in samples if row['outcome'] == 'shed' and 'elapsed_s' in row
            ),
        )
    return summaries


def summarize_by_tier(rows, durations, *, phases=PHASES):
    return {
        tier: summarize([row for row in rows if row.get('tier', 'free') == tier], durations, phases=phases)
        for tier in TIERS
    }


async def measured_request(client, target, task, origin, timeout, tier, metrics):
    if metrics is not None:
        metrics.record_offer(task[0], tier)
    row = await request(client, target, task, origin, timeout, tier)
    if metrics is not None:
        metrics.record(row)
    return row


async def replay(args, tickets, metrics=None, *, phases=PHASES):
    loop = asyncio.get_running_loop()
    target = args.url.rstrip('/') + '/v1/chat/completions'
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=20)
    async with httpx.AsyncClient(timeout=None, limits=limits, trust_env=False) as client:
        origin = loop.time()
        paid_rates = getattr(args, 'paid_rates', (0,) * len(phases))
        traffic = schedule_by_tier(args.rates, paid_rates, args.durations, tickets, args.seed, phases=phases)
        async with asyncio.TaskGroup() as group:
            tasks = []
            for task, tier in traffic:
                await asyncio.sleep(max(0, origin + task[1] - loop.time()))
                pending = measured_request(client, target, task, origin, args.timeout, tier, metrics)
                tasks.append(group.create_task(pending))
            await asyncio.sleep(max(0, origin + sum(args.durations) - loop.time()))
        return [task.result() for task in tasks]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8080')
    parser.add_argument('--rates', type=float, nargs=3, default=(0.125, 0.25, 0.125),
                        help='Free requests per second in baseline, spike and recovery (default: %(default)s)')
    parser.add_argument('--paid-rates', type=float, nargs=3, default=(0, 0, 0),
                        help='Additional paid requests per second in baseline, spike and recovery')
    parser.add_argument('--durations', type=float, nargs=3, default=(8, 12, 8),
                        help='Seconds in baseline, spike and recovery (default: %(default)s)')
    parser.add_argument('--timeout', type=float, default=15)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--metrics-port', type=int, help='Serve live Prometheus metrics on loopback')
    parser.add_argument('--output', type=Path, default=Path('runs/direct.json'))
    args = parser.parse_args()
    if any(not math.isfinite(value) or value <= 0 for value in [*args.durations, args.timeout]):
        parser.error('durations and timeout must be positive and finite')
    rates = (*args.rates, *args.paid_rates)
    if any(not math.isfinite(value) or value < 0 for value in rates) or not any(rates):
        parser.error('rates must be finite and nonnegative, with at least one positive rate')
    if not is_loopback_endpoint(args.url):
        parser.error('the replay endpoint must be a loopback HTTP(S) base URL')
    if args.metrics_port is not None and not 1 <= args.metrics_port <= 65535:
        parser.error('metrics port must be between 1 and 65535')
    tickets = json.loads(Path(__file__).with_name('tickets.json').read_text(encoding='utf-8'))
    if args.metrics_port is None:
        rows = asyncio.run(replay(args, tickets))
    else:
        metrics = ReplayMetrics(PHASES)
        try:
            server, thread = metrics.start(args.metrics_port)
        except OSError as exc:
            parser.error(f'cannot start metrics exporter: {exc}')
        try:
            # With 2s scrapes, expose zero and final counters for at least two samples.
            time.sleep(5)
            rows = asyncio.run(replay(args, tickets, metrics))
            time.sleep(5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
    summary = summarize(rows, args.durations)
    tier_summary = summarize_by_tier(rows, args.durations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(
        url=args.url, rates=args.rates, paid_rates=args.paid_rates, durations=args.durations, seed=args.seed,
        timeout_s=args.timeout, measurement=measurement_metadata(tickets),
        summary=summary, tier_summary=tier_summary, requests=rows,
    ), indent=2) + '\n', encoding='utf-8')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
