"""Replay synthetic support tickets against a local streaming chat endpoint."""
import argparse
import asyncio
from collections import Counter
import json
import math
from pathlib import Path
import random
import statistics

import httpx

PHASES = ('baseline', 'spike', 'recovery')


def schedule(rates, durations, tickets, seed):
    rng = random.Random(seed)
    start = 0
    for phase, rate, duration in zip(PHASES, rates, durations, strict=True):
        for index in range(math.ceil(rate * duration)):
            yield phase, start + index / rate, rng.choice(tickets)
        start += duration


async def request(client, target, task, origin, timeout):
    phase, offset, ticket = task
    loop = asyncio.get_running_loop()
    started = loop.time()
    result = dict(phase=phase, scheduled_s=offset, input_chars=len(ticket['text']),
                  max_tokens=ticket['max_tokens'], dispatch_lag_s=started-origin-offset,
                  ttft_s=None, status=None, outcome='incomplete')
    payload = {
        'model': 'spikeguard', 'stream': True, 'temperature': 0,
        'max_tokens': ticket['max_tokens'],
        'messages': [
            {'role': 'system', 'content': 'Summarize the support ticket. Include the issue, impact, and next step.'},
            {'role': 'user', 'content': ticket['text']},
        ],
    }
    try:
        async with asyncio.timeout_at(origin + offset + timeout):
            async with client.stream('POST', target, json=payload) as response:
                result['status'] = response.status_code
                if response.status_code == 429:
                    result['outcome'] = 'shed' if 'retry-after' in response.headers else 'http_error'
                    result['retry_after'] = response.headers.get('retry-after')
                elif not response.is_success:
                    result['outcome'] = 'http_error'
                else:
                    async for line in response.aiter_lines():
                        if not line.startswith('data:'):
                            continue
                        data = line[5:].strip()
                        if data == '[DONE]':
                            if result['ttft_s'] is not None:
                                result['outcome'] = 'completed'
                            break
                        for choice in json.loads(data).get('choices', []):
                            if choice.get('delta', {}).get('content') and result['ttft_s'] is None:
                                result['ttft_s'] = loop.time() - started
    except TimeoutError:
        result['outcome'] = 'timeout'
    except httpx.HTTPError:
        result['outcome'] = 'transport_error'
    except (ValueError, TypeError, AttributeError):
        result['outcome'] = 'protocol_error'
    result['elapsed_s'] = loop.time() - started
    return result


def summarize(rows, durations):
    summaries = {}
    for phase, duration in zip(PHASES, durations, strict=True):
        samples = [row for row in rows if row['phase'] == phase]
        outcomes = Counter(row['outcome'] for row in samples)
        latencies = sorted(row['ttft_s'] for row in samples if row['outcome'] == 'completed')
        offered = len(samples)
        completed = len(latencies)
        shed = outcomes['shed']
        summaries[phase] = dict(
            offered=offered, outcomes=dict(outcomes),
            completion_fraction=completed / offered if offered else None,
            completed_per_offered_second=completed / duration,
            shed_fraction=shed / offered if offered else None,
            error_fraction=(offered - completed - shed) / offered if offered else None,
            median_ttft_s=statistics.median(latencies) if latencies else None,
            p95_ttft_s=latencies[math.ceil(0.95 * completed) - 1] if latencies else None,
        )
    return summaries


async def replay(args, tickets):
    tasks = []
    loop = asyncio.get_running_loop()
    target = args.url.rstrip('/') + '/v1/chat/completions'
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=20)
    async with httpx.AsyncClient(timeout=None, limits=limits, trust_env=False) as client:
        origin = loop.time()
        for task in schedule(args.rates, args.durations, tickets, args.seed):
            await asyncio.sleep(max(0, origin + task[1] - loop.time()))
            tasks.append(asyncio.create_task(request(client, target, task, origin, args.timeout)))
        return await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8080')
    parser.add_argument('--rates', type=float, nargs=3, default=(0.5, 8, 0.5))
    parser.add_argument('--durations', type=float, nargs=3, default=(30, 30, 45))
    parser.add_argument('--timeout', type=float, default=15)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--output', type=Path, default=Path('runs/direct.json'))
    args = parser.parse_args()
    if any(not math.isfinite(value) or value <= 0 for value in [*args.rates, *args.durations, args.timeout]):
        parser.error('rates, durations and timeout must be positive and finite')
    if httpx.URL(args.url).host not in {'localhost', '127.0.0.1', '::1'}:
        parser.error('the replay endpoint must be on loopback')
    tickets = json.loads(Path(__file__).with_name('tickets.json').read_text())
    rows = asyncio.run(replay(args, tickets))
    summary = summarize(rows, args.durations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(
        url=args.url, rates=args.rates, durations=args.durations, seed=args.seed,
        timeout_s=args.timeout, summary=summary, requests=rows,
    ), indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
