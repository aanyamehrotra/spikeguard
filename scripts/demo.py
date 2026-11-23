"""Compare direct, FIFO, and priority streaming with a local two-slot fixture."""
import argparse
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
import json
from pathlib import Path
import socket

import httpx
from prometheus_client.parser import text_string_to_metric_families
import uvicorn

from benchmarks.mock_upstream import create_mock_upstream
from gateway.app import Settings, create_app
from replay import replay, summarize, summarize_by_tier

RATES = (4, 24, 4)
PAID_RATES = (2, 2, 2)
DURATIONS = (1, 3, 1)
SEED = 42
TIMEOUT = 1.5
ROOT = Path(__file__).resolve().parents[1]


class EmbeddedServer(uvicorn.Server):
    def capture_signals(self):
        # asyncio.run owns Ctrl-C while the fixture and gateway share its loop.
        return nullcontext()


@asynccontextmanager
async def serve(app):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
        server = EmbeddedServer(uvicorn.Config(
            app, log_level='warning', access_log=False, ws='none', proxy_headers=False,
            timeout_graceful_shutdown=2,
        ))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not server.started and not task.done():
                    await asyncio.sleep(0.01)
            if task.done():
                await task
                raise RuntimeError('the demo server exited before becoming ready')
            yield f'http://127.0.0.1:{port}'
        finally:
            server.should_exit = True
            await task


async def run_demo(tickets):
    targets = {}
    for name in ('direct', 'fifo', 'priority'):
        async with AsyncExitStack() as stack:
            upstream = await stack.enter_async_context(serve(create_mock_upstream()))
            url = upstream
            if name != 'direct':
                reserve = int(name == 'priority')
                settings = Settings(upstream=upstream, paid_slots=reserve, paid_queue_slots=reserve)
                url = await stack.enter_async_context(serve(create_app(settings)))

            client = await stack.enter_async_context(httpx.AsyncClient(base_url=url, trust_env=False))
            readiness = '/health' if name == 'direct' else '/ready'
            (await client.get(readiness)).raise_for_status()
            traffic = argparse.Namespace(
                url=url, rates=RATES, paid_rates=PAID_RATES, durations=DURATIONS, seed=SEED, timeout=TIMEOUT,
            )
            rows = await replay(traffic, tickets)
            result = dict(summary=summarize(rows, DURATIONS), tier_summary=summarize_by_tier(rows, DURATIONS), requests=rows)
            if name != 'direct':
                response = await client.get('/metrics')
                response.raise_for_status()
                result['gateway_drained'] = {
                    sample.name: sample.value
                    for metric in text_string_to_metric_families(response.text)
                    for sample in metric.samples
                    if sample.name in {'spikeguard_gateway_active', 'spikeguard_gateway_queue_depth'}
                }
            targets[name] = result
    return dict(
        upstream='Two-slot streaming fixture (40 ms to first content, 140 ms slot occupancy before I/O)',
        free_rates_rps=RATES, paid_rates_rps=PAID_RATES, durations_s=DURATIONS, seed=SEED,
        client_timeout_s=TIMEOUT, targets=targets,
    )


def check_report(report):
    targets = report['targets']
    schedules = [
        [(row['phase'], row['tier'], row['scheduled_s'], row['input_chars'], row['max_tokens']) for row in result['requests']]
        for result in targets.values()
    ]
    priority = targets['priority']
    paid_spike = priority['tier_summary']['paid']['spike']
    free_spike = priority['tier_summary']['free']['spike']
    drained = {'spikeguard_gateway_active': 0, 'spikeguard_gateway_queue_depth': 0}
    checks = {
        'identical offered traffic': bool(schedules[0]) and all(schedule == schedules[0] for schedule in schedules),
        'priority paid traffic completed through the spike': paid_spike['completion_fraction'] == 1,
        'priority shed excess free traffic': free_spike['shed_fraction'] > 0,
        'priority kept serving free traffic': free_spike['completion_fraction'] > 0,
        'priority served both tiers in recovery': all(
            tier['recovery']['completion_fraction'] > 0 for tier in priority['tier_summary'].values()
        ),
        'priority recorded zero unexpected spike errors': priority['summary']['spike']['error_fraction'] == 0,
        'gateway overload responses carried Retry-After': all(
            row.get('retry_after') == '1'
            for name in ('fifo', 'priority') for row in targets[name]['requests'] if row['outcome'] == 'shed'
        ),
        'both gateways drained': all(targets[name]['gateway_drained'] == drained for name in ('fifo', 'priority')),
    }
    return [description for description, passed in checks.items() if not passed]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Fail if core overload and recovery invariants are violated')
    parser.add_argument('--output', type=Path, default=Path('runs/demo.json'), help='Write the JSON report to this path')
    args = parser.parse_args()

    tickets = json.loads((ROOT / 'tickets.json').read_text(encoding='utf-8'))
    print('Running three matched 5-second replays against a local streaming fixture...', flush=True)
    report = asyncio.run(run_demo(tickets))
    print('\nSpike results')
    print(f'{"Path":<11} {"Tier":<6} {"Complete":>10} {"Shed":>5} {"Errors":>6} {"p95 TTFT":>10}')
    for name, result in report['targets'].items():
        for tier, summary in result['tier_summary'].items():
            spike = summary['spike']
            counts = spike['outcomes']
            completed = counts.get('completed', 0)
            shed = counts.get('shed', 0)
            errors = spike['offered'] - completed - shed
            ttft = spike['p95_ttft_s']
            latency = f'{ttft:.3f}s' if ttft is not None else '-'
            fraction = f'{completed}/{spike["offered"]}'
            print(f'{name:<11} {tier:<6} {fraction:>10} {shed:>5} {errors:>6} {latency:>10}')

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'\nReport written to {args.output}')
    if args.check:
        failures = check_report(report)
        if failures:
            parser.exit(1, 'Demo checks failed\n- ' + '\n- '.join(failures) + '\n')
        print('Demo checks passed')


if __name__ == '__main__':
    main()
