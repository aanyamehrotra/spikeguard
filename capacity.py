"""Measure the highest tested steady request rate that meets SpikeGuard's SLO."""
import argparse
import asyncio
import json
import math
from pathlib import Path

from replay import is_loopback_endpoint, measurement_metadata, replay, summarize, summarize_by_tier

PHASES = ('steady',)
SLO = dict(max_p95_ttft_s=2, max_error_fraction=0.01, min_completion_fraction=0.99)
DEFAULT_TARGETS = ('direct=http://127.0.0.1:8080', 'gateway=http://127.0.0.1:8081')


def assess(summary, min_samples):
    checks = dict(
        ttft=summary['p95_ttft_s'] is not None and summary['p95_ttft_s'] < SLO['max_p95_ttft_s'],
        errors=summary['error_fraction'] is not None and summary['error_fraction'] < SLO['max_error_fraction'],
        completion=summary['completion_fraction'] is not None
        and summary['completion_fraction'] >= SLO['min_completion_fraction'],
    )
    if summary['offered'] < min_samples:
        status = 'insufficient_samples'
    else:
        status = 'pass' if all(checks.values()) else 'fail'
    return dict(summary, checks=checks, status=status)


def summarize_trial(rows, warmup, duration, paid_rate, min_samples):
    measured = [row for row in rows if warmup <= row['scheduled_s'] < warmup + duration]
    totals = summarize(measured, (duration,), phases=PHASES)['steady']
    tiers = summarize_by_tier(measured, (duration,), phases=PHASES)
    aggregate = assess(totals, min_samples)
    by_tier = {tier: assess(tier_summary['steady'], min_samples) for tier, tier_summary in tiers.items()}
    statuses = {aggregate['status'], by_tier['free']['status']}
    if paid_rate > 0:
        statuses.add(by_tier['paid']['status'])
    if 'fail' in statuses:
        status = 'fail'
    elif 'insufficient_samples' in statuses:
        status = 'insufficient_samples'
    else:
        status = 'pass'
    return dict(status=status, aggregate=aggregate, by_tier=by_tier)


def summarize_rates(trials, rates, paid_rate):
    points = []
    for rate in rates:
        statuses = [trial['status'] for trial in trials if trial['free_rate_rps'] == rate]
        points.append(dict(free_rate_rps=rate, paid_rate_rps=paid_rate, total_rate_rps=rate + paid_rate,
                           all_repeats_pass=bool(statuses) and all(status == 'pass' for status in statuses),
                           repeat_statuses=statuses))
    passing = [point for point in points if point['all_repeats_pass']]
    return dict(rates=points, highest_passing_rate=passing[-1] if passing else None)


async def sweep(args, targets, tickets):
    results = {label: dict(url=url, trials=[]) for label, url in targets.items()}
    target_order = tuple(targets.items())
    duration = args.warmup + args.duration
    execution_order = 0
    for point, rate in enumerate(args.rates):
        for repeat in range(args.repeats):
            seed = args.seed + repeat
            # Rotate the first target so the same path does not always inherit a warmed backend.
            first_target = (point + repeat) % len(target_order)
            for label, url in target_order[first_target:] + target_order[:first_target]:
                if execution_order:
                    await asyncio.sleep(args.cooldown)
                execution_order += 1
                traffic = argparse.Namespace(
                    url=url, rates=(rate,), paid_rates=(args.paid_rate,), durations=(duration,),
                    timeout=args.timeout, seed=seed,
                )
                rows = await replay(traffic, tickets, phases=PHASES)
                trial = summarize_trial(rows, args.warmup, args.duration, args.paid_rate, args.min_samples)
                trial.update(free_rate_rps=rate, repeat=repeat + 1, seed=seed, execution_order=execution_order)
                if args.include_requests:
                    trial['requests'] = rows
                results[label]['trials'].append(trial)
                print(f'{label} at {rate + args.paid_rate:g} req/s, repeat {repeat + 1}: {trial["status"]}', flush=True)
    for result in results.values():
        result.update(summarize_rates(result['trials'], args.rates, args.paid_rate))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', action='append', metavar='NAME=URL', help='Loopback base URL. Repeat for comparisons.')
    parser.add_argument('--rates', type=float, nargs='+', default=(0.05, 0.1, 0.2, 0.4, 0.8),
                        help='Increasing free request rates to test')
    parser.add_argument('--paid-rate', type=float, default=0, help='Additional paid requests per second at every point')
    parser.add_argument('--warmup', type=float, default=30, help='Seconds to exclude at the start of each run')
    parser.add_argument('--duration', type=float, default=120, help='Measured seconds per run after warm-up')
    parser.add_argument('--cooldown', type=float, default=2, help='Idle seconds between runs, after outstanding requests end')
    parser.add_argument('--timeout', type=float, default=25, help='Client deadline in seconds per request')
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--min-samples', type=int, default=5,
                        help='Minimum measured offers per active tier and repeat (default: %(default)s for a pilot sweep)')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--environment', help='Hardware, model, slots and gateway settings to record with the results')
    parser.add_argument('--include-requests', action='store_true', help='Include every request including warm-up in the JSON')
    parser.add_argument('--output', type=Path, default=Path('runs/capacity.json'))
    args = parser.parse_args()

    if any(not math.isfinite(rate) or rate <= 0 for rate in args.rates) or list(args.rates) != sorted(set(args.rates)):
        parser.error('free rates must be finite, positive, and strictly increasing')
    if any(not math.isfinite(value) or value < 0 for value in (args.warmup, args.cooldown, args.paid_rate)):
        parser.error('warmup, cooldown, and paid rate must be finite and nonnegative')
    if any(not math.isfinite(value) or value <= 0 for value in (args.duration, args.timeout)):
        parser.error('duration and timeout must be finite and positive')
    if args.repeats < 1 or args.min_samples < 1:
        parser.error('repeats and minimum sample count must be positive')
    targets = {}
    for value in args.target or DEFAULT_TARGETS:
        label, _, url = value.partition('=')
        if not label or label in targets or not is_loopback_endpoint(url):
            parser.error('each target must have a unique name and a loopback HTTP(S) base URL')
        targets[label] = url

    tickets = json.loads(Path(__file__).with_name('tickets.json').read_text(encoding='utf-8'))
    results = asyncio.run(sweep(args, targets, tickets))
    report = dict(
        slo=SLO, free_rates_rps=args.rates, paid_rate_rps=args.paid_rate, warmup_s=args.warmup,
        measured_s=args.duration, cooldown_s=args.cooldown, timeout_s=args.timeout, repeats=args.repeats,
        min_samples=args.min_samples, seed=args.seed, environment=args.environment,
        measurement=measurement_metadata(tickets), targets=results,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    for label, result in results.items():
        highest = result['highest_passing_rate']
        description = f'{highest["total_rate_rps"]:g} req/s' if highest else 'none'
        print(f'{label} highest passing rate: {description}')


if __name__ == '__main__':
    main()
