# Direct-to-model overload run

Measured on an Apple Silicon Mac using Homebrew llama.cpp 1.0.0, Qwen 2.5 3B Instruct Q4_K_M, two parallel slots and continuous batching. The client used seed 42 and a 15-second end-to-end deadline.

```sh
python replay.py --rates 0.5 8 0.5 --durations 12 15 24 --output runs/direct.json
```

| Phase | Offered | Completed | Timed out | Completed p95 TTFT |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 6 | 2 | 4 | 2.63 s |
| Spike | 120 | 0 | 120 | — |
| Recovery | 12 | 1 | 11 | 11.27 s |

One Prometheus scrape during the run observed two processing requests and 30 deferred requests. This is a point-in-time observation. Spike p95 is undefined because no spike request completed.

The starting rate already exceeded the SLO in this run. Treat it as an overload reproduction and use a lower starting rate for the capacity sweep. The small baseline and recovery samples also make their p95 estimates noisy. See [raw results](direct-overload.json).

## Live observability smoke test

This direct replay verified that Prometheus could scrape both native processes and that Grafana's provisioned dashboard returned data for all six panels. Prometheus sampled a peak of 22 deferred requests.

```sh
docker compose up -d
python replay.py --metrics-port 9109 --rates 0.125 2 0.125 \
  --durations 16 12 24 --timeout 25 --output runs/observability-smoke.json
```

| Phase | Offered | Completed | Timed out | Completed p95 TTFT |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 2 | 2 | 0 | 2.05 s |
| Spike | 24 | 4 | 20 | 17.81 s |
| Recovery | 3 | 0 | 3 | — |

The table uses exact values from the [replay JSON](observability-smoke.json). Grafana estimates p95 from histogram buckets over rolling windows. The baseline sample is too small for a capacity claim.

## Gateway v1 comparison

Two gateway runs used the same seeded ticket sequence, offered schedule, and 25-second client deadline as the earlier direct observability run. All runs used the same native model and two llama.cpp slots. Gateway settings were two upstream requests, two waiting requests, a 250 ms queue-wait budget and a 25-second request deadline. The direct result is historical and the model cache was allowed to persist across runs.

```sh
docker compose up -d --build
python replay.py --url http://127.0.0.1:8081 --metrics-port 9109 \
  --rates 0.125 2 0.125 --durations 16 12 24 --timeout 25 --output runs/gateway-v1.json
```

Each spike offered 24 requests at 2 requests/sec.

| Spike path | Completed | Timed out | Shed | Completed p95 TTFT | Shed median | Shed p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Direct | 4 | 20 | 0 | 17.81 s | — | — |
| Gateway run 1 | 3 | 0 | 21 | 0.56 s | 0.39 s | 2.39 s |
| Gateway run 2 | 5 | 0 | 19 | 0.37 s | 0.26 s | 0.32 s |

The native deferred queue peaked at 22 sampled requests during the direct run and stayed at zero in both gateway runs. Gateway queue depth peaked at two and one respectively in the two-second samples. Every gateway rejection included `Retry-After: 1`. The 250 ms wait applies inside the admission queue. The first run's client-observed tail shows end-to-end rejection latency can exceed that budget.

The gateway kept completed spike requests below the 2-second TTFT target in these runs, while rejecting 79–88% of offered spike traffic. The resulting completion counts are small, and shedding persisted into recovery. These runs demonstrate admission control under overload. A steady-state rate sweep is needed to determine capacity under the completion SLO.

Raw traces are available for the [direct run](observability-smoke.json), [gateway run 1](gateway-v1.json), and [gateway run 2](gateway-v1-repeat.json).

## Steady-state sweep with a streaming fixture

This smoke test ran on Windows with Python 3.12 and a [two-slot SSE fixture](mock_upstream.py). Each request occupies a fixture slot for 140 ms plus scheduling and I/O. The FIFO gateway used the default settings. The priority gateway reserved one of its two upstream slots and one of its two queue places for paid traffic. Both gateways used fixed concurrency. Each point ran twice with one second of warm-up, five measured seconds and a two-second client deadline.

| Path | Verdict at 7 req/s | Verdict at 22 req/s | Free completed at 22 req/s | Paid completed at 22 req/s |
| --- | --- | --- | ---: | ---: |
| Direct | Pass, both repeats | Fail, both repeats | 78 / 200 | 8 / 20 |
| FIFO gateway | Pass, both repeats | Fail, both repeats | 119 / 200 | 6 / 20 |
| Priority gateway | Pass, both repeats | Fail, both repeats | 61 / 200 | 20 / 20 |

At 22 req/s, direct replay timed out on 134 requests. The FIFO and priority gateways returned 95 and 139 intentional 429s, respectively, with zero unexpected errors. The priority gateway kept paid p95 TTFT at or below 0.08 seconds in both repeats. The aggregate failed the 99% completion target on all three paths. Seven requests per second was the highest passing point of the two tested. These short trials exercise the sweep and show the effect of reserving a lane. Apple Silicon capacity still needs a longer sweep on the native model. See the [recorded JSON](synthetic-capacity.json).

To reproduce the fixture run, use three terminals at the repository root with the requirements installed. Start the fixture, then the FIFO and priority gateways.

```sh
python -m uvicorn benchmarks.mock_upstream:app --port 18080
```

```sh
SPIKEGUARD_UPSTREAM=http://127.0.0.1:18080 python -m uvicorn gateway.app:app --port 18081
```

```sh
SPIKEGUARD_UPSTREAM=http://127.0.0.1:18080 SPIKEGUARD_PAID_SLOTS=1 SPIKEGUARD_PAID_QUEUE_SLOTS=1 \
  python -m uvicorn gateway.app:app --port 18082
```

Run the sweep from another terminal.

```sh
python capacity.py --target direct=http://127.0.0.1:18080 \
  --target fifo=http://127.0.0.1:18081 --target priority=http://127.0.0.1:18082 \
  --rates 5 20 --paid-rate 2 --warmup 1 --duration 5 --cooldown 0.5 \
  --timeout 2 --repeats 2 --min-samples 8 --output runs/synthetic-capacity.json
```
