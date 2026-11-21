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
