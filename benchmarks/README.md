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
