# SpikeGuard

SpikeGuard is a local load-shedding gateway project for LLM ticket summarization. Run a native model and a local streaming gateway, then replay the same traffic against both to measure overload.

## Run the baseline on Apple Silicon

```sh
brew install llama.cpp
./scripts/serve.sh
```

The first launch downloads Qwen 2.5 3B Instruct Q4_K_M (about 2.1 GB). The launcher enables Metal offload, two parallel slots, continuous batching and the Prometheus metrics endpoint. Run inference natively because Docker Desktop's Linux containers cannot use the Mac's Metal GPU. The gateway, Prometheus and Grafana run in Docker. On a GPU host, vLLM can replace llama.cpp while retaining the streaming client and repeating the capacity sweep.

In another terminal with Python 3.11 or newer:

```sh
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/metrics
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python replay.py --output runs/direct.json
```

The default baseline, spike and recovery offer 0.5, 8 and 0.5 requests per second over 30, 30 and 45 seconds. Tune `--rates` so baseline is healthy and the spike overloads your Mac. Requests arrive on schedule independently of previous completions. Keep rates, durations, seed and timeout fixed between direct and gateway runs. Warm the model before measurement and record the Mac hardware, llama.cpp version, model quantization and slot count.

The JSON records per-request dispatch lag, status, latency and outcome. TTFT starts at client dispatch and ends at the first nonempty assistant content. A completed request also needs the stream terminator. Phase summaries group requests by scheduled arrival and report nearest-rank p95 among completions, intentional shedding and errors. Completed requests per offered second includes completions after the phase ended. Compare completion fraction alongside p95, especially when requests time out.

## Replay through the gateway

Keep the native model running with two parallel slots. From the repo:

```sh
docker compose up -d --build
curl -fsS http://127.0.0.1:8081/ready
.venv/bin/python replay.py --url http://127.0.0.1:8081 --metrics-port 9109 --output runs/gateway.json
```

The gateway forwards the chat response as it arrives. Each request has a deadline covering its body, queue wait and upstream response. A full queue or an expired queue wait returns `429` with `Retry-After: 1`. A request deadline before upstream headers returns `504`. If the deadline hits after headers, the stream closes without a completion marker and the replay counts it as incomplete. A client disconnect removes its queue entry or closes its upstream stream before handing off the slot.

| Environment variable | Default | Controls |
| --- | ---: | --- |
| `SPIKEGUARD_CONCURRENCY` | 2 | Maximum simultaneous upstream requests. Match llama.cpp's slot count. |
| `SPIKEGUARD_QUEUE_LIMIT` | 2 | Number of waiting requests. Set to zero to reject immediately when all slots are busy. |
| `SPIKEGUARD_QUEUE_WAIT_S` | 0.25 | Maximum admission wait in seconds. |
| `SPIKEGUARD_DEADLINE_S` | 25 | End-to-end request budget in seconds. |

Run one gateway worker so all requests share the same in-process limit. `/health` checks the gateway process, `/ready` checks the upstream model, and `/metrics` reports active requests, queue depth, and rejections by reason. For a matched run, keep the replay seed, traffic schedule, model settings and client deadline fixed, and let the model return to idle between runs.

## Watch a replay in Grafana

With llama.cpp running on port 8080, start Docker Desktop and run these commands from the repo:

```sh
docker compose up -d
.venv/bin/python replay.py --metrics-port 9109 --output runs/direct.json
```

Open [SpikeGuard · Replay overview](http://localhost:3000/d/spikeguard-overview) in Grafana. On first login use Grafana's default `admin` / `admin` and set a new password. The [Prometheus targets page](http://localhost:9090/targets) shows whether scrapes are working. The `replay` target is available only while a replay with metrics enabled is running. Both services are published on loopback. Docker Desktop reaches the native endpoints through `host.docker.internal`.

The dashboard plots the gateway queue, llama.cpp's deferred requests and active slots alongside client p95 TTFT, offered and completed throughput, terminal outcomes, shed fraction and error fraction. It uses rolling 12-second windows for replay metrics. Completion metrics are tagged by the phase in which each request was dispatched, so late outcomes can appear after the next phase starts. The dashboard estimates p95 from histogram buckets and leaves it blank when no requests complete. Its shed and error fractions use terminal outcomes in the current window. Use the final JSON for exact run-wide and phase-wide counts and offered-request denominators.

Metrics mode leaves five seconds before the first request and after the last outcome so Prometheus can collect the initial and final counters. For a fresh run, zoom Grafana to the minutes around the replay. `docker compose down` stops monitoring and preserves its local volumes.

See [AGENTS.md](AGENTS.md) for the draft SLO and test scenarios. The [measured direct and gateway runs](benchmarks/README.md) show the trade-off between queueing and explicit rejection. Priority lanes, adaptive concurrency and the full capacity report are pending. Capacity on this Mac will differ from datacenter GPU capacity.

[llama.cpp server documentation](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md) · [Qwen model card](https://huggingface.co/Qwen/Qwen2.5-3B-Instruct-GGUF) · [Docker Desktop host connectivity](https://docs.docker.com/desktop/features/networking/networking-how-tos/)
