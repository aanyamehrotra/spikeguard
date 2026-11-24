# Run SpikeGuard with a local model

Start with the [Python-only demo](../README.md#run-the-demo) to verify the gateway before loading model weights. The examples below use the virtual environment from the quick start.

## Start llama.cpp on Apple Silicon

Install native ARM64 llama.cpp and run the launcher from the repository root.

```sh
brew install llama.cpp
./scripts/serve.sh
```

The first launch downloads Qwen2.5 0.5B Instruct Q4_K_M (about 491 MB) and lets llama.cpp select GPU offload. The launcher uses two slots, a 4096-token context, two CPU threads, and a 128 MiB prompt-cache limit. [Homebrew formula](https://formulae.brew.sh/formula/llama.cpp) · [Qwen model file](https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/blob/main/qwen2.5-0.5b-instruct-q4_k_m.gguf)

| Launcher setting | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` | Loopback server port |
| `SLOTS` | `2` | Parallel inference slots |
| `CONTEXT` | `4096` | Total context tokens |
| `LLAMA_MODEL_REPO` | `Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M` | Download model and quantization |
| `LLAMA_MODEL_PATH` | unset | Load a local GGUF instead of downloading |
| `LLAMA_THREADS` | `2` | CPU thread count |
| `LLAMA_GPU_LAYERS` | `auto` | Select GPU offload. Set to `0` for CPU inference. |
| `LLAMA_CACHE_MIB` | `128` | Prompt-cache memory limit in MiB |

For the larger model used in the historical Apple Silicon runs, set `LLAMA_MODEL_REPO=Qwen/Qwen2.5-3B-Instruct-GGUF:Q4_K_M` and `CONTEXT=8192`. For Windows CPU, see the [native reproduction commands](../benchmarks/README.md#reproduce-the-cpu-pilot). For other installations, use a native llama.cpp build with an OpenAI-compatible chat endpoint on loopback. [llama.cpp server options](https://github.com/ggml-org/llama.cpp/blob/v1.0.0/tools/server/README.md)

## Compare direct and gateway replays

Run a direct replay once the model is healthy.

```sh
curl -fsS http://127.0.0.1:8080/health
.venv/bin/python replay.py --output runs/direct.json
```

In a separate terminal, start the Python gateway. Its default upstream is `http://127.0.0.1:8080`.

```sh
.venv/bin/python -m uvicorn gateway.app:app --host 127.0.0.1 --port 8081
```

Repeat the same workload through the gateway.

```sh
curl -fsS http://127.0.0.1:8081/ready
.venv/bin/python replay.py --url http://127.0.0.1:8081 --output runs/gateway.json
```

On Windows, use `.\.venv\Scripts\python.exe` and `curl.exe`. The Python gateway runs without Docker. The default replay sends five requests across 28 seconds. For a matched load test, set explicit `--rates` and `--durations`, preserve model settings, seed, and deadlines across runs, and let the upstream return to idle.

## Configure admission

Use one gateway worker so every request is subject to the same limit. Match `SPIKEGUARD_CONCURRENCY` to the model's slot count. Apply settings as environment variables before starting Uvicorn or Docker Compose.

| Gateway setting | Default | Purpose |
| --- | ---: | --- |
| `SPIKEGUARD_UPSTREAM` | `http://127.0.0.1:8080` | Upstream URL outside Docker |
| `SPIKEGUARD_CONCURRENCY` | 2 | Maximum active requests |
| `SPIKEGUARD_QUEUE_LIMIT` | 2 | Maximum waiting requests. Zero sheds immediately when full. |
| `SPIKEGUARD_QUEUE_WAIT_S` | 0.25 | Admission wait budget in seconds |
| `SPIKEGUARD_DEADLINE_S` | 25 | Request budget in seconds, including intake, queue wait and streaming |
| `SPIKEGUARD_PAID_SLOTS` | 0 | Active slots reserved for paid traffic. Leave at least one free slot. |
| `SPIKEGUARD_PAID_QUEUE_SLOTS` | 0 | Waiting places reserved for paid traffic |
| `SPIKEGUARD_ADAPTIVE` | 0 | Set to `1` to adjust the concurrency target |
| `SPIKEGUARD_ADAPTIVE_MIN` | paid slots + 1 | Lowest adaptive target |
| `SPIKEGUARD_ADAPTIVE_TARGET_S` | 1.5 | Upstream first-content threshold in seconds |
| `SPIKEGUARD_ADAPTIVE_RECOVERY_SAMPLES` | 8 | Fast samples admitted at saturation before raising the target |

A full queue or an expired wait returns `429` with `Retry-After: 1` and a reason in `X-SpikeGuard-Shed`. An end-to-end deadline before upstream headers returns `504`. The gateway closes a partial stream when its deadline expires after headers. Replay records an incomplete stream or timeout according to which deadline took effect first. Client disconnects cancel the upstream stream before the gateway releases its slot.

`/health` checks the gateway process, `/ready` probes the upstream, and `/metrics` exposes queue wait, upstream first-content latency, outcomes, and concurrency.

### Reserve paid capacity

With a two-slot upstream, start the gateway with one reserved upstream slot and one reserved waiting place. Then replay a free spike while holding the paid arrival rate steady.

```sh
SPIKEGUARD_PAID_SLOTS=1 SPIKEGUARD_PAID_QUEUE_SLOTS=1 \
  .venv/bin/python -m uvicorn gateway.app:app --host 127.0.0.1 --port 8081
```

```sh
.venv/bin/python replay.py --url http://127.0.0.1:8081 \
  --rates 0.125 2 0.125 --paid-rates 0.125 0.125 0.125 \
  --durations 60 60 60 --timeout 25 --output runs/priority.json
```

Paid requests may use spare shared slots and have priority when a slot opens. Free requests cannot borrow reserved places. Each lane stays FIFO. Replay uses `X-SpikeGuard-Tier: paid` to classify the independent paid stream on loopback. Use a trusted identity layer to assign tiers in an authenticated deployment.

### Adapt concurrency

Use a model with at least three slots to demonstrate adaptation with one reserved paid slot. With two total slots and one paid reservation, the minimum and maximum both equal two. For example, start the model with `SLOTS=4 ./scripts/serve.sh` and use the matching gateway ceiling.

```sh
SPIKEGUARD_CONCURRENCY=4 SPIKEGUARD_ADAPTIVE=1 \
  SPIKEGUARD_PAID_SLOTS=1 SPIKEGUARD_PAID_QUEUE_SLOTS=1 \
  .venv/bin/python -m uvicorn gateway.app:app --host 127.0.0.1 --port 8081
```

The controller starts at the configured maximum and halves its target toward the minimum on a slow upstream first-content sample, a pre-content timeout, or an upstream failure that prevents content. Queue waits, intentional admission rejections, and client disconnects are outside the feedback signal. A streak of fast samples admitted at saturation raises the target by one. In-flight requests drain after a reduction and the paid reservation stays available.

## Measure capacity

With the model and gateway running, the default sweep compares the direct and gateway URLs at two low rates, with 32-second measurement windows and one repeat. To test other endpoints, pass repeated `--target NAME=URL` options. Use `--paid-rate` for constant paid traffic and `--include-requests` to retain arrivals and outcomes, including warm-up.

This longer example offers at least 100 measured requests at the lowest rate in each repeat.

```sh
.venv/bin/python capacity.py --rates 0.05 0.1 0.2 \
  --warmup 60 --duration 2000 --repeats 2 --min-samples 100 \
  --environment 'Record CPU/GPU, llama.cpp version, model, quantization and slot count' \
  --output runs/native-capacity.json
```

The sweep offers requests on an open-loop schedule, excludes warm-up by scheduled arrival, and waits for outstanding requests at each trial boundary. It pairs ticket seeds across targets, rotates target order, and reports the offered rate, workload fingerprint, and dispatch lag. Use the same upstream settings and reset the gateway when changing its configuration.

A rate qualifies when each active tier and the aggregate achieve p95 first-content latency below 2 seconds, unexpected errors below 1% of offered requests, and at least 99% completion in every adequately sampled repeat. Read `highest_passing_rate` as the highest tested point under the recorded configuration. See the [benchmark reports](../benchmarks/README.md) for measured examples.

## Add Prometheus and Grafana with Docker Desktop

Keep llama.cpp running natively and stop the standalone gateway if it is using port 8081. Docker Desktop routes container requests to the host via `host.docker.internal`. [Docker Desktop networking](https://docs.docker.com/desktop/features/networking/networking-how-tos/#connect-a-container-to-a-service-on-the-host)

```sh
docker compose up -d --build
curl -fsS http://127.0.0.1:8081/ready
.venv/bin/python replay.py --url http://127.0.0.1:8081 \
  --metrics-port 9109 --output runs/gateway.json
```

Open [the provisioned Grafana dashboard](http://localhost:3000/d/spikeguard-overview). Use `admin` / `admin` on first login if the default credentials still apply, then set a new password. The [Prometheus targets page](http://localhost:9090/targets) shows whether scrapes are working. The replay exporter is available only while a replay with metrics enabled is running. It adds five seconds before and after the replay to capture initial and final samples.

Compare completion alongside p95 TTFT by tier. Gateway timings separate admission wait from upstream first content. The dashboard uses 12-second windows and terminal-outcome denominators. Report exact SLO figures from the JSON, which uses offered requests and assigns outcomes to their arrival phase. Run `docker compose down` to stop the stack.
