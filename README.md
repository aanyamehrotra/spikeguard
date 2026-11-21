# SpikeGuard

SpikeGuard is a local load-shedding gateway project for LLM ticket summarization. Phase 1 provides a native inference launcher and a reproducible direct-to-server baseline.

## Run the baseline on Apple Silicon

```sh
brew install llama.cpp
./scripts/serve.sh
```

The first launch downloads Qwen 2.5 3B Instruct Q4_K_M (about 2.1 GB). The launcher enables Metal offload, two parallel slots, continuous batching and the Prometheus metrics endpoint. Run inference natively because Docker Desktop's Linux containers cannot use the Mac's Metal GPU. The gateway and observability stack are planned for Docker. On a GPU host, vLLM can replace llama.cpp while retaining the streaming client and repeating the capacity sweep.

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

See [AGENTS.md](AGENTS.md) for the draft SLO and test scenarios. The [measured direct run](benchmarks/README.md) reproduced queue growth and timeouts. Prometheus/Grafana provisioning, admission control and the comparison report are pending. Capacity on this Mac will differ from datacenter GPU capacity.

[llama.cpp server documentation](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md) · [Qwen model card](https://huggingface.co/Qwen/Qwen2.5-3B-Instruct-GGUF)
