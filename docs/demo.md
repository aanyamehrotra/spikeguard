# Show SpikeGuard in 90 seconds

Run the [Python quick start](../README.md#run-the-demo) before the interview. It uses a streaming fixture, so you can present on a laptop without Docker, a GPU, or model weights.

## Run it live

On Windows PowerShell, run the checked demo from the repository root.

```powershell
.\.venv\Scripts\python.exe -m scripts.demo --check --html runs/demo.html
Invoke-Item runs/demo.html
```

On macOS or Linux, run `.venv/bin/python -m scripts.demo --check --html runs/demo.html` and open the report in your browser. The terminal shows the spike comparison and verifies paid admission, recovery, retry headers, and gateway cleanup.

## What to show

1. **Explain the problem.** A streaming inference server has a fixed number of slots. Under load, queued requests can expire before the client sees any assistant content.
2. **Compare the spike.** The report compares the same arrivals against the direct path, a FIFO gateway, and a gateway with one reserved paid slot. Read completion and errors together with p95 time to first content. Ask what happened to requests that did not finish.
3. **Explain the trade-off.** Early `429` responses turn excess load into explicit rejections with a retry hint. A reserved paid slot protects admission while both tiers still share inference compute. Finish by showing the real llama.cpp measurements and the SLO criteria. [CPU pilot](../benchmarks/README.md#windows-cpu-pilot-with-llamacpp)

The fixture checks isolation with predictable streaming delays. In the separate Windows CPU pilot, direct llama.cpp timed out on all 76 requests at the higher offered rate. The priority gateway completed nine and intentionally shed 67. That trial measured overload behavior, and neither tested rate met the full SLO. [Trial records](../benchmarks/windows-cpu-pilot.json)

## Resume bullet

> Built a FastAPI gateway for streaming llama.cpp inference with deadline-aware admission, adaptive concurrency, and reserved paid capacity. In a matched CPU overload pilot, the gateway completed 9 of 76 requests and returned 67 explicit overload responses while direct llama.cpp timed out on all 76.

For implementation details, show the [admission queue](../gateway/admission.py), the [streaming relay](../gateway/app.py), or the [real HTTP cancellation test](../tests/test_demo.py).
