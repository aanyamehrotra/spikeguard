# SpikeGuard

Run llama.cpp natively on Apple Silicon with Metal, parallel slots and continuous batching. The load client drives either llama.cpp directly or a FastAPI streaming proxy. Run the gateway, Prometheus and Grafana in Docker. Keep phase changes independently reviewable.

## Draft SLO

For each measurement window, target p95 client time to first nonempty assistant content below 2 seconds among completed requests. Count timeouts, incomplete streams and unexpected HTTP errors against a budget under 1% of offered requests. Report intentional 429s separately and include completion and shed rates alongside latency to expose selection bias. Report paid and free tiers separately when lanes are introduced. For capacity, require at least 99% completion in steady state, so shedding cannot inflate capacity.

## Scenarios

Compare identical seeded, open-loop baseline, spike and recovery schedules. Check low-load streaming, overload, timeouts, and recovery. Later phases must also verify queue bounds, fast 429s with Retry-After, deadline and disconnect cleanup, and paid-lane isolation under overload. Run tests before finishing.
