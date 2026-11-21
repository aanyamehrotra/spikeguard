#!/usr/bin/env bash
set -euo pipefail
exec llama-server \
  --host 127.0.0.1 --port "${PORT:-8080}" \
  --hf-repo Qwen/Qwen2.5-3B-Instruct-GGUF:Q4_K_M \
  --alias spikeguard --parallel "${SLOTS:-2}" --ctx-size "${CONTEXT:-8192}" \
  --n-gpu-layers 99 --cont-batching --metrics
