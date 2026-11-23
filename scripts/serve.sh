#!/usr/bin/env bash
set -euo pipefail

model_args=(--hf-repo "${LLAMA_MODEL_REPO:-Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M}")
if [[ -n ${LLAMA_MODEL_PATH:-} ]]; then
  model_args=(--model "$LLAMA_MODEL_PATH")
fi

exec llama-server \
  --host 127.0.0.1 --port "${PORT:-8080}" \
  "${model_args[@]}" --alias spikeguard \
  --parallel "${SLOTS:-2}" --ctx-size "${CONTEXT:-4096}" \
  --threads "${LLAMA_THREADS:-2}" --cache-ram "${LLAMA_CACHE_MIB:-128}" \
  --n-gpu-layers "${LLAMA_GPU_LAYERS:-auto}" --cont-batching --metrics
