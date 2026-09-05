#!/usr/bin/env bash
set -euo pipefail

task="${TASK:-regex-log}"
attempts="${ATTEMPTS:-16}"
concurrency="${CONCURRENCY:-16}"
repetitions="${REPETITIONS:-3}"
read -r -a providers <<<"${PROVIDERS:-smol-branch smol-cold}"
stamp="$(date -u +%Y%m%d-%H%M%S)"
result="results/${stamp}-${task}.json"
report="results/${stamp}-${task}.html"

uv run python bench/harbor_fanout.py \
  --task "$task" \
  --attempts "$attempts" \
  --concurrency "$concurrency" \
  --repetitions "$repetitions" \
  --providers "${providers[@]}" \
  --output "$result" \
  "$@"

uv run python bench/render_results.py "$result" --output "$report"
printf 'Open %s\n' "$report"
