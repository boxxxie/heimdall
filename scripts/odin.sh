#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SKADI_PATH="../skadi/skadi"
[[ -d "$SKADI_PATH" ]] || SKADI_PATH="./deps/skadi/skadi"

ACTOR_PATH="../sleipnir/actor"
[[ -d "$ACTOR_PATH" ]] || ACTOR_PATH="./deps/sleipnir/actor"

COLLECTIONS=(
  "-collection:skadi=${SKADI_PATH}"
  "-collection:actor=${ACTOR_PATH}"
)

cmd="${1:-test}"
shift || true

case "$cmd" in
  gen)
    odin run tools/heimdall_gen -vet -strict-style -vet-tabs -vet-packages:main,heimdall_codegen -- tests "$@"
    ;;
  check)
    odin check heimdall "${COLLECTIONS[@]}" -no-entry-point -vet -strict-style -vet-tabs -custom-attribute:telemetry_schema -vet-packages:heimdall "$@"
    odin check heimdall/codegen -no-entry-point -vet -strict-style -vet-tabs -vet-packages:heimdall_codegen "$@"
    ;;
  test)
    odin test tests "${COLLECTIONS[@]}" -vet -strict-style -vet-tabs -custom-attribute:telemetry_schema -vet-packages:heimdall_tests,heimdall,heimdall_codegen -define:ODIN_TEST_FAIL_ON_BAD_MEMORY=true "$@"
    ;;
  *)
    echo "usage: $0 {gen|check|test} [extra odin args...]" >&2
    exit 2
    ;;
esac
