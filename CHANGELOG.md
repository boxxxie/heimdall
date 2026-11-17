# Changelog

All notable changes to **Heimdall** (`package heimdall`) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

---

## [0.1.0] - 2026-11-17

### Added
- **Core `#soa` Ring Telemetry Engine (`heimdall/heimdall.odin`, `heimdall/schema_gen.odin`):**
  - 11-column OpenTelemetry-compatible `@(telemetry_schema) Record` persisted directly to Apache Arrow IPC (`.arrow`) segments via Skadi without AoS-to-SoA transposition.
  - Zero-cost short-circuit (`0 B` allocations) when passed `nil` or a disabled `^heimdall.Recorder`.
  - Scoped RAII spans (`heimdall.scoped_span` via `@(deferred_out = span_end)`), point metrics (`counter_add`, `gauge_set`, `histogram_record`), and events (`event_emit`) with thread-local `(Recorder, _instance_id, actor_env)` span tree isolation.
  - Adaptive `Backend_Mode.Auto` selecting between `.Dedicated_Thread` and `.Sleipnir_Actor` (with explicit overrides and `.Drop` / `.Block` backpressure policies).
- **Per-Page Bump Slab for `string_fallback_copy` (`heimdall/heimdall.odin`):**
  - Lazy-allocated `128 KB` per-`Staging_Page` string slab (`PAGE_STRING_SLAB_CAP`) with deferred slot reservation after `acquire_free_page_locked` and `O(1)` reset in `free_page_strings`, cutting dynamic-string `event_emit` latency from `727.40 ns/rec` (`65,536` allocs) to `120.08 ns/rec` (`2` slab allocs) while falling back cleanly to heap cloning for `scoped_span` and slab overflows.
- **AST Metaprogramming & 4-Layer ATDD Suite (`heimdall/codegen/`, `tools/heimdall_gen/`, `tests/`):**
  - `core:odin/parser` codegen for `@(telemetry_schema)` Arrow serializers, 4-layer ATDD DSL wrappers, and static `scoped_span` call-site linting (`29/29` specs passing with zero leaks).
  - Expanded `auto_detect_actor_receive` to flush a full `BATCH_CAP` (`4096`) page inside a 1-worker actor turn before `recorder_destroy`.
- **Documentation & Benchmarks (`README.md`, `usage-rules.md`, `docs/`, `benchmarks/`):**
  - Complete API reference, architecture/ATDD guide, usage rules, tuning guidance for `string_fallback_copy` and `ring_page_count`, and 7-section benchmark suite.

### Dependencies
- **Skadi `v0.2.4` (`deps/skadi`):** Includes Arrow IPC multi-batch scratch bounding, per-batch vectored writer flushing, `os.fstat`-guarded truncation, steady-state `Buffer_Pool` bucket retention, and vendored Sleipnir `v1.1.3` sync.
- **Sleipnir `v1.1.3` (`deps/sleipnir`):** Includes cooperative `call` / `try_call` stepping, OTP supervision, and cooperative shard-drain wait loops in `actor_destroy` / `actor_base_destroy_sched`.

[Unreleased]: https://github.com/boxxxie/heimdall/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/boxxxie/heimdall/releases/tag/v0.1.0
