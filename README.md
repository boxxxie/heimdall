# Heimdall

[![CI](https://github.com/boxxxie/heimdall/actions/workflows/ci.yml/badge.svg)](https://github.com/boxxxie/heimdall/actions/workflows/ci.yml)

**Heimdall** is a high-throughput, low-latency telemetry ingestion engine for [Odin](https://odin-lang.org/), backed by `#soa` staging ring pages, zero-copy [Skadi](https://github.com/boxxxie/skadi) (Apache Arrow) IPC columnar persistence, and adaptive background execution via either a dedicated OS worker thread or a [Sleipnir](https://github.com/boxxxie/sleipnir) actor scheduler.

Heimdall is designed so that third-party Odin libraries and applications can instrument hot paths with spans, counters, gauges, histograms, and events at nanosecond overhead—dropping to a zero-allocation no-op when disabled or passed a `nil` recorder.

---

## Key Features

* **Zero-Cost When Disabled or `nil`:** Passing `nil` or a disabled `^heimdall.Recorder` short-circuits before reading clocks or acquiring locks (`0 B` allocations).
* **`#soa` Columnar Staging Ring:** Producers write directly into cache-line-isolated `#soa[4096]Record` (`BATCH_CAP :: 4096`) staging pages, eliminating AoS-to-SoA transposition during Apache Arrow serialization.
* **Single-Line Scoped RAII Spans:** `@(deferred_out = span_end)` on `heimdall.scoped_span` automatically captures monotonic duration, links nested cross-library calls via thread-local `parent_id -> span_id` trees, and emits the span when the enclosing lexical scope exits.
* **Multi-Recorder & Cooperative Actor Turn Isolation:** Thread-local active span slots are keyed by `(Recorder, _instance_id, actor_env)` (`MAX_TLS_RECORDERS :: 16`) with batched 64-ID reservations (`SPAN_ID_BATCH_SIZE :: 64`), preventing cross-recorder or cooperative `actor.call` parent-span pollution on shared OS worker threads.
* **Adaptive Execution Backends (`.Auto`):** Automatically selects between `.Dedicated_Thread` and `.Sleipnir_Actor` based on whether `recorder_init` is called inside a live Sleipnir actor turn with spare worker capacity, while supporting explicit overrides.
* **Configurable Backpressure Policies:** Choose `.Drop` for real-time/audio/game loops that must never block the caller, or `.Block` for lossless telemetry pipelines.
* **AST Metaprogramming & Static Linter (`tools/heimdall_gen`):** Uses `core:odin/parser` and `core:odin/ast` to generate `@(telemetry_schema)` Arrow serializers (`heimdall/schema_gen.odin`), ATDD DSL harnesses (`tests/dsl_atdd_gen.odin`), and static call-site diagnostics (catching misplaced `scoped_span` calls and uncloned dynamic strings).

---

## Quick Start

### 1. Instrumenting Library & Application Code

Libraries accept an optional `rec: ^heimdall.Recorder` (or read `heimdall.default_recorder()`):

```odin
package my_service

import "heimdall"

handle_http_request :: proc(rec: ^heimdall.Recorder, payload_bytes: int) {
	// Automatically begins a span and records its duration when `handle_http_request` exits
	heimdall.scoped_span(rec, "net.http", "handle_request", i64(payload_bytes))

	heimdall.counter_add(rec, "net.http", "requests_total", 1.0, i64(payload_bytes))

	if !execute_db_query(rec, 42) {
		// Marks the currently active span ("handle_request") as Status.Error with tag 500
		heimdall.span_set_error(rec, 500)
	}
}

execute_db_query :: proc(rec: ^heimdall.Recorder, query_id: i64) -> bool {
	// Automatically links `parent_id` to the enclosing "handle_request" span on this thread
	heimdall.scoped_span(rec, "db.driver", "execute_query", query_id)
	heimdall.gauge_set(rec, "db.driver", "pool_connections_active", 8.0, query_id)
	return true
}
```

### 2. Initializing & Flushing the Recorder

```odin
package main

import "heimdall"

main :: proc() {
	rec: heimdall.Recorder
	err := heimdall.recorder_init(&rec, heimdall.Config{
		file_prefix          = "logs/service_telemetry", // Writes logs/service_telemetry_000000.arrow, ...
		batches_per_file     = 16,                       // 16 * 4,096 = 65,536 rows per .arrow file
		ring_page_count      = 4,                        // 4 #soa pages in ring (use 8-16 for bursty .Block workloads)
		string_fallback_copy = false,                    // Keep false for 0 B alloc hot paths; set true only for dynamic strings
		policy               = .Block,                   // .Block (lossless) or .Drop (real-time non-blocking)
		backend              = .Auto,                    // .Auto, .Dedicated_Thread, or .Sleipnir_Actor
	})
	assert(err == nil)
	defer heimdall.recorder_destroy(&rec)

	// Optional: register as global default recorder
	heimdall.set_default_recorder(&rec)

	// Emit telemetry...
	heimdall.event_emit(&rec, "service", "startup_complete", 1, .Ok)

	// Optional live checkpoint flush while recorder remains active
	heimdall.recorder_flush(&rec)
}
```

---

## Execution Backends & `.Auto` Selection Matrix

`Config.backend` defaults to `Backend_Mode.Auto` (`0`), which inspects `Config` and `actor.current_context()` during `recorder_init`:

| Calling Context / Configuration | `.Auto` Selected Backend | Rationale |
| :--- | :--- | :--- |
| Plain OS thread (`scheduler == nil`) | `.Dedicated_Thread` | Offloads Arrow serialization and disk I/O to a dedicated background thread. |
| Inside a Sleipnir actor turn (`num_workers >= 2`) | `.Sleipnir_Actor` (shared `ctx.sched`) | Avoids spawning extra OS threads and shares the multi-worker actor scheduler. |
| Inside a 1-worker Sleipnir actor turn + `.Block` policy | `.Dedicated_Thread` | Prevents blocking disk I/O from stalling the lone actor worker thread. |
| Inside a 1-worker Sleipnir actor turn + `.Drop` policy | `.Sleipnir_Actor` (shared `ctx.sched`) | Non-blocking producer safely multiplexes with the flusher actor on 1 worker. |
| Explicit `backend = .Dedicated_Thread` | `.Dedicated_Thread` | Always spawns a dedicated background OS thread (even if `scheduler != nil`). |
| Explicit `backend = .Sleipnir_Actor` (`scheduler == nil` on OS thread) | `.Sleipnir_Actor` (`_owned_sched = true`) | Spawns an internal 1-worker `actor.Scheduler` owned by the `Recorder`. |

### Performance & Configuration Tuning
* **Prefer `string_fallback_copy = false` (default) on hot paths:** Pass string literals or compile-time constants to achieve `0 B` producer allocations. When `string_fallback_copy = true` is required for dynamically freed strings, point metrics and events (`event_emit`, `counter_add`, `gauge_set`, `histogram_record`) copy into a reusable per-page `128 KB` bump slab (`O(1)` reset per batch, falling back to `Recorder.allocator` if a batch overflows the slab), whereas `scoped_span` / `span_begin` must heap-clone strings across the open lifetime of the span.
* **Increase `ring_page_count` for bursty `.Block` pipelines:** The default `4` pages (`~1.4 MB`) balances memory against I/O overlap; setting `8`–`16` pages prevents producers from stalling behind Arrow disk serialization during heavy bursts (`2` pages roughly halves `.Block` saturation throughput).

---

## Canonical 11-Column Arrow Telemetry Schema

Heimdall persists `.arrow` IPC files with 11 canonical OpenTelemetry-compatible columns defined by `@(telemetry_schema) Record :: struct` in `heimdall/heimdall.odin`:

| Column Index | Field Name | Odin Type | Arrow Type | Description |
| ---: | :--- | :--- | :--- | :--- |
| `0` | `timestamp_ns` | `i64` | `Int64` | Calibrated Unix epoch timestamp in nanoseconds |
| `1` | `duration_ns` | `i64` | `Int64` | Monotonic span duration in nanoseconds (`0` for point metrics/events) |
| `2` | `span_id` | `u64` | `UInt64` | Unique non-zero ID for `.Span` records (`0` for point metrics/events) |
| `3` | `parent_id` | `u64` | `UInt64` | Enclosing active `span_id` on the same `(Recorder, Actor_Turn)` (`0` if root) |
| `4` | `thread_id` | `u32` | `UInt32` | Cached OS producer thread ID |
| `5` | `kind` | `Kind` (`u8`) | `UInt8` | `Span (0)`, `Counter (1)`, `Gauge (2)`, `Histogram (3)`, `Event (4)` |
| `6` | `status` | `Status` (`u8`) | `UInt8` | `Ok (0)`, `Error (1)` |
| `7` | `scope` | `string` | `String` | Library or subsystem name (e.g., `"net.http"`, `"db.driver"`) |
| `8` | `name` | `string` | `String` | Operation or metric name (e.g., `"handle_request"`, `"requests_total"`) |
| `9` | `value_f64` | `f64` | `Float64` | Metric value/delta/sample, or convenience span duration in milliseconds (`f64(duration_ns) / 1e6`) |
| `10` | `attr_i64` | `i64` | `Int64` | Secondary integer attribute (payload bytes, query ID, HTTP status code, entity ID) |

---

## Repository Structure & Documentation

* `heimdall/` — Core telemetry recorder (`heimdall.odin`), generated Arrow schema serializer/reader (`schema_gen.odin`), and AST codegen/linter library (`heimdall/codegen/`).
* `tools/heimdall_gen/` — CLI AST code generator for `@(telemetry_schema)` structs and the 4-layer ATDD test suite.
* `tests/` — 4-layer ATDD specification suite (`spec_*.odin`, `dsl_heimdall.odin`, `dsl_atdd_gen.odin`, `driver_heimdall.odin`).
* `benchmarks/` — `hugi`-backed throughput, latency, multi-threaded scaling, and Arrow I/O roundtrip benchmark suite (`bench_heimdall.odin`, [benchmarks/README.md](benchmarks/README.md)).
* [`usage-rules.md`](./usage-rules.md) — Strict usage rules, idioms, and anti-patterns for AI coding agents and contributors.
* [`docs/api_reference.md`](./docs/api_reference.md) — Complete API reference for `package heimdall` and `package heimdall_codegen`.
* [`docs/architecture_and_atdd.md`](./docs/architecture_and_atdd.md) — Deep dive into the `#soa` ring architecture, concurrency model, and 4-layer ATDD AST verifier.

---

## Building, Testing & Benchmarking

### Regenerate Schema & ATDD DSL Code
```bash
odin run tools/heimdall_gen -- tests
```

### Run the 4-Layer ATDD Specification Suite
```bash
odin test tests -custom-attribute:telemetry_schema
```

### Run Static Style & AST Vet Checks
```bash
odin check heimdall/codegen -no-entry-point -vet -strict-style -vet-tabs -disallow-do
odin check tests -no-entry-point -vet -strict-style -vet-tabs -custom-attribute:telemetry_schema -vet-packages:heimdall,heimdall_codegen,heimdall_tests
odin check benchmarks -collection:heimdall=./heimdall -collection:skadi=./deps/skadi/skadi -collection:actor=./deps/sleipnir/actor -collection:hugi=../hugi/hugi -vet -strict-style -vet-tabs -disallow-do -custom-attribute:telemetry_schema -vet-packages:heimdall,heimdall_benchmarks
```

### Run the Benchmark Suite
```bash
hugi run benchmarks --runs 5
```
See [benchmarks/README.md](benchmarks/README.md) for group filters, `hugi compare --last`, and updating the benchmark summary table (`hugi export --update-readme benchmarks/README.md`).
