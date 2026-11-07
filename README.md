# Heimdall (`package heimdall`)

**Heimdall** is a general-purpose, zero-cost-when-disabled OpenTelemetry-style telemetry library for [Odin](https://odin-lang.org/), backed by an `#soa` producer-consumer ring and a single-threaded [**OdinArrow**](deps/OdinArrow) background serializer emitting rolling Apache Arrow IPC (`.arrow` / Feather v2) segment files.

---

## Architecture Overview

1. **Decoupled Ingestion (`#soa` Ring) & Configurable Background Backend (`Dedicated_Thread` or `Sleipnir_Actor`):**
   Producers write directly into pre-allocated `#soa[BATCH_CAP]Record` pages (`BATCH_CAP :: 4096`) and copy entire `#soa` columns into Arrow buffers via `mem.copy_non_overlapping`. The background serializer can run either on a dedicated OS thread (`backend = .Dedicated_Thread`, default) or as a [**Sleipnir**](deps/sleipnir) actor (`backend = .Sleipnir_Actor`, optionally sharing your application's `actor.Scheduler` via `Config.scheduler`).
2. **AST-Generated 11-Column OpenTelemetry-Style Schema (`heimdall/schema_gen.odin`):**
   Represents **Spans, Counters, Gauges, Histograms, and Events** from any library in a uniform columnar layout (`timestamp_ns`, `duration_ns`, `span_id`, `parent_id`, `thread_id`, `kind`, `status`, `scope`, `name`, `value_f64`, `attr_i64`). `tools/heimdall_gen` parses `Record` via `core:odin/parser` to generate `make_record_arrow_schema`, `stage_record_into_soa`, `build_arrow_batch`, and zero-copy `Record_Batch_Reader`. Parent-child call trees are automatically wired across libraries via per-`(Recorder, Actor_Turn)` thread-local span slots (`tls_active_span_id`).
3. **Zero-Cost Ergonomics ("Zero Is Initialization" + `@(deferred_out)`):**
   Library procedures can accept an optional `^heimdall.Recorder` (or fall back to `heimdall.default_recorder()`). When `recorder == nil` or disabled, instrumentation calls short-circuit in a single branch. Using `@(deferred_out = span_end)` on `scoped_span` instruments any procedure in one line and records nanosecond duration on any scope exit.
4. **4-Layer ATDD Architecture & Odin AST Analyzers (`heimdall/codegen`, `tools/heimdall_gen`, `tests/`):**
   - **Layer 1 (`tests/spec_*.odin`):** Pure domain specifications using AST-generated `given.*` / `upon.*` / `then.*` ambient namespaces and `s->step(...)` selector syntax, plus `@(deferred_out)` RAII `scenario(t)` teardown.
   - **Layer 2 (`tests/dsl_heimdall.odin` + `tests/dsl_atdd_gen.odin`):** Domain-specific language scanned by `heimdall/codegen` (`core:odin/parser` + `core:odin/ast`) to generate ambient wrappers and selector vtables.
   - **Layer 3 (`tests/driver_heimdall.odin`):** Protocol driver isolating all `OdinArrow` IPC reads, `Recorder` lifecycle management, and temporary `.arrow` file cleanup.
   - **AST Purity & Telemetry Call-Site Analysis (`tests/spec_ast_architecture.odin`):** Parses all `tests/*.odin` and `heimdall/*.odin` files at test time to enforce zero Layer 3/4 leaks in Layer 1 specs, zero `do` statements, 100% DSL step coverage, byte-exact `dsl_atdd_gen.odin` and `schema_gen.odin` codegen sync, and static telemetry call-site hazard/interning inspection.

---

## Quick Start

```odin
package example

import "heimdall"

handle_request :: proc(rec: ^heimdall.Recorder, payload_size: int) {
	heimdall.scoped_span(rec, "http_lib", "handle_request", i64(payload_size))
	heimdall.counter_add(rec, "http_lib", "requests_total", 1.0)
	execute_query(rec, 42)
}

execute_query :: proc(rec: ^heimdall.Recorder, query_id: i64) {
	// Automatically captures `handle_request`'s span_id as its `parent_id`
	heimdall.scoped_span(rec, "db_driver", "execute_query", query_id)
	heimdall.gauge_set(rec, "db_driver", "pool_connections_active", 8.0)
}
```

---

## Running Checks, AST Codegen & ATDD Specs

```bash
# Regenerate `tests/dsl_atdd_gen.odin` and `heimdall/schema_gen.odin` via `core:odin/parser`
odin run tools/heimdall_gen -vet -strict-style -vet-tabs -disallow-do -- tests heimdall

# Type-check library, codegen & benchmark packages
odin check heimdall -no-entry-point -vet -strict-style -vet-tabs -vet-packages:heimdall
odin check heimdall/codegen -no-entry-point -vet -strict-style -vet-tabs -disallow-do
odin check benchmarks -vet -strict-style -vet-tabs -vet-packages:heimdall_benchmarks,heimdall

# Run 4-layer ATDD executable specifications (with Tracking_Allocator & AST purity verification)
odin test tests -vet -strict-style -vet-tabs -vet-packages:heimdall,heimdall_codegen,heimdall_tests

# Run throughput & latency benchmark suite
odin run benchmarks -o:speed -no-bounds-check
```

---

## Instant Inspection Queries (DuckDB CLI)

Because every segment file emitted by `worker_proc` is a self-describing `.arrow` IPC file using a uniform schema, you can query across all rolled segment files simultaneously in DuckDB using glob syntax (`telemetry_*.arrow`):

### Latency Percentiles (p50, p95, p99) per Library Procedure

```sql
SELECT
    scope,
    name,
    count(*) AS calls,
    round(quantile_cont(value_f64, 0.50), 3) AS p50_ms,
    round(quantile_cont(value_f64, 0.95), 3) AS p95_ms,
    round(quantile_cont(value_f64, 0.99), 3) AS p99_ms
FROM 'telemetry_*.arrow'
WHERE kind = 0 -- Kind.Span
GROUP BY scope, name
ORDER BY p99_ms DESC;
```

### Reconstruct Parent-Child Call Trees Across Libraries

```sql
SELECT
    p.scope || '.' || p.name AS parent_call,
    c.scope || '.' || c.name AS child_call,
    count(*) AS invocations,
    round(avg(c.value_f64), 3) AS avg_child_ms
FROM 'telemetry_*.arrow' c
JOIN 'telemetry_*.arrow' p ON c.parent_id = p.span_id
WHERE c.kind = 0 AND p.kind = 0
GROUP BY 1, 2;
```
