# Heimdall Architecture & 4-Layer ATDD Guide

This document explains Heimdall's internal memory layout, lock-free/low-lock concurrency protocol, `.Auto` backend selection, and AST-enforced 4-layer Acceptance Test-Driven Development (ATDD) architecture.

---

## 1. Memory Layout & Cache-Line Isolation

### 1.1 `Recorder` Cache-Line Zones
To prevent false sharing between high-frequency producer threads and the background `OdinArrow` serializer, `Recorder` (`#align(64)`) is divided into four cache-line-isolated zones separated by explicit `[64]u8` padding (`_pad0`, `_pad1`, `_pad2`):

1. **Zone 1 (Read-Mostly Configuration & Allocators):** `enabled`, `_instance_id`, `epoch_offset_ns`, `allocator`, `cfg`, and the `_pages` slice header. Shared across all CPU cores in the Shared (S) MESI state during normal ingestion.
2. **Zone 2 (Producer-Consumer Ring State):** `_mu`, `_active_page_idx`, `_free_stack`, `_ready_ring`, condition variables (`_cv_work_ready`, `_cv_page_free`, `_cv_flush_done`, `_cv_actor_idle`), and `_alloc_mu`.
3. **Zone 3 (Producer Atomic Counters):** `_next_span_id` (updated only once every `SPAN_ID_BATCH_SIZE :: 64` spans per thread) and `_dropped_records`.
4. **Zone 4 (Background Worker / Actor State):** `_flushed_segments`, `_failed_segments`, `_eng` (`Flusher_Engine`), `_worker`, `_sched`, and `_flusher_actor`.

### 1.2 `#soa` Staging Pages & Two-Phase Commit
Each `Staging_Page` (`#align(64)`) holds `#soa[4096]Record` (`BATCH_CAP :: 4096`, `~344 KB` per page):
```odin
Staging_Page :: struct #align(64) {
	rows:      #soa[]Record,
	count:     int,
	_pad:      [64]u8,
	committed: int,
}
```
1. **Slot Reservation (Under `_mu`):** A producer thread increments `page.count` (`idx := page.count; page.count = idx + 1`). If `idx + 1 == BATCH_CAP`, the producer pushes the page index onto `_ready_ring` and pops the next free page from `_free_stack` (`rotate_active_page_locked`).
2. **Columnar Store (Unlocked):** After releasing `_mu`, the producer writes the 11 fields directly into the contiguous `#soa` arrays (`stage_record_into_soa(page, idx, ...)`) and executes an `.Release` atomic increment on `page.committed`.
3. **Worker Drain & Zero-Copy Arrow Construction:** When the background worker or flusher actor pops a ready page, it spins briefly with `intrinsics.cpu_relax()` until `atomic_load_explicit(&page.committed, .Acquire) == page.count`. Because each field in `page.rows` is already a contiguous column array, `build_primitive_column_from_soa` copies each primitive column in a single `mem.copy_non_overlapping` into an `oa.Buffer_Pool`-backed Arrow buffer.

---

## 2. Thread-Local Span Hierarchy & Cooperative Actor Isolation

Instrumenting nested library calls without passing a `Span_Context` parameter through every function signature requires thread-local storage (`@(thread_local)`). However, a naive single `tls_active_span_id` variable breaks in two real-world scenarios:

1. **Multiple Recorders on One Thread:** An application runs both an operational telemetry `Recorder` and a security audit `Recorder` on the same thread.
2. **Cooperative Sleipnir `actor.call` on One Worker Thread:** Actor A opens a `scoped_span` and invokes `actor.call(sched, actor_b, msg)`. On a 1-worker scheduler, `actor.call` cooperatively steps Actor B on the **same OS thread** while Actor A's stack frame is suspended.

Heimdall solves both by indexing `tls_recorder_spans: [MAX_TLS_RECORDERS]TLS_Recorder_Span` by the 3-tuple:

```text
(recorder, recorder_id, actor_env)
```

where `actor_env` is read from `actor.current_context().current_env`.
* Actor B's message turn has a distinct `current_env` pointer from Actor A's suspended turn, so Actor B starts a clean root span (`parent_id == 0`) without inheriting Actor A's open span.
* Including `_instance_id` ensures that if a `Recorder` is destroyed and a new `Recorder` is re-initialized at the exact same memory address, stale `tls_span_id_cache` entries and stale `Span_Guard` values from the prior lifecycle generation are never confused with the new instance.

---

## 3. Bounded-Generation Live Flushing (`recorder_flush`)

Under continuous multi-threaded producer load, a naive `for r._ready_count > 0` flush loop can starve indefinitely as producers keep filling new pages faster than the disk flushes them.

Heimdall prevents flush starvation using a monotonic generation counter:
1. `recorder_flush` rotates any non-empty active page into `_ready_ring`, increments `_flush_target_gen`, and waits until `_flush_completed_gen >= target_gen`.
2. `flusher_engine_drain_ready_pages` snapshots `max_flush_pages := r._ready_count` at the start of the flush pass (`bounded_by_flush`), drains at most `max_flush_pages` pages, writes the segment file via `oa.ipc_write_file`, advances `_flush_completed_gen`, and broadcasts `_cv_flush_done`.
3. If additional pages arrived while the disk write was in progress (`needs_actor_resignal`), the flusher re-signals itself asynchronously so subsequent pages continue draining without blocking the caller of `recorder_flush`.

---

## 4. Four-Layer ATDD Architecture & AST Verification

Heimdall's test suite (`tests/`) follows Dave Farley's **4-Layer Acceptance Test-Driven Development (ATDD)** architecture and enforces it via AST analysis (`heimdall/codegen/atdd_codegen.odin`):

```
┌───────────────────────────────────────────────────────────────────────────┐
│ Layer 1: Executable Specifications (`tests/spec_*.odin`)                  │
│   - Pure domain language (`given.*`, `upon.*`, `then.*`, `s->step(...)`)  │
│   - Only imports `"core:testing"`; zero SUT/driver/expect calls           │
├───────────────────────────────────────────────────────────────────────────┤
│ Layer 2: Domain-Specific Language (`tests/dsl_heimdall.odin` + codegen)   │
│   - Translates domain steps into Layer 3 driver calls + `testing.expect`  │
│   - `tools/heimdall_gen` generates `tests/dsl_atdd_gen.odin`              │
├───────────────────────────────────────────────────────────────────────────┤
│ Layer 3: Protocol Driver (`tests/driver_heimdall.odin`)                   │
│   - Owns `Suite_Ctx`, `heimdall.Recorder`, `OdinArrow` IPC reader,        │
│     `sleipnir/actor` schedulers, and `.tmp/*.arrow` file cleanup          │
├───────────────────────────────────────────────────────────────────────────┤
│ Layer 4: System Under Test (`heimdall/`)                                  │
│   - Core `#soa` ring recorder, `schema_gen.odin`, and `heimdall/codegen`  │
└───────────────────────────────────────────────────────────────────────────┘
```

### Static AST Contract Checks (`AST_Contract_Report`)
When `odin test tests -custom-attribute:telemetry_schema` runs, `spec_ast_architecture.odin` parses every `.odin` file in `tests/` and `heimdall/` using `core:odin/parser` and verifies:
* **`codegen_matches == true`:** Both `tests/dsl_atdd_gen.odin` and `heimdall/schema_gen.odin` match the exact in-memory output of `heimdall_codegen` (preventing stale generated files).
* **`forbidden_imports == 0`:** Layer 1 `spec_*.odin` files only import `"core:testing"`.
* **`non_test_procs_in_spec == 0`:** Every declaration in `spec_*.odin` is an `@(test)` scenario procedure.
* **`direct_sut_calls == 0` & `raw_testing_calls == 0`:** Layer 1 scenarios never bypass the DSL to call `heimdall.*`, `driver_*`, or `testing.expect*`.
* **`zero_arg_mega_steps == 0`:** Every `upon_*` and `then_*` DSL procedure and call site specifies explicit domain parameters (preventing opaque "macro theater" steps).
* **`driver_field_leaks == 0`:** Layer 1 scenarios never read or mutate `Suite_Ctx` fields (`s.rec`, etc.) directly.
* **`unused_dsl_procedures == 0` & `unknown_dsl_calls == 0`:** Every Layer 2 DSL step is exercised by at least one Layer 1 scenario, and every step called in Layer 1 exists in Layer 2.
* **`do_statements == 0`:** Zero `ast.Block_Stmt.uses_do` nodes across the entire repository.
