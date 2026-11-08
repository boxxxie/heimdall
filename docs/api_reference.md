# Heimdall API Reference

This document provides the complete API reference for `package heimdall` and `package heimdall_codegen`.

---

## 1. Constants & Enumerations (`package heimdall`)

### Constants
```odin
BATCH_CAP            :: 4096 // Rows per #soa Staging_Page and Arrow Record_Batch
DEFAULT_RING_PAGES   :: 4    // Default number of #soa pages in the producer/consumer ring
DEFAULT_SEGMENT_SIZE :: 16   // Default batches per .arrow segment file (65,536 rows)
MAX_TLS_RECORDERS    :: 16   // Max concurrent active (Recorder, Actor_Turn) span slots per OS thread
SPAN_ID_BATCH_SIZE   :: 64   // Thread-local span ID reservation batch size
RECORD_COLUMN_COUNT  :: 11   // Generated constant: column count of @(telemetry_schema) Record
```

### `Kind`
```odin
Kind :: enum u8 {
	Span      = 0,
	Counter   = 1,
	Gauge     = 2,
	Histogram = 3,
	Event     = 4,
}
```

### `Status`
```odin
Status :: enum u8 {
	Ok    = 0,
	Error = 1,
}
```

### `Backpressure_Policy`
```odin
Backpressure_Policy :: enum u8 {
	Drop  = 0, // Never block caller when all ring pages are full; increments dropped_records
	Block = 1, // Wait on _cv_page_free (or synchronous actor drain) until a page is freed
}
```

### `Backend_Mode`
```odin
Backend_Mode :: enum u8 {
	Auto             = 0, // Automatically selects .Dedicated_Thread or .Sleipnir_Actor
	Dedicated_Thread = 1, // Dedicated background OS thread owns OdinArrow + disk I/O
	Sleipnir_Actor   = 2, // Runs OdinArrow serializer as a Sleipnir Actor on an actor.Scheduler
}
```

---

## 2. Core Data Structures

### `Config`
```odin
Config :: struct {
	file_prefix:          string,              // Default: "telemetry" (cloned on init)
	batches_per_file:     int,                 // Default: 16 (65,536 rows per .arrow file)
	ring_page_count:      int,                 // Default: 4 (minimum 2 pages enforced)
	string_fallback_copy: bool,                // Clone scope & name strings on ingestion
	policy:               Backpressure_Policy, // .Drop or .Block
	backend:              Backend_Mode,        // .Auto (default), .Dedicated_Thread, or .Sleipnir_Actor
	scheduler:            ^actor.Scheduler,    // Optional external Sleipnir Scheduler
}
```

### `Record`
Annotated with `@(telemetry_schema)` to drive `heimdall/schema_gen.odin`:
```odin
@(telemetry_schema)
Record :: struct {
	timestamp_ns: i64,
	duration_ns:  i64,
	span_id:      u64,
	parent_id:    u64,
	thread_id:    u32,
	kind:         Kind,
	status:       Status,
	scope:        string,
	name:         string,
	value_f64:    f64,
	attr_i64:     i64,
}
```

---

## 3. Lifecycle & Configuration Procedures

### `resolve_ideal_backend`
```odin
resolve_ideal_backend :: proc(cfg: Config) -> (mode: Backend_Mode, sched: ^actor.Scheduler)
```
Inspects `cfg` and the calling thread's live Sleipnir `actor.Context` (`actor.current_context()`) to resolve `.Auto` into either `.Dedicated_Thread` or `.Sleipnir_Actor` and identify the target `^actor.Scheduler`.

### `recorder_init`
```odin
recorder_init :: proc(
	r: ^Recorder,
	cfg := Config{},
	allocator := context.allocator,
) -> (err: mem.Allocator_Error)
```
Initializes `r`, clones `cfg.file_prefix`, allocates `ring_page_count` `#soa[BATCH_CAP]Record` pages, calibrates `epoch_offset_ns`, initializes the `Flusher_Engine`, and starts the resolved background execution backend. On any allocation failure, rolls back all partial allocations and leaves `r^` zeroed.

### `recorder_destroy`
```odin
recorder_destroy :: proc(r: ^Recorder)
```
Disables recording, pushes any non-empty active `#soa` page to the ready ring, waits for any in-flight Sleipnir actor calls (`_inflight_actor_calls`), joins the background worker thread or sends `.Shutdown` to the flusher actor, flushes remaining batches to disk, frees all `#soa` pages and cloned strings, and preserves final diagnostic counters (`flushed_segments`, `dropped_records`, `failed_segments`).

### `recorder_flush`
```odin
recorder_flush :: proc(r: ^Recorder)
```
Rotates the current partial `#soa` page into the ready ring and blocks until all pages and buffered `OdinArrow` batches up to the current flush generation (`_flush_target_gen`) have been written to disk as an `.arrow` segment file. Safe to call while the recorder remains active or while the worker is paused.

### `set_default_recorder` / `default_recorder`
```odin
set_default_recorder :: proc(r: ^Recorder)
default_recorder :: #force_inline proc() -> ^Recorder
```
Atomically sets or loads the process-wide default `^Recorder`.

### Runtime Controls & Diagnostics
```odin
recorder_diagnostics       :: #force_inline proc(r: ^Recorder) -> (flushed_segments, dropped_records: u64)
recorder_failed_segments   :: #force_inline proc(r: ^Recorder) -> u64
recorder_is_enabled        :: #force_inline proc(r: ^Recorder) -> bool
recorder_set_enabled       :: proc(r: ^Recorder, enabled: bool)
recorder_is_saturated      :: proc(r: ^Recorder) -> bool
recorder_waiting_producers :: proc(r: ^Recorder) -> int
recorder_set_worker_paused :: proc(r: ^Recorder, paused: bool)
```

---

## 4. Instrumentation API

All instrumentation procedures are `#force_inline` and perform a zero-cost short-circuit when `r == nil` or `!recorder_is_enabled(r)`.

### Spans
```odin
@(deferred_out = span_end)
scoped_span :: #force_inline proc(
	r: ^Recorder,
	scope: string,
	name: string,
	attr_i64: i64 = 0,
) -> Span_Guard

span_begin :: #force_inline proc(
	r: ^Recorder,
	scope: string,
	name: string,
	attr_i64: i64 = 0,
) -> Span_Guard

span_end :: #force_inline proc(g: Span_Guard)

span_set_status :: #force_inline proc(
	r:           ^Recorder,
	status:      Status,
	attr_i64:    i64  = 0,
	update_attr: bool = false,
)

span_set_error :: #force_inline proc(
	r:           ^Recorder,
	attr_i64:    i64  = 0,
	update_attr: bool = true,
)
```
* When a span is active on `(r, current_actor_env())`, any nested `scoped_span`, `span_begin`, `counter_add`, `gauge_set`, `histogram_record`, or `event_emit` on the same thread and recorder automatically sets its `parent_id` to the active `span_id`.
* When a `.Span` record is written, `duration_ns` holds the monotonic duration in nanoseconds and `value_f64` holds the duration in milliseconds ($\text{duration\_ns} / 10^6$).

### Point Metrics & Events
```odin
counter_add      :: #force_inline proc(r: ^Recorder, scope, name: string, delta: f64 = 1.0, attr_i64: i64 = 0)
gauge_set        :: #force_inline proc(r: ^Recorder, scope, name: string, val: f64, attr_i64: i64 = 0)
histogram_record :: #force_inline proc(r: ^Recorder, scope, name: string, sample: f64, attr_i64: i64 = 0)
event_emit       :: #force_inline proc(r: ^Recorder, scope, name: string, attr_i64: i64 = 0, status: Status = .Ok)
```

---

## 5. Columnar Arrow Reader API (`heimdall/schema_gen.odin`)

Generated from `@(telemetry_schema) Record :: struct` to allow zero-overhead columnar inspection of `.arrow` files read via `oa.ipc_read_file`:

```odin
record_batch_reader_resolve :: proc(b: ^oa.Record_Batch) -> (reader: Record_Batch_Reader, ok: bool)
record_batch_reader_decode_row :: #force_inline proc(reader: ^Record_Batch_Reader, row_idx: int) -> Record
decode_string_from_buffers :: #force_inline proc(
	offsets:    [^]i32,
	bytes:      [^]u8,
	bytes_size: int,
	row_idx:    int,
) -> string
```
`record_batch_reader_resolve` validates all 11 columns, checks buffer lengths and non-zero `Array.offset` slices, and caches typed multi-pointers (`reader.timestamp_ns_data`, `reader.attr_i64_data`, etc.) for $O(1)$ row decoding.

---

## 6. AST Codegen & Static Analysis API (`package heimdall_codegen`)

Located in `heimdall/codegen/`:

* `generate_telemetry_schema_directory(sut_dir, out_filename, diagnostics, allocator) -> (out_path: string, ok: bool)`
* `generate_telemetry_schema_from_source(src, filename, allocator) -> (code: string, column_count: int, ok: bool)`
* `analyze_telemetry_call_sites(sources, allocator) -> (report: Telemetry_Call_Site_Report, ok: bool)`
  * Inspects AST call sites for `scoped_span`, `span_begin`, `counter_add`, `gauge_set`, `histogram_record`, and `event_emit`.
  * Counts unique static `scope` and `name` literals, flags uncloned dynamic string arguments (`dynamic_string_hazards`), detects misplaced `scoped_span` statements (`misplaced_scoped_spans`), and detects telemetry inside Sleipnir actor handlers (`actor_handler_call_sites`).
* `generate_atdd_directory(dir_path, out_filename, diagnostics, allocator) -> (out_path: string, ok: bool)`
* `analyze_atdd_directories(tests_dir, sut_dir, allocator) -> (report: AST_Contract_Report, codegen_matches: bool, ok: bool)`
