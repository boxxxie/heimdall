package heimdall

import "base:intrinsics"
import "core:fmt"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import oa "../deps/OdinArrow/src"

BATCH_CAP            :: 4096
DEFAULT_RING_PAGES   :: 4  // 2+ pages allow lock-free I/O overlap
DEFAULT_SEGMENT_SIZE :: 16 // 16 batches * 4096 rows = 65,536 rows per .arrow file

Kind :: enum u8 {
	Span      = 0,
	Counter   = 1,
	Gauge     = 2,
	Histogram = 3,
	Event     = 4,
}

Status :: enum u8 {
	Ok    = 0,
	Error = 1,
}

Backpressure_Policy :: enum u8 {
	Drop  = 0, // Never block caller (ideal for real-time/game/audio loops)
	Block = 1, // Wait for background worker if all ring pages are full
}

Config :: struct {
	file_prefix:          string, // e.g., "logs/app_telemetry"
	batches_per_file:     int,    // Default: 16 (65,536 rows per .arrow file)
	ring_page_count:      int,    // Default: 4 pages in the producer/consumer ring
	string_Fallback_copy: bool,   // Set true if passing dynamically freed strings
	policy:               Backpressure_Policy,
}

// Canonical telemetry row stored in #soa layout inside each staging page.
Record :: struct {
	timestamp_ns: i64,
	duration_ns:  i64,
	span_id:      u64,
	parent_id:    u64,
	thread_id:    u32,
	kind:         Kind,
	status:       Status,
	scope:        string, // Library or subsystem name (e.g., "net.http", "ecs")
	name:         string, // Operation or metric name (e.g., "parse_header")
	value_f64:    f64,    // Metric value, delta, or sample
	attr_i64:     i64,    // Secondary int tag (e.g., bytes, entity_id, status_code)
}

@(private)
Staging_Page :: struct {
	rows:  #soa[]Record,
	count: int,
}

Recorder :: struct {
	allocator:       mem.Allocator,
	cfg:             Config,
	enabled:         bool,

	// Producer-Consumer Ring of #soa pages
	mu:              sync.Mutex,
	cv_work_ready:   sync.Cond,
	cv_page_free:    sync.Cond,
	cv_flush_done:   sync.Cond,
	pages:           []Staging_Page,
	free_stack:      [dynamic]int,
	ready_queue:     [dynamic]int,
	active_page_idx: int,
	worker_paused:   bool,
	flush_requested: bool,
	shutdown:        bool,

	// Diagnostics (updated atomically)
	next_span_id:    u64,
	dropped_records: u64,
	flushed_batches: u64,

	// Background worker state (single-threaded ownership of OdinArrow + Buffer_Pool)
	worker:          ^thread.Thread,
}

// Thread-local active span state so nested calls across libraries automatically
// wire up parent_id -> span_id call trees and per-span status/attr overrides.
@(thread_local)
tls_active_span_id: u64

@(private)
@(thread_local)
tls_active_span_status: Status

@(private)
@(thread_local)
tls_active_span_attr: i64

@(private)
@(thread_local)
tls_thread_id: u32

@(private)
current_tid :: #force_inline proc() -> u32 {
	tid := tls_thread_id
	if tid == 0 {
		tid = u32(sync.current_thread_id())
		tls_thread_id = tid
	}
	return tid
}

// Optional global default recorder for libraries that don't pass ^Recorder explicitly.
@(private)
g_default_recorder: ^Recorder

set_default_recorder :: proc(r: ^Recorder) {
	intrinsics.atomic_store(&g_default_recorder, r)
}

default_recorder :: #force_inline proc() -> ^Recorder {
	return intrinsics.atomic_load(&g_default_recorder)
}

// -----------------------------------------------------------------------------
// Lifecycle Management
// -----------------------------------------------------------------------------

recorder_init :: proc(
	r: ^Recorder,
	cfg := Config{},
	allocator := context.allocator,
) -> (err: mem.Allocator_Error) {
	r^ = {}
	r.allocator = allocator
	r.cfg       = cfg

	if r.cfg.file_prefix == "" {
		r.cfg.file_prefix = "telemetry"
	}
	if r.cfg.batches_per_file <= 0 {
		r.cfg.batches_per_file = DEFAULT_SEGMENT_SIZE
	}
	if r.cfg.ring_page_count < 2 {
		r.cfg.ring_page_count = DEFAULT_RING_PAGES
	}

	r.pages = make([]Staging_Page, r.cfg.ring_page_count, allocator) or_return
	defer if err != nil {
		for i in 0 ..< len(r.pages) {
			if r.pages[i].rows != nil {
				delete_soa(r.pages[i].rows, allocator)
			}
		}
		delete(r.pages, allocator)
		delete(r.free_stack)
		delete(r.ready_queue)
		r^ = {}
	}

	for i in 0 ..< len(r.pages) {
		r.pages[i].rows = make_soa(#soa[]Record, BATCH_CAP, allocator) or_return
	}

	r.free_stack  = make([dynamic]int, 0, r.cfg.ring_page_count, allocator) or_return
	r.ready_queue = make([dynamic]int, 0, r.cfg.ring_page_count, allocator) or_return

	// Page 0 starts as the active front buffer; remaining pages go onto free_stack
	r.active_page_idx = 0
	for i := r.cfg.ring_page_count - 1; i >= 1; i -= 1 {
		append(&r.free_stack, i)
	}

	r.next_span_id = 1
	r.enabled      = true
	r.worker       = thread.create_and_start_with_poly_data(r, worker_proc)
	return nil
}

recorder_destroy :: proc(r: ^Recorder) {
	if r == nil || r.pages == nil {
		return
	}

	sync.mutex_lock(&r.mu)
	r.enabled       = false
	r.worker_paused = false
	if r.active_page_idx >= 0 && r.pages[r.active_page_idx].count > 0 {
		append(&r.ready_queue, r.active_page_idx)
		r.active_page_idx = -1
	}
	r.shutdown = true
	sync.cond_signal(&r.cv_work_ready)
	sync.cond_broadcast(&r.cv_page_free)
	sync.cond_broadcast(&r.cv_flush_done)
	sync.mutex_unlock(&r.mu)

	if r.worker != nil {
		thread.join(r.worker)
		thread.destroy(r.worker)
	}

	dropped := intrinsics.atomic_load(&r.dropped_records)
	flushed := intrinsics.atomic_load(&r.flushed_batches)

	for i in 0 ..< len(r.pages) {
		if r.cfg.string_Fallback_copy {
			free_page_strings(&r.pages[i], r.allocator)
		}
		delete_soa(r.pages[i].rows, r.allocator)
	}
	delete(r.pages, r.allocator)
	delete(r.free_stack)
	delete(r.ready_queue)
	r^ = {}
	r.dropped_records = dropped
	r.flushed_batches = flushed
}

// Forces the current partial page and any buffered Arrow batches to disk immediately.
recorder_flush :: proc(r: ^Recorder) {
	if r == nil || !r.enabled {
		return
	}
	sync.mutex_lock(&r.mu)
	defer sync.mutex_unlock(&r.mu)

	if r.active_page_idx >= 0 && r.pages[r.active_page_idx].count > 0 {
		_ = rotate_active_page_locked(r)
	}
	r.flush_requested = true
	sync.cond_signal(&r.cv_work_ready)
	for r.flush_requested && !r.shutdown {
		sync.cond_wait(&r.cv_flush_done, &r.mu)
	}
}

// Pauses or resumes background worker consumption of ready #soa pages.
recorder_set_worker_paused :: proc(r: ^Recorder, paused: bool) {
	if r == nil {
		return
	}
	sync.mutex_lock(&r.mu)
	r.worker_paused = paused
	if !paused {
		sync.cond_signal(&r.cv_work_ready)
	}
	sync.mutex_unlock(&r.mu)
}

// -----------------------------------------------------------------------------
// Library Instrumentation API (Zero-Cost when `r == nil` or disabled)
// -----------------------------------------------------------------------------

Span_Guard :: struct {
	recorder:       ^Recorder,
	start_tick:     time.Tick,
	timestamp_ns:   i64,
	span_id:        u64,
	prev_parent:    u64,
	prev_status:    Status,
	prev_attr:      i64,
	scope:          string,
	name:           string,
	attr_i64:       i64,
	status:         Status,
	strings_cloned: bool,
}

// Single-line RAII span: automatically calls `span_end` when enclosing block exits!
// Usage: `heimdall.scoped_span(r, "my_lib", "decode_packet", i64(len(buf)))`
@(deferred_out = span_end)
scoped_span :: #force_inline proc(
	r: ^Recorder,
	scope: string,
	name: string,
	attr_i64: i64 = 0,
) -> Span_Guard {
	return span_begin(r, scope, name, attr_i64)
}

span_begin :: #force_inline proc(
	r: ^Recorder,
	scope: string,
	name: string,
	attr_i64: i64 = 0,
) -> Span_Guard {
	if r == nil || !r.enabled {
		return Span_Guard{}
	}
	id          := intrinsics.atomic_add(&r.next_span_id, 1)
	parent      := tls_active_span_id
	prev_status := tls_active_span_status
	prev_attr   := tls_active_span_attr

	tls_active_span_id     = id
	tls_active_span_status = .Ok
	tls_active_span_attr   = attr_i64

	stored_scope := scope
	stored_name  := name
	cloned       := false
	if r.cfg.string_Fallback_copy {
		if len(stored_scope) > 0 {
			stored_scope = strings.clone(stored_scope, r.allocator)
		}
		if len(stored_name) > 0 {
			stored_name = strings.clone(stored_name, r.allocator)
		}
		cloned = true
	}

	return Span_Guard{
		recorder       = r,
		start_tick     = time.tick_now(),
		timestamp_ns   = time.now()._nsec,
		span_id        = id,
		prev_parent    = parent,
		prev_status    = prev_status,
		prev_attr      = prev_attr,
		scope          = stored_scope,
		name           = stored_name,
		attr_i64       = attr_i64,
		status         = .Ok,
		strings_cloned = cloned,
	}
}

// Updates the status (and optionally `attr_i64`) of the currently active span on this thread.
span_set_status :: #force_inline proc(
	r:           ^Recorder,
	status:      Status,
	attr_i64:    i64  = 0,
	update_attr: bool = false,
) {
	if r == nil || !r.enabled || tls_active_span_id == 0 {
		return
	}
	tls_active_span_status = status
	if update_attr {
		tls_active_span_attr = attr_i64
	}
}

// Marks the currently active span on this thread as `Status.Error` (and optionally sets `attr_i64`).
span_set_error :: #force_inline proc(
	r:           ^Recorder,
	attr_i64:    i64  = 0,
	update_attr: bool = true,
) {
	span_set_status(r, .Error, attr_i64, update_attr)
}

span_end :: #force_inline proc(g: Span_Guard) {
	if g.recorder == nil {
		return
	}
	dur_ns := i64(time.tick_since(g.start_tick))

	final_status := g.status   if g.status != .Ok                  else tls_active_span_status
	final_attr   := g.attr_i64 if tls_active_span_attr == g.attr_i64 else tls_active_span_attr

	tls_active_span_id     = g.prev_parent
	tls_active_span_status = g.prev_status
	tls_active_span_attr   = g.prev_attr

	record_raw(g.recorder, Record{
		timestamp_ns = g.timestamp_ns,
		duration_ns  = dur_ns,
		span_id      = g.span_id,
		parent_id    = g.prev_parent,
		thread_id    = current_tid(),
		kind         = .Span,
		status       = final_status,
		scope        = g.scope,
		name         = g.name,
		value_f64    = f64(dur_ns) / 1e6, // Convenience: duration in ms in value_f64
		attr_i64     = final_attr,
	}, g.strings_cloned)
}

counter_add :: #force_inline proc(r: ^Recorder, scope, name: string, delta: f64 = 1.0, attr_i64: i64 = 0) {
	if r == nil || !r.enabled {
		return
	}
	emit_point(r, .Counter, scope, name, delta, attr_i64, .Ok)
}

gauge_set :: #force_inline proc(r: ^Recorder, scope, name: string, val: f64, attr_i64: i64 = 0) {
	if r == nil || !r.enabled {
		return
	}
	emit_point(r, .Gauge, scope, name, val, attr_i64, .Ok)
}

histogram_record :: #force_inline proc(r: ^Recorder, scope, name: string, sample: f64, attr_i64: i64 = 0) {
	if r == nil || !r.enabled {
		return
	}
	emit_point(r, .Histogram, scope, name, sample, attr_i64, .Ok)
}

event_emit :: #force_inline proc(r: ^Recorder, scope, name: string, attr_i64: i64 = 0, status: Status = .Ok) {
	if r == nil || !r.enabled {
		return
	}
	emit_point(r, .Event, scope, name, 0.0, attr_i64, status)
}

@(private)
emit_point :: #force_inline proc(
	r: ^Recorder,
	kind: Kind,
	scope, name: string,
	val: f64,
	attr_i64: i64,
	status: Status,
) {
	record_raw(r, Record{
		timestamp_ns = time.now()._nsec,
		duration_ns  = 0,
		span_id      = 0,
		parent_id    = tls_active_span_id,
		thread_id    = current_tid(),
		kind         = kind,
		status       = status,
		scope        = scope,
		name         = name,
		value_f64    = val,
		attr_i64     = attr_i64,
	})
}

// -----------------------------------------------------------------------------
// Hot-Path #soa Staging Ring
// -----------------------------------------------------------------------------

@(private)
free_record_strings :: proc(rec: ^Record, allocator: mem.Allocator) {
	if len(rec.scope) > 0 {
		delete(rec.scope, allocator)
		rec.scope = ""
	}
	if len(rec.name) > 0 {
		delete(rec.name, allocator)
		rec.name = ""
	}
}

@(private)
record_raw :: proc(r: ^Recorder, rec: Record, already_cloned := false) {
	sync.mutex_lock(&r.mu)
	defer sync.mutex_unlock(&r.mu)

	stored := rec
	if !r.enabled {
		if already_cloned {
			free_record_strings(&stored, r.allocator)
		}
		return
	}

	if r.active_page_idx < 0 {
		if !acquire_free_page_locked(r) {
			if already_cloned {
				free_record_strings(&stored, r.allocator)
			}
			_ = intrinsics.atomic_add(&r.dropped_records, 1)
			return
		}
	}

	if r.cfg.string_Fallback_copy && !already_cloned {
		if len(stored.scope) > 0 {
			stored.scope = strings.clone(stored.scope, r.allocator)
		}
		if len(stored.name) > 0 {
			stored.name = strings.clone(stored.name, r.allocator)
		}
	}

	page := &r.pages[r.active_page_idx]
	page.rows[page.count] = stored
	page.count += 1

	if page.count >= BATCH_CAP {
		_ = rotate_active_page_locked(r)
	}
}

@(private)
free_page_strings :: proc(page: ^Staging_Page, allocator: mem.Allocator) {
	for i in 0 ..< page.count {
		if len(page.rows.scope[i]) > 0 {
			delete(page.rows.scope[i], allocator)
			page.rows.scope[i] = ""
		}
		if len(page.rows.name[i]) > 0 {
			delete(page.rows.name[i], allocator)
			page.rows.name[i] = ""
		}
	}
}

@(private)
rotate_active_page_locked :: proc(r: ^Recorder) -> bool {
	append(&r.ready_queue, r.active_page_idx)
	r.active_page_idx = -1
	sync.cond_signal(&r.cv_work_ready)
	return acquire_free_page_locked(r)
}

@(private)
acquire_free_page_locked :: proc(r: ^Recorder) -> bool {
	for len(r.free_stack) == 0 {
		if r.cfg.policy == .Drop || r.shutdown {
			return false
		}
		sync.cond_wait(&r.cv_page_free, &r.mu)
	}
	idx := pop(&r.free_stack)
	r.pages[idx].count = 0
	r.active_page_idx  = idx
	return true
}

// -----------------------------------------------------------------------------
// Single-Threaded Background OdinArrow Worker (Owns Buffer_Pool & Disk I/O)
// -----------------------------------------------------------------------------

@(private)
worker_proc :: proc(r: ^Recorder) {
	// Buffer_Pool is owned 100% by this worker thread — zero mutex overhead!
	pool: oa.Buffer_Pool
	oa.buffer_pool_init(&pool, r.allocator)
	defer oa.buffer_pool_destroy(&pool)

	context.allocator = oa.buffer_pool_allocator(&pool)

	schema, schema_err := oa.schema_make([]oa.Field{
		oa.field_make("timestamp_ns", oa.Int64_Type{}),
		oa.field_make("duration_ns",  oa.Int64_Type{}),
		oa.field_make("span_id",      oa.UInt64_Type{}),
		oa.field_make("parent_id",    oa.UInt64_Type{}),
		oa.field_make("thread_id",    oa.UInt32_Type{}),
		oa.field_make("kind",         oa.UInt8_Type{}),
		oa.field_make("status",       oa.UInt8_Type{}),
		oa.field_make("scope",        oa.String_Type{}),
		oa.field_make("name",         oa.String_Type{}),
		oa.field_make("value_f64",    oa.Float64_Type{}),
		oa.field_make("attr_i64",     oa.Int64_Type{}),
	})
	if schema_err != nil {
		return
	}
	defer oa.schema_free(&schema)

	segment_batches := make([dynamic]oa.Record_Batch, 0, r.cfg.batches_per_file, r.allocator)
	defer delete(segment_batches)

	file_seq: u64 = 0

	for {
		sync.mutex_lock(&r.mu)
		for ((len(r.ready_queue) == 0 && !r.flush_requested) || r.worker_paused) && !r.shutdown {
			sync.cond_wait(&r.cv_work_ready, &r.mu)
		}

		has_page         := len(r.ready_queue) > 0
		is_shutting_down := r.shutdown
		is_flushing      := r.flush_requested
		page_idx         := -1

		if has_page {
			page_idx = r.ready_queue[0]
			ordered_remove(&r.ready_queue, 0)
		}
		queue_drained := len(r.ready_queue) == 0
		sync.mutex_unlock(&r.mu)

		if has_page {
			page := &r.pages[page_idx]
			if batch, ok := build_arrow_batch(&schema, page); ok {
				append(&segment_batches, batch)
			}
			if r.cfg.string_Fallback_copy {
				free_page_strings(page, r.allocator)
			}

			// Return the #soa staging page immediately to producers
			sync.mutex_lock(&r.mu)
			page.count = 0
			append(&r.free_stack, page_idx)
			sync.cond_signal(&r.cv_page_free)
			sync.mutex_unlock(&r.mu)
		}

		// Flush multi-batch .arrow segment file when full, on explicit flush, or on shutdown drain
		should_flush := len(segment_batches) >= r.cfg.batches_per_file ||
			((is_shutting_down || is_flushing) && queue_drained && len(segment_batches) > 0)

		if should_flush {
			filename := fmt.tprintf("%s_%06d.arrow", r.cfg.file_prefix, file_seq)
			_ = oa.ipc_write_file(filename, &schema, segment_batches[:])
			file_seq += 1

			// Freeing all batches returns their backing buffers to `pool` for the next segment
			for &b in segment_batches {
				oa.record_batch_free(&b)
			}
			clear(&segment_batches)
			_ = intrinsics.atomic_add(&r.flushed_batches, 1)
			free_all(context.temp_allocator)
		}

		if is_flushing && queue_drained {
			sync.mutex_lock(&r.mu)
			if len(r.ready_queue) == 0 {
				r.flush_requested = false
				sync.cond_broadcast(&r.cv_flush_done)
			}
			sync.mutex_unlock(&r.mu)
		}

		if is_shutting_down && !has_page {
			break
		}
	}
}

@(private)
build_arrow_batch :: proc(schema: ^oa.Schema, page: ^Staging_Page) -> (batch: oa.Record_Batch, ok: bool) {
	n := page.count
	// Always allocate builders with fixed BATCH_CAP so Buffer_Pool reuses identical size buckets
	ts_b   := oa.builder_make(i64, BATCH_CAP)
	dur_b  := oa.builder_make(i64, BATCH_CAP)
	sid_b  := oa.builder_make(u64, BATCH_CAP)
	pid_b  := oa.builder_make(u64, BATCH_CAP)
	tid_b  := oa.builder_make(u32, BATCH_CAP)
	kind_b := oa.builder_make(u8,  BATCH_CAP)
	stat_b := oa.builder_make(u8,  BATCH_CAP)
	scp_b  := oa.string_builder_make(BATCH_CAP)
	name_b := oa.string_builder_make(BATCH_CAP)
	val_b  := oa.builder_make(f64, BATCH_CAP)
	attr_b := oa.builder_make(i64, BATCH_CAP)

	// Because `page.rows` is `#soa[]Record`, each field is already a contiguous array!
	for i in 0 ..< n {
		oa.builder_append(&ts_b,   page.rows.timestamp_ns[i])
		oa.builder_append(&dur_b,  page.rows.duration_ns[i])
		oa.builder_append(&sid_b,  page.rows.span_id[i])
		oa.builder_append(&pid_b,  page.rows.parent_id[i])
		oa.builder_append(&tid_b,  page.rows.thread_id[i])
		oa.builder_append(&kind_b, u8(page.rows.kind[i]))
		oa.builder_append(&stat_b, u8(page.rows.status[i]))
		oa.string_builder_append(&scp_b,  page.rows.scope[i])
		oa.string_builder_append(&name_b, page.rows.name[i])
		oa.builder_append(&val_b,  page.rows.value_f64[i])
		oa.builder_append(&attr_b, page.rows.attr_i64[i])
	}

	ts_col,   _ := oa.builder_finish(&ts_b);   oa.builder_destroy(&ts_b)
	dur_col,  _ := oa.builder_finish(&dur_b);  oa.builder_destroy(&dur_b)
	sid_col,  _ := oa.builder_finish(&sid_b);  oa.builder_destroy(&sid_b)
	pid_col,  _ := oa.builder_finish(&pid_b);  oa.builder_destroy(&pid_b)
	tid_col,  _ := oa.builder_finish(&tid_b);  oa.builder_destroy(&tid_b)
	kind_col, _ := oa.builder_finish(&kind_b); oa.builder_destroy(&kind_b)
	stat_col, _ := oa.builder_finish(&stat_b); oa.builder_destroy(&stat_b)
	scp_col,  _ := oa.string_builder_finish(&scp_b);  oa.string_builder_destroy(&scp_b)
	name_col, _ := oa.string_builder_finish(&name_b); oa.string_builder_destroy(&name_b)
	val_col,  _ := oa.builder_finish(&val_b);  oa.builder_destroy(&val_b)
	attr_col, _ := oa.builder_finish(&attr_b); oa.builder_destroy(&attr_b)

	return oa.record_batch_make(schema, []oa.Array{
		ts_col, dur_col, sid_col, pid_col, tid_col,
		kind_col, stat_col, scp_col, name_col, val_col, attr_col,
	})
}
