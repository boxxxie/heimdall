package heimdall

import "base:intrinsics"
@(require) import "core:c"
import "core:fmt"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:thread"
@(require) import "core:time"
import oa "../deps/OdinArrow/src"
import "../deps/sleipnir/actor"

when ODIN_OS == .Linux {
	foreign import libc "system:c"

	@(private)
	CLOCK_REALTIME  :: c.int(0)
	@(private)
	CLOCK_MONOTONIC :: c.int(1)

	@(private)
	Posix_Timespec :: struct {
		tv_sec:  i64,
		tv_nsec: i64,
	}

	@(default_calling_convention = "c")
	foreign libc {
		clock_gettime :: proc(clk_id: c.int, tp: ^Posix_Timespec) -> c.int ---
	}

	// Userspace vDSO monotonic clock read (~12-15 ns vs ~500 ns raw kernel syscall in `core:time`).
	@(private)
	monotonic_now_ns :: #force_inline proc "contextless" () -> i64 {
		ts: Posix_Timespec
		clock_gettime(CLOCK_MONOTONIC, &ts)
		return ts.tv_sec * 1_000_000_000 + ts.tv_nsec
	}

	@(private)
	realtime_now_ns :: #force_inline proc "contextless" () -> i64 {
		ts: Posix_Timespec
		clock_gettime(CLOCK_REALTIME, &ts)
		return ts.tv_sec * 1_000_000_000 + ts.tv_nsec
	}
} else {
	@(private)
	monotonic_now_ns :: #force_inline proc "contextless" () -> i64 {
		return time.tick_now()._nsec
	}

	@(private)
	realtime_now_ns :: #force_inline proc "contextless" () -> i64 {
		return time.now()._nsec
	}
}

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

Backend_Mode :: enum u8 {
	Dedicated_Thread = 0, // Default: dedicated background OS thread owns OdinArrow + disk I/O
	Sleipnir_Actor   = 1, // Runs the OdinArrow serializer as a Sleipnir Actor on an `actor.Scheduler`
}

Config :: struct {
	file_prefix:          string,              // e.g., "logs/app_telemetry"
	batches_per_file:     int,                 // Default: 16 (65,536 rows per .arrow file)
	ring_page_count:      int,                 // Default: 4 pages in the producer/consumer ring
	string_Fallback_copy: bool,                // Set true if passing dynamically freed strings
	policy:               Backpressure_Policy,
	backend:              Backend_Mode,        // Default: .Dedicated_Thread (or .Sleipnir_Actor if `scheduler != nil`)
	scheduler:            ^actor.Scheduler,    // Optional shared Sleipnir Scheduler (auto-creates 1-worker scheduler if nil in `.Sleipnir_Actor` mode)
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

@(private)
Flusher_Msg_Kind :: enum u8 {
	Drain_Ready = 0,
	Flush_Sync  = 1,
	Shutdown    = 2,
}

@(private)
Flusher_Msg :: struct {
	kind: Flusher_Msg_Kind,
}

@(private)
Flusher_Engine :: struct {
	mu:              sync.Mutex,
	r:               ^Recorder,
	pool:            oa.Buffer_Pool,
	schema:          oa.Schema,
	segment_batches: [dynamic]oa.Record_Batch,
	file_seq:        u64,
	initialized:     bool,
}

Recorder :: struct {
	allocator:       mem.Allocator,
	cfg:             Config,
	enabled:         bool,
	epoch_offset_ns: i64, // Calibrated `realtime_now_ns() - monotonic_now_ns()` offset

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

	// Background execution backend (Dedicated OS Thread OR Sleipnir Actor)
	worker:          ^thread.Thread,
	sched:           ^actor.Scheduler,
	owned_sched:     bool,
	flusher_actor:   ^actor.Actor(Flusher_Engine, Flusher_Msg),
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

	if r.cfg.scheduler != nil {
		r.cfg.backend = .Sleipnir_Actor
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
		if r.flusher_actor != nil {
			flusher_engine_destroy(&r.flusher_actor.state)
			actor.destroy(r.flusher_actor)
		}
		if r.owned_sched && r.sched != nil {
			actor.scheduler_stop_and_destroy(r.sched)
			free(r.sched, allocator)
		}
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

	r.epoch_offset_ns = realtime_now_ns() - monotonic_now_ns()
	r.next_span_id    = 1
	r.enabled         = true

	if r.cfg.backend == .Sleipnir_Actor {
		if r.cfg.scheduler != nil {
			r.sched       = r.cfg.scheduler
			r.owned_sched = false
		} else if amb_sched := actor.current_scheduler(); amb_sched != nil {
			r.sched       = amb_sched
			r.owned_sched = false
		} else {
			prev_amb := actor.current_scheduler()
			r.sched = new(actor.Scheduler, allocator) or_return
			sched_err := actor.scheduler_init(
				r.sched,
				actor.Scheduler_Config{
					num_workers    = 1,
					num_shards     = 1,
					enable_parking = true,
				},
				allocator,
			)
			actor.use_scheduler(prev_amb)
			if sched_err != nil {
				free(r.sched, allocator)
				r.sched = nil
				return sched_err
			}
			actor.scheduler_start(r.sched)
			r.owned_sched = true
		}

		r.flusher_actor = actor.spawn(
			r.sched,
			Flusher_Engine{r = r},
			flusher_actor_receive,
			0,
			allocator,
		) or_return

		if !flusher_engine_init(&r.flusher_actor.state, r) {
			return .Out_Of_Memory
		}
	} else {
		r.worker = thread.create_and_start_with_poly_data(r, worker_proc)
	}
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

	if r.cfg.backend == .Sleipnir_Actor {
		if r.flusher_actor != nil {
			flusher_engine_drain_ready_pages(&r.flusher_actor.state, true)
			flusher_engine_destroy(&r.flusher_actor.state)
			actor.destroy(r.flusher_actor)
			r.flusher_actor = nil
		}
		if r.owned_sched && r.sched != nil {
			sched_alloc := r.allocator
			actor.scheduler_stop_and_destroy(r.sched)
			free(r.sched, sched_alloc)
			r.sched       = nil
			r.owned_sched = false
		}
	} else if r.worker != nil {
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
	if r.cfg.backend == .Sleipnir_Actor {
		sync.mutex_lock(&r.mu)
		if r.active_page_idx >= 0 && r.pages[r.active_page_idx].count > 0 {
			rotate_active_page_locked(r)
		}
		sync.mutex_unlock(&r.mu)
		if r.flusher_actor != nil {
			flusher_engine_drain_ready_pages(&r.flusher_actor.state, true)
		}
		return
	}

	sync.mutex_lock(&r.mu)
	defer sync.mutex_unlock(&r.mu)

	if r.active_page_idx >= 0 && r.pages[r.active_page_idx].count > 0 {
		rotate_active_page_locked(r)
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
	has_ready := len(r.ready_queue) > 0
	if !paused {
		if r.cfg.backend == .Sleipnir_Actor {
			if has_ready && r.flusher_actor != nil {
				actor.send(r.sched, r.flusher_actor, Flusher_Msg{kind = .Drain_Ready})
			}
		} else {
			sync.cond_signal(&r.cv_work_ready)
		}
	}
	sync.mutex_unlock(&r.mu)
}

// -----------------------------------------------------------------------------
// Library Instrumentation API (Zero-Cost when `r == nil` or disabled)
// -----------------------------------------------------------------------------

Span_Guard :: struct {
	recorder:       ^Recorder,
	start_mono_ns:  i64,
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
		start_mono_ns  = monotonic_now_ns(),
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
	end_mono_ns := monotonic_now_ns()
	dur_ns      := max(end_mono_ns - g.start_mono_ns, 0)
	ts_ns       := g.start_mono_ns + g.recorder.epoch_offset_ns

	final_status := g.status   if g.status != .Ok                  else tls_active_span_status
	final_attr   := g.attr_i64 if tls_active_span_attr == g.attr_i64 else tls_active_span_attr

	tls_active_span_id     = g.prev_parent
	tls_active_span_status = g.prev_status
	tls_active_span_attr   = g.prev_attr

	record_raw(
		g.recorder,
		ts_ns,
		dur_ns,
		g.span_id,
		g.prev_parent,
		current_tid(),
		.Span,
		final_status,
		g.scope,
		g.name,
		f64(dur_ns) / 1e6, // Convenience: duration in ms in value_f64
		final_attr,
		g.strings_cloned,
	)
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
	ts_ns := monotonic_now_ns() + r.epoch_offset_ns
	record_raw(
		r,
		ts_ns,
		0,
		0,
		tls_active_span_id,
		current_tid(),
		kind,
		status,
		scope,
		name,
		val,
		attr_i64,
		false,
	)
}

// -----------------------------------------------------------------------------
// Hot-Path #soa Staging Ring
// -----------------------------------------------------------------------------

@(private)
free_cloned_pair :: #force_inline proc(scope, name: string, allocator: mem.Allocator) {
	if len(scope) > 0 {
		delete(scope, allocator)
	}
	if len(name) > 0 {
		delete(name, allocator)
	}
}

@(private)
record_raw :: #force_inline proc(
	r:              ^Recorder,
	timestamp_ns:   i64,
	duration_ns:    i64,
	span_id:        u64,
	parent_id:      u64,
	thread_id:      u32,
	kind:           Kind,
	status:         Status,
	scope:          string,
	name:           string,
	value_f64:      f64,
	attr_i64:       i64,
	already_cloned: bool = false,
) {
	stored_scope := scope
	stored_name  := name
	owns_strings := already_cloned

	if r.cfg.string_Fallback_copy && !already_cloned {
		if len(stored_scope) > 0 {
			stored_scope = strings.clone(stored_scope, r.allocator)
		}
		if len(stored_name) > 0 {
			stored_name = strings.clone(stored_name, r.allocator)
		}
		owns_strings = true
	}

	sync.mutex_lock(&r.mu)

	if !r.enabled {
		sync.mutex_unlock(&r.mu)
		if owns_strings {
			free_cloned_pair(stored_scope, stored_name, r.allocator)
		}
		return
	}

	if r.active_page_idx < 0 {
		if !acquire_free_page_locked(r) {
			sync.mutex_unlock(&r.mu)
			if owns_strings {
				free_cloned_pair(stored_scope, stored_name, r.allocator)
			}
			_ = intrinsics.atomic_add(&r.dropped_records, 1)
			return
		}
	}

	page := &r.pages[r.active_page_idx]
	idx  := page.count
	page.count = idx + 1

	page.rows.timestamp_ns[idx] = timestamp_ns
	page.rows.duration_ns[idx]  = duration_ns
	page.rows.span_id[idx]      = span_id
	page.rows.parent_id[idx]    = parent_id
	page.rows.thread_id[idx]    = thread_id
	page.rows.kind[idx]         = kind
	page.rows.status[idx]       = status
	page.rows.scope[idx]        = stored_scope
	page.rows.name[idx]         = stored_name
	page.rows.value_f64[idx]    = value_f64
	page.rows.attr_i64[idx]     = attr_i64

	if idx + 1 >= BATCH_CAP {
		rotate_active_page_locked(r)
	}
	sync.mutex_unlock(&r.mu)
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
rotate_active_page_locked :: proc(r: ^Recorder) {
	was_empty := len(r.ready_queue) == 0
	append(&r.ready_queue, r.active_page_idx)
	if len(r.free_stack) > 0 {
		next_idx := pop(&r.free_stack)
		r.pages[next_idx].count = 0
		r.active_page_idx = next_idx
	} else {
		r.active_page_idx = -1
	}
	if r.cfg.backend == .Sleipnir_Actor {
		if was_empty && !r.worker_paused && r.flusher_actor != nil {
			actor.send(r.sched, r.flusher_actor, Flusher_Msg{kind = .Drain_Ready})
		}
	} else {
		sync.cond_signal(&r.cv_work_ready)
	}
}

@(private)
acquire_free_page_locked :: proc(r: ^Recorder) -> bool {
	for r.active_page_idx < 0 {
		if len(r.free_stack) > 0 {
			idx := pop(&r.free_stack)
			r.pages[idx].count = 0
			r.active_page_idx  = idx
			return true
		}
		if r.cfg.policy == .Drop || r.shutdown {
			return false
		}
		if r.cfg.backend == .Sleipnir_Actor && !r.worker_paused && r.flusher_actor != nil {
			sync.mutex_unlock(&r.mu)
			flusher_engine_drain_ready_pages(&r.flusher_actor.state, false)
			sync.mutex_lock(&r.mu)
			continue
		}
		sync.cond_wait(&r.cv_page_free, &r.mu)
	}
	return true
}

// -----------------------------------------------------------------------------
// Background OdinArrow Serializer (Shared by Dedicated Thread & Sleipnir Actor)
// -----------------------------------------------------------------------------

@(private)
flusher_engine_init :: proc(eng: ^Flusher_Engine, r: ^Recorder) -> bool {
	eng.r        = r
	eng.file_seq = 0
	oa.buffer_pool_init(&eng.pool, r.allocator)

	prev_alloc := context.allocator
	context.allocator = oa.buffer_pool_allocator(&eng.pool)
	defer context.allocator = prev_alloc

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
		oa.buffer_pool_destroy(&eng.pool)
		return false
	}
	eng.schema = schema

	batches, b_err := make([dynamic]oa.Record_Batch, 0, r.cfg.batches_per_file, r.allocator)
	if b_err != nil {
		oa.schema_free(&eng.schema)
		oa.buffer_pool_destroy(&eng.pool)
		return false
	}
	eng.segment_batches = batches
	eng.initialized     = true
	return true
}

@(private)
flusher_engine_destroy :: proc(eng: ^Flusher_Engine) {
	sync.mutex_lock(&eng.mu)
	defer sync.mutex_unlock(&eng.mu)
	if !eng.initialized {
		return
	}
	prev_alloc := context.allocator
	context.allocator = oa.buffer_pool_allocator(&eng.pool)

	for &b in eng.segment_batches {
		oa.record_batch_free(&b)
	}
	delete(eng.segment_batches)
	oa.schema_free(&eng.schema)

	context.allocator = prev_alloc
	oa.buffer_pool_destroy(&eng.pool)
	eng.initialized = false
}

@(private)
flusher_engine_flush_segment :: proc(eng: ^Flusher_Engine) {
	if len(eng.segment_batches) == 0 {
		return
	}
	r        := eng.r
	filename := fmt.tprintf("%s_%06d.arrow", r.cfg.file_prefix, eng.file_seq)
	_ = oa.ipc_write_file(filename, &eng.schema, eng.segment_batches[:])
	eng.file_seq += 1

	for &b in eng.segment_batches {
		oa.record_batch_free(&b)
	}
	clear(&eng.segment_batches)
	_ = intrinsics.atomic_add(&r.flushed_batches, 1)
	free_all(context.temp_allocator)
}

@(private)
flusher_engine_drain_ready_pages :: proc(eng: ^Flusher_Engine, force_flush: bool) {
	sync.mutex_lock(&eng.mu)
	defer sync.mutex_unlock(&eng.mu)
	if !eng.initialized {
		return
	}
	r := eng.r
	prev_alloc := context.allocator
	context.allocator = oa.buffer_pool_allocator(&eng.pool)
	defer context.allocator = prev_alloc

	for {
		sync.mutex_lock(&r.mu)
		if (r.worker_paused && !force_flush && !r.shutdown) || len(r.ready_queue) == 0 {
			sync.mutex_unlock(&r.mu)
			break
		}
		page_idx := r.ready_queue[0]
		ordered_remove(&r.ready_queue, 0)
		sync.mutex_unlock(&r.mu)

		page := &r.pages[page_idx]
		if batch, ok := build_arrow_batch(&eng.schema, page); ok {
			append(&eng.segment_batches, batch)
		}
		if r.cfg.string_Fallback_copy {
			free_page_strings(page, r.allocator)
		}

		// Return the #soa staging page immediately to producers
		sync.mutex_lock(&r.mu)
		page.count = 0
		append(&r.free_stack, page_idx)
		sync.cond_broadcast(&r.cv_page_free)
		sync.mutex_unlock(&r.mu)

		if len(eng.segment_batches) >= r.cfg.batches_per_file {
			flusher_engine_flush_segment(eng)
		}
	}

	if force_flush && len(eng.segment_batches) > 0 {
		flusher_engine_flush_segment(eng)
	}
}

@(private)
flusher_actor_receive :: proc(ctx: ^actor.Context, state: ^Flusher_Engine, msg: ^Flusher_Msg) {
	switch msg.kind {
	case .Drain_Ready:
		flusher_engine_drain_ready_pages(state, false)
		actor.reply(ctx, true)
	case .Flush_Sync:
		flusher_engine_drain_ready_pages(state, true)
		actor.reply(ctx, true)
	case .Shutdown:
		flusher_engine_drain_ready_pages(state, true)
		flusher_engine_destroy(state)
		actor.reply(ctx, true)
	}
}

@(private)
worker_proc :: proc(r: ^Recorder) {
	eng: Flusher_Engine
	if !flusher_engine_init(&eng, r) {
		return
	}
	defer flusher_engine_destroy(&eng)

	for {
		sync.mutex_lock(&r.mu)
		for ((len(r.ready_queue) == 0 && !r.flush_requested) || r.worker_paused) && !r.shutdown {
			sync.cond_wait(&r.cv_work_ready, &r.mu)
		}
		is_shutting_down := r.shutdown
		is_flushing      := r.flush_requested
		sync.mutex_unlock(&r.mu)

		flusher_engine_drain_ready_pages(&eng, is_shutting_down || is_flushing)

		if is_flushing {
			sync.mutex_lock(&r.mu)
			if len(r.ready_queue) == 0 {
				r.flush_requested = false
				sync.cond_broadcast(&r.cv_flush_done)
			}
			sync.mutex_unlock(&r.mu)
		}

		if is_shutting_down {
			sync.mutex_lock(&r.mu)
			done := len(r.ready_queue) == 0
			sync.mutex_unlock(&r.mu)
			if done {
				break
			}
		}
	}
}

@(private)
build_primitive_column_from_soa :: #force_inline proc(
	$T:      typeid,
	dt:      oa.DataType,
	src_ptr: [^]T,
	n:       int,
) -> oa.Array {
	buf, _ := oa.buffer_make_uninit(BATCH_CAP * size_of(T))
	buf.size = n * size_of(T)
	if n > 0 {
		mem.copy_non_overlapping(rawptr(buf.data), rawptr(src_ptr), n * size_of(T))
	}
	return oa.Array{
		type       = dt,
		length     = n,
		null_count = 0,
		offset     = 0,
		buffers    = {{}, buf, {}},
	}
}

@(private)
build_string_column_from_soa :: #force_inline proc(
	src_strings: [^]string,
	n:           int,
) -> oa.Array {
	off_buf, _ := oa.buffer_make_uninit((BATCH_CAP + 1) * size_of(i32))
	off_buf.size = (n + 1) * size_of(i32)
	offsets := cast([^]i32)off_buf.data
	offsets[0] = 0

	total_bytes := 0
	for i in 0 ..< n {
		total_bytes += len(src_strings[i])
		offsets[i + 1] = i32(total_bytes)
	}

	data_buf, _ := oa.buffer_make_uninit(max(total_bytes, 64))
	data_buf.size = total_bytes
	dst := data_buf.data
	pos := 0
	for i in 0 ..< n {
		s := src_strings[i]
		s_len := len(s)
		if s_len > 0 {
			mem.copy_non_overlapping(rawptr(&dst[pos]), rawptr(raw_data(s)), s_len)
			pos += s_len
		}
	}

	return oa.Array{
		type       = oa.String_Type{},
		length     = n,
		null_count = 0,
		offset     = 0,
		buffers    = {{}, off_buf, data_buf},
	}
}

@(private)
build_arrow_batch :: proc(schema: ^oa.Schema, page: ^Staging_Page) -> (batch: oa.Record_Batch, ok: bool) {
	n := page.count

	// Bulk-copy each contiguous `#soa` primitive column slice directly into an
	// Arrow Buffer_Pool buffer (single `memcpy` per column, zero bitmap allocs).
	ts_col   := build_primitive_column_from_soa(i64, oa.Int64_Type{},   page.rows.timestamp_ns,     n)
	dur_col  := build_primitive_column_from_soa(i64, oa.Int64_Type{},   page.rows.duration_ns,      n)
	sid_col  := build_primitive_column_from_soa(u64, oa.UInt64_Type{},  page.rows.span_id,          n)
	pid_col  := build_primitive_column_from_soa(u64, oa.UInt64_Type{},  page.rows.parent_id,        n)
	tid_col  := build_primitive_column_from_soa(u32, oa.UInt32_Type{},  page.rows.thread_id,        n)
	kind_col := build_primitive_column_from_soa(u8,  oa.UInt8_Type{},   cast([^]u8)page.rows.kind,   n)
	stat_col := build_primitive_column_from_soa(u8,  oa.UInt8_Type{},   cast([^]u8)page.rows.status, n)
	scp_col  := build_string_column_from_soa(page.rows.scope, n)
	name_col := build_string_column_from_soa(page.rows.name,  n)
	val_col  := build_primitive_column_from_soa(f64, oa.Float64_Type{}, page.rows.value_f64,        n)
	attr_col := build_primitive_column_from_soa(i64, oa.Int64_Type{},   page.rows.attr_i64,         n)

	return oa.record_batch_make(schema, []oa.Array{
		ts_col, dur_col, sid_col, pid_col, tid_col,
		kind_col, stat_col, scp_col, name_col, val_col, attr_col,
	})
}
