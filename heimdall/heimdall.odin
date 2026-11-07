package heimdall

import "base:intrinsics"
import "base:runtime"
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
	monotonic_now_ns :: #force_inline proc() -> i64 {
		ts: Posix_Timespec
		clock_gettime(CLOCK_MONOTONIC, &ts)
		return ts.tv_sec * 1_000_000_000 + ts.tv_nsec
	}

	@(private)
	realtime_now_ns :: #force_inline proc() -> i64 {
		ts: Posix_Timespec
		clock_gettime(CLOCK_REALTIME, &ts)
		return ts.tv_sec * 1_000_000_000 + ts.tv_nsec
	}
} else {
	@(private)
	monotonic_now_ns :: #force_inline proc() -> i64 {
		return time.tick_now()._nsec
	}

	@(private)
	realtime_now_ns :: #force_inline proc() -> i64 {
		return time.now()._nsec
	}
}

BATCH_CAP            :: 4096
DEFAULT_RING_PAGES   :: 4  // 2+ pages allow lock-free I/O overlap
DEFAULT_SEGMENT_SIZE :: 16 // 16 batches * 4096 rows = 65,536 rows per .arrow file
MAX_TLS_RECORDERS    :: 16 // Concurrent active (Recorder, Actor_Turn) span slots per OS thread
SPAN_ID_BATCH_SIZE   :: 64 // Thread-local span ID reservation batch size

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
	string_fallback_copy: bool,                // Set true if passing dynamically freed strings
	policy:               Backpressure_Policy,
	backend:              Backend_Mode,        // Default: .Dedicated_Thread (or .Sleipnir_Actor if `scheduler != nil`)
	scheduler:            ^actor.Scheduler,    // Optional shared Sleipnir Scheduler (auto-creates 1-worker scheduler if nil in `.Sleipnir_Actor` mode)
}

// Canonical telemetry row stored in #soa layout inside each staging page.
// Parsed by `tools/heimdall_gen` (`core:odin/parser`) to generate `heimdall/schema_gen.odin`
// (`make_record_arrow_schema`, `stage_record_into_soa`, `build_arrow_batch`, and `Record_Batch_Reader`).
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
	rows:      #soa[]Record,
	count:     int,
	_pad:      [64]u8,
	committed: int,
}

@(private)
Flusher_Msg_Kind :: enum u8 {
	Drain_Ready = 0, // Async signal via actor.send
	Drain_Sync  = 1, // Synchronous backpressure drain via actor.call
	Flush_Sync  = 2, // Synchronous flush of partial + ready pages via actor.call
	Shutdown    = 3, // Synchronous drain + engine teardown via actor.call
}

@(private)
Flusher_Msg :: struct {
	kind: Flusher_Msg_Kind,
}

@(private)
Flusher_Engine :: struct {
	// 100% single-actor (or single-worker-thread) owned — no mutex needed!
	r:               ^Recorder,
	pool:            oa.Buffer_Pool,
	schema:          oa.Schema,
	segment_batches: [dynamic]oa.Record_Batch,
	file_seq:        u64,
	initialized:     bool,
}

Recorder :: struct #align(64) {
	// Zone 1: Read-mostly configuration & allocator state (shared across cores without invalidation)
	enabled:            bool,
	_instance_id:       u64,
	epoch_offset_ns:    i64, // Calibrated `realtime_now_ns() - monotonic_now_ns()` offset
	allocator:          mem.Allocator,
	_page_alloc:        mem.Allocator,
	_owned_file_prefix: string,
	cfg:                Config,
	_pages:             []Staging_Page,

	// Zone 2: Producer-Consumer Ring of #soa pages & lock state (cache-line isolated)
	_pad0:                 [64]u8,
	_mu:                   sync.Mutex,
	_active_page_idx:      int,
	_free_stack:           []int,
	_free_top:             int,
	_ready_ring:           []int,
	_ready_head:           int,
	_ready_tail:           int,
	_ready_count:          int,
	_waiting_producers:    int,
	_inflight_actor_calls: int,
	_flush_target_gen:     u64,
	_flush_completed_gen:  u64,
	_worker_sleeping:      bool,
	_worker_paused:        bool,
	_flush_requested:      bool,
	_shutdown:             bool,
	_cv_work_ready:        sync.Cond,
	_cv_page_free:         sync.Cond,
	_cv_flush_done:        sync.Cond,
	_cv_actor_idle:        sync.Cond,
	_alloc_mu:             sync.Mutex,

	// Zone 3: Atomic counters & background execution backend (cache-line isolated)
	_pad1:             [64]u8,
	_next_span_id:     u64,
	_dropped_records:  u64,
	_flushed_segments: u64,
	_eng:              Flusher_Engine,
	_worker:           ^thread.Thread,
	_sched:            ^actor.Scheduler,
	_owned_sched:      bool,
	_flusher_actor:    ^actor.Actor(Flusher_Engine, Flusher_Msg),
}

// Per-(Recorder, Actor_Turn) thread-local active span state so nested calls across libraries
// automatically wire up `parent_id -> span_id` call trees without colliding when multiple
// `Recorder` instances or cooperative `actor.call` turns share the same OS thread.
@(thread_local)
tls_active_span_id: u64

@(private)
TLS_Recorder_Span :: struct {
	recorder:        ^Recorder,
	actor_env:       rawptr,
	span_id:         u64,
	attr_i64:        i64,
	status:          Status,
	attr_overridden: bool,
}

@(private)
@(thread_local)
tls_recorder_spans: [MAX_TLS_RECORDERS]TLS_Recorder_Span

@(private)
@(thread_local)
tls_active_recorder_count: int

@(private)
TLS_Span_ID_Cache :: struct {
	instance_id: u64,
	next_id:     u64,
	limit_id:    u64,
}

@(private)
@(thread_local)
tls_span_id_cache: [2]TLS_Span_ID_Cache

@(private)
g_next_recorder_instance_id: u64

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

@(private)
current_actor_env :: #force_inline proc() -> rawptr {
	ctx := actor.current_context()
	if ctx != nil {
		return ctx.current_env
	}
	return nil
}

@(private)
next_span_id :: #force_inline proc(r: ^Recorder) -> u64 {
	inst := r._instance_id
	if tls_span_id_cache[0].instance_id == inst && tls_span_id_cache[0].next_id < tls_span_id_cache[0].limit_id {
		id := tls_span_id_cache[0].next_id
		tls_span_id_cache[0].next_id = id + 1
		return id
	}
	if tls_span_id_cache[1].instance_id == inst && tls_span_id_cache[1].next_id < tls_span_id_cache[1].limit_id {
		id := tls_span_id_cache[1].next_id
		tls_span_id_cache[1].next_id = id + 1
		return id
	}
	base := intrinsics.atomic_add_explicit(&r._next_span_id, SPAN_ID_BATCH_SIZE, .Relaxed)
	tls_span_id_cache[1] = tls_span_id_cache[0]
	tls_span_id_cache[0] = TLS_Span_ID_Cache{
		instance_id = inst,
		next_id     = base + 1,
		limit_id    = base + SPAN_ID_BATCH_SIZE,
	}
	return base
}

@(private)
tls_acquire_recorder_slot :: #force_inline proc(r: ^Recorder, env: rawptr) -> (slot: ^TLS_Recorder_Span, slot_idx: i8) {
	if tls_active_span_id == 0 {
		tls_recorder_spans[0] = TLS_Recorder_Span{recorder = r, actor_env = env}
		return &tls_recorder_spans[0], 0
	}
	if tls_recorder_spans[0].recorder == r && tls_recorder_spans[0].actor_env == env {
		return &tls_recorder_spans[0], 0
	}
	free_idx := 0
	found_free := tls_recorder_spans[0].recorder == nil || tls_recorder_spans[0].span_id == 0
	for i in 1 ..< MAX_TLS_RECORDERS {
		if tls_recorder_spans[i].recorder == r && tls_recorder_spans[i].actor_env == env {
			return &tls_recorder_spans[i], i8(i)
		}
		if !found_free && (tls_recorder_spans[i].recorder == nil || tls_recorder_spans[i].span_id == 0) {
			free_idx = i
			found_free = true
		}
	}
	tls_recorder_spans[free_idx] = TLS_Recorder_Span{recorder = r, actor_env = env}
	return &tls_recorder_spans[free_idx], i8(free_idx)
}

@(private)
tls_find_recorder_slot :: #force_inline proc(r: ^Recorder) -> ^TLS_Recorder_Span {
	if tls_active_span_id == 0 {
		return nil
	}
	env := current_actor_env()
	if tls_recorder_spans[0].recorder == r && tls_recorder_spans[0].actor_env == env {
		return &tls_recorder_spans[0]
	}
	for i in 1 ..< MAX_TLS_RECORDERS {
		if tls_recorder_spans[i].recorder == r && tls_recorder_spans[i].actor_env == env {
			return &tls_recorder_spans[i]
		}
	}
	return nil
}

@(private)
tls_active_span_for_recorder :: #force_inline proc(r: ^Recorder) -> u64 {
	if tls_active_span_id == 0 {
		return 0
	}
	env := current_actor_env()
	if tls_recorder_spans[0].recorder == r && tls_recorder_spans[0].actor_env == env {
		return tls_recorder_spans[0].span_id
	}
	for i in 1 ..< MAX_TLS_RECORDERS {
		if tls_recorder_spans[i].recorder == r && tls_recorder_spans[i].actor_env == env {
			return tls_recorder_spans[i].span_id
		}
	}
	return 0
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
// Lifecycle Management & Diagnostics
// -----------------------------------------------------------------------------

recorder_init :: proc(
	r: ^Recorder,
	cfg := Config{},
	allocator := context.allocator,
) -> (err: mem.Allocator_Error) {
	r^ = {}
	r.allocator   = allocator
	r._page_alloc = allocator
	r.cfg         = cfg

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

	defer if err != nil {
		for i in 0 ..< len(r._pages) {
			if r._pages[i].rows != nil {
				delete_soa(r._pages[i].rows, allocator)
			}
		}
		delete(r._pages, allocator)
		delete(r._free_stack, allocator)
		delete(r._ready_ring, allocator)
		delete(r._owned_file_prefix, allocator)
		flusher_engine_destroy(&r._eng)
		if r._flusher_actor != nil {
			flusher_engine_destroy(&r._flusher_actor.state)
		}
		if r._owned_sched && r._sched != nil {
			actor.scheduler_stop_and_destroy(r._sched)
			free(r._sched, allocator)
			r._flusher_actor = nil
		} else if r._flusher_actor != nil {
			actor.destroy(r._flusher_actor)
			r._flusher_actor = nil
		}
		r^ = {}
	}

	r._owned_file_prefix = strings.clone(r.cfg.file_prefix, allocator) or_return
	r.cfg.file_prefix    = r._owned_file_prefix

	r._pages = make([]Staging_Page, r.cfg.ring_page_count, allocator) or_return

	for i in 0 ..< len(r._pages) {
		r._pages[i].rows = make_soa(#soa[]Record, BATCH_CAP, allocator) or_return
	}

	r._free_stack = make([]int, r.cfg.ring_page_count, allocator) or_return
	r._ready_ring = make([]int, r.cfg.ring_page_count, allocator) or_return

	// Page 0 starts as the active front buffer; remaining pages go onto `_free_stack`
	r._active_page_idx = 0
	r._free_top        = 0
	for i := r.cfg.ring_page_count - 1; i >= 1; i -= 1 {
		r._free_stack[r._free_top] = i
		r._free_top += 1
	}

	r.epoch_offset_ns = realtime_now_ns() - monotonic_now_ns()
	r._instance_id    = intrinsics.atomic_add_explicit(&g_next_recorder_instance_id, 1, .Relaxed) + 1
	r._next_span_id   = 1

	if r.cfg.backend == .Sleipnir_Actor {
		if r.cfg.scheduler != nil {
			r._sched       = r.cfg.scheduler
			r._owned_sched = false
		} else {
			prev_amb := actor.current_scheduler()
			r._sched = new(actor.Scheduler, allocator) or_return
			sched_err := actor.scheduler_init(
				r._sched,
				actor.Scheduler_Config{
					num_workers    = 1,
					num_shards     = 1,
					enable_parking = true,
				},
				allocator,
			)
			actor.use_scheduler(prev_amb)
			if sched_err != nil {
				free(r._sched, allocator)
				r._sched = nil
				return sched_err
			}
			actor.scheduler_start(r._sched)
			r._owned_sched = true
		}

		r._flusher_actor = actor.spawn(
			r._sched,
			Flusher_Engine{r = r},
			flusher_actor_receive,
			0,
			allocator,
		) or_return

		if !flusher_engine_init(&r._flusher_actor.state, r) {
			return .Out_Of_Memory
		}
	} else {
		if !flusher_engine_init(&r._eng, r) {
			return .Out_Of_Memory
		}
		prev_alloc := context.allocator
		context.allocator = allocator
		r._worker = thread.create_and_start_with_poly_data(r, worker_proc)
		context.allocator = prev_alloc
		if r._worker == nil {
			return .Out_Of_Memory
		}
	}
	intrinsics.atomic_store_explicit(&r.enabled, true, .Release)
	return nil
}

recorder_destroy :: proc(r: ^Recorder) {
	if r == nil || r._pages == nil {
		return
	}

	for i in 0 ..< MAX_TLS_RECORDERS {
		if tls_recorder_spans[i].recorder == r {
			if tls_recorder_spans[i].span_id != 0 {
				tls_active_recorder_count = max(tls_active_recorder_count - 1, 0)
			}
			if tls_active_span_id == tls_recorder_spans[i].span_id {
				tls_active_span_id = 0
			}
			tls_recorder_spans[i] = {}
		}
	}

	sync.mutex_lock(&r._mu)
	intrinsics.atomic_store_explicit(&r.enabled, false, .Release)
	r._worker_paused = false
	if r._active_page_idx >= 0 && r._pages[r._active_page_idx].count > 0 {
		ready_ring_push_locked(r, r._active_page_idx)
		r._active_page_idx = -1
	}
	r._shutdown = true
	sync.cond_signal(&r._cv_work_ready)
	sync.cond_broadcast(&r._cv_page_free)
	sync.cond_broadcast(&r._cv_flush_done)
	for r._inflight_actor_calls > 0 {
		sync.cond_wait(&r._cv_actor_idle, &r._mu)
	}
	sync.mutex_unlock(&r._mu)

	if r.cfg.backend == .Sleipnir_Actor {
		if r._flusher_actor != nil {
			if r._sched != nil && r._sched.started && intrinsics.atomic_load_explicit(&r._sched.running, .Acquire) {
				_ = actor.call(r._sched, r._flusher_actor, Flusher_Msg{kind = .Shutdown}, bool)
			} else {
				flusher_engine_drain_ready_pages(&r._flusher_actor.state, true)
				flusher_engine_destroy(&r._flusher_actor.state)
			}
		}
		if r._owned_sched && r._sched != nil {
			sched_alloc := r._page_alloc
			actor.scheduler_stop_and_destroy(r._sched)
			free(r._sched, sched_alloc)
			r._sched         = nil
			r._owned_sched   = false
			r._flusher_actor = nil
		} else if r._flusher_actor != nil {
			actor.destroy(r._flusher_actor)
			r._flusher_actor = nil
		}
	} else {
		if r._worker != nil {
			thread.join(r._worker)
			prev_alloc := context.allocator
			context.allocator = r._page_alloc
			thread.destroy(r._worker)
			context.allocator = prev_alloc
			r._worker = nil
		}
		flusher_engine_destroy(&r._eng)
	}

	dropped := intrinsics.atomic_load_explicit(&r._dropped_records, .Acquire)
	flushed := intrinsics.atomic_load_explicit(&r._flushed_segments, .Acquire)

	alloc      := r.allocator
	page_alloc := r._page_alloc
	for i in 0 ..< len(r._pages) {
		expected := r._pages[i].count
		for intrinsics.atomic_load_explicit(&r._pages[i].committed, .Acquire) < expected {
			intrinsics.cpu_relax()
		}
		if r.cfg.string_fallback_copy {
			free_page_strings(r, &r._pages[i])
		}
		delete_soa(r._pages[i].rows, page_alloc)
	}
	delete(r._pages, page_alloc)
	delete(r._free_stack, page_alloc)
	delete(r._ready_ring, page_alloc)
	delete(r._owned_file_prefix, page_alloc)
	r^ = {}
	r.allocator         = alloc
	r._page_alloc       = page_alloc
	r._dropped_records  = dropped
	r._flushed_segments = flushed
}

// Returns the number of flushed `.arrow` segment files and dropped telemetry records.
recorder_diagnostics :: #force_inline proc(r: ^Recorder) -> (flushed_segments, dropped_records: u64) {
	if r == nil {
		return 0, 0
	}
	return intrinsics.atomic_load_explicit(&r._flushed_segments, .Acquire),
	       intrinsics.atomic_load_explicit(&r._dropped_records, .Acquire)
}

// Returns whether the recorder is currently enabled.
recorder_is_enabled :: #force_inline proc(r: ^Recorder) -> bool {
	if r == nil {
		return false
	}
	return intrinsics.atomic_load_explicit(&r.enabled, .Relaxed)
}

// Enables or disables telemetry recording at runtime.
recorder_set_enabled :: proc(r: ^Recorder, enabled: bool) {
	if r == nil {
		return
	}
	sync.mutex_lock(&r._mu)
	intrinsics.atomic_store_explicit(&r.enabled, enabled, .Release)
	if !enabled && r._waiting_producers > 0 {
		sync.cond_broadcast(&r._cv_page_free)
	}
	sync.mutex_unlock(&r._mu)
}

// Returns true when all `#soa` ring pages are full and queued for the background flusher.
recorder_is_saturated :: proc(r: ^Recorder) -> bool {
	if r == nil || r._pages == nil {
		return false
	}
	sync.mutex_lock(&r._mu)
	saturated := (r._free_top == 0 && r._active_page_idx == -1 && r._ready_count == r.cfg.ring_page_count)
	sync.mutex_unlock(&r._mu)
	return saturated
}

// Returns the number of producer threads currently blocked waiting for a free `#soa` page.
recorder_waiting_producers :: proc(r: ^Recorder) -> int {
	if r == nil || r._pages == nil {
		return 0
	}
	sync.mutex_lock(&r._mu)
	waiting := r._waiting_producers
	sync.mutex_unlock(&r._mu)
	return waiting
}

// Forces the current partial page and any buffered Arrow batches to disk immediately.
recorder_flush :: proc(r: ^Recorder) {
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		return
	}
	if r.cfg.backend == .Sleipnir_Actor {
		sync.mutex_lock(&r._mu)
		if r._shutdown || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
			sync.mutex_unlock(&r._mu)
			return
		}
		if r._active_page_idx >= 0 && r._pages[r._active_page_idx].count > 0 {
			rotate_active_page_locked(r)
		}
		flusher  := r._flusher_actor
		sched    := r._sched
		can_call := flusher != nil && sched != nil && sched.started && intrinsics.atomic_load_explicit(&sched.running, .Acquire)
		if can_call {
			r._inflight_actor_calls += 1
		}
		sync.mutex_unlock(&r._mu)

		if can_call {
			_ = actor.call(sched, flusher, Flusher_Msg{kind = .Flush_Sync}, bool)
			sync.mutex_lock(&r._mu)
			r._inflight_actor_calls -= 1
			if r._inflight_actor_calls == 0 && r._shutdown {
				sync.cond_broadcast(&r._cv_actor_idle)
			}
			sync.mutex_unlock(&r._mu)
		} else if flusher != nil {
			flusher_engine_drain_ready_pages(&flusher.state, true)
		}
		return
	}

	sync.mutex_lock(&r._mu)
	defer sync.mutex_unlock(&r._mu)

	if r._active_page_idx >= 0 && r._pages[r._active_page_idx].count > 0 {
		rotate_active_page_locked(r)
	}
	r._flush_target_gen += 1
	target_gen := r._flush_target_gen
	r._flush_requested = true
	sync.cond_signal(&r._cv_work_ready)
	for r._flush_completed_gen < target_gen && !r._shutdown {
		sync.cond_wait(&r._cv_flush_done, &r._mu)
	}
}

// Pauses or resumes background worker consumption of ready #soa pages.
recorder_set_worker_paused :: proc(r: ^Recorder, paused: bool) {
	if r == nil {
		return
	}
	sync.mutex_lock(&r._mu)
	r._worker_paused = paused
	has_ready := r._ready_count > 0
	if !paused {
		if r.cfg.backend == .Sleipnir_Actor {
			if has_ready && r._flusher_actor != nil {
				actor.send(r._sched, r._flusher_actor, Flusher_Msg{kind = .Drain_Ready})
			}
		} else {
			sync.cond_signal(&r._cv_work_ready)
		}
	}
	sync.mutex_unlock(&r._mu)
}

// -----------------------------------------------------------------------------
// Library Instrumentation API (Zero-Cost when `r == nil` or disabled)
// -----------------------------------------------------------------------------

Span_Guard :: struct {
	recorder:             ^Recorder,
	start_mono_ns:        i64,
	span_id:              u64,
	prev_parent:          u64,
	prev_attr:            i64,
	scope:                string,
	name:                 string,
	attr_i64:             i64,
	status:               Status,
	prev_status:          Status,
	prev_attr_overridden: bool,
	strings_cloned:       bool,
	tls_slot_idx:         i8,
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
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		return Span_Guard{}
	}
	id             := next_span_id(r)
	slot, slot_idx := tls_acquire_recorder_slot(r, current_actor_env())

	parent               := slot.span_id
	prev_status          := slot.status
	prev_attr            := slot.attr_i64
	prev_attr_overridden := slot.attr_overridden

	if parent == 0 {
		tls_active_recorder_count += 1
	}
	slot.span_id         = id
	slot.status          = .Ok
	slot.attr_i64        = attr_i64
	slot.attr_overridden = false
	tls_active_span_id   = id

	stored_scope := scope
	stored_name  := name
	cloned       := false
	if r.cfg.string_fallback_copy {
		stored_scope, stored_name = clone_string_pair(r, scope, name)
		cloned = true
	}

	return Span_Guard{
		recorder             = r,
		start_mono_ns        = monotonic_now_ns(),
		span_id              = id,
		prev_parent          = parent,
		prev_attr            = prev_attr,
		scope                = stored_scope,
		name                 = stored_name,
		attr_i64             = attr_i64,
		status               = .Ok,
		prev_status          = prev_status,
		prev_attr_overridden = prev_attr_overridden,
		strings_cloned       = cloned,
		tls_slot_idx         = slot_idx,
	}
}

// Updates the status (and optionally `attr_i64`) of the currently active span on `r` on this thread.
span_set_status :: #force_inline proc(
	r:           ^Recorder,
	status:      Status,
	attr_i64:    i64  = 0,
	update_attr: bool = false,
) {
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		return
	}
	slot := tls_find_recorder_slot(r)
	if slot == nil || slot.span_id == 0 {
		return
	}
	slot.status = status
	if update_attr {
		slot.attr_i64        = attr_i64
		slot.attr_overridden = true
	}
}

// Marks the currently active span on `r` on this thread as `Status.Error` (and optionally sets `attr_i64`).
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

	final_status := g.status
	final_attr   := g.attr_i64

	slot_idx := int(g.tls_slot_idx)
	slot: ^TLS_Recorder_Span
	if slot_idx >= 0 && slot_idx < MAX_TLS_RECORDERS && tls_recorder_spans[slot_idx].recorder == g.recorder {
		slot = &tls_recorder_spans[slot_idx]
	} else {
		slot = tls_find_recorder_slot(g.recorder)
	}
	if slot != nil {
		if final_status == .Ok {
			final_status = slot.status
		}
		if slot.attr_overridden {
			final_attr = slot.attr_i64
		}
		if g.prev_parent == 0 {
			slot^ = {}
			tls_active_recorder_count = max(tls_active_recorder_count - 1, 0)
		} else {
			slot.span_id         = g.prev_parent
			slot.status          = g.prev_status
			slot.attr_i64        = g.prev_attr
			slot.attr_overridden = g.prev_attr_overridden
		}
	}
	if g.prev_parent != 0 {
		tls_active_span_id = g.prev_parent
	} else if tls_active_recorder_count == 0 {
		tls_active_span_id = 0
	} else {
		active_id: u64 = 0
		for i in 0 ..< MAX_TLS_RECORDERS {
			if tls_recorder_spans[i].span_id != 0 {
				active_id = tls_recorder_spans[i].span_id
				break
			}
		}
		tls_active_span_id = active_id
	}

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
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		return
	}
	emit_point(r, .Counter, scope, name, delta, attr_i64, .Ok)
}

gauge_set :: #force_inline proc(r: ^Recorder, scope, name: string, val: f64, attr_i64: i64 = 0) {
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		return
	}
	emit_point(r, .Gauge, scope, name, val, attr_i64, .Ok)
}

histogram_record :: #force_inline proc(r: ^Recorder, scope, name: string, sample: f64, attr_i64: i64 = 0) {
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		return
	}
	emit_point(r, .Histogram, scope, name, sample, attr_i64, .Ok)
}

event_emit :: #force_inline proc(r: ^Recorder, scope, name: string, attr_i64: i64 = 0, status: Status = .Ok) {
	if r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
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
		tls_active_span_for_recorder(r),
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
clone_string_pair :: #force_inline proc(r: ^Recorder, scope, name: string) -> (s, n: string) {
	s_len := len(scope)
	n_len := len(name)
	total := s_len + n_len
	if total <= 0 {
		return "", ""
	}
	sync.mutex_lock(&r._alloc_mu)
	buf, err := mem.alloc_bytes_non_zeroed(total, 1, r.allocator)
	sync.mutex_unlock(&r._alloc_mu)
	if err != nil || buf == nil {
		return "", ""
	}
	ptr := raw_data(buf)
	if s_len > 0 {
		mem.copy_non_overlapping(ptr, raw_data(scope), s_len)
		s = string(buf[:s_len])
	}
	if n_len > 0 {
		mem.copy_non_overlapping(rawptr(uintptr(ptr) + uintptr(s_len)), raw_data(name), n_len)
		n = string(buf[s_len:total])
	}
	return
}

@(private)
free_cloned_pair :: #force_inline proc(r: ^Recorder, scope, name: string) {
	s_len := len(scope)
	n_len := len(name)
	total := s_len + n_len
	if total <= 0 {
		return
	}
	base := raw_data(scope) if s_len > 0 else raw_data(name)
	sync.mutex_lock(&r._alloc_mu)
	_ = mem.free_with_size(base, total, r.allocator)
	sync.mutex_unlock(&r._alloc_mu)
}

@(private)
free_stack_push_locked :: #force_inline proc(r: ^Recorder, page_idx: int) {
	r._free_stack[r._free_top] = page_idx
	r._free_top += 1
}

@(private)
free_stack_pop_locked :: #force_inline proc(r: ^Recorder) -> int {
	r._free_top -= 1
	return r._free_stack[r._free_top]
}

@(private)
ready_ring_push_locked :: #force_inline proc(r: ^Recorder, page_idx: int) {
	r._ready_ring[r._ready_tail] = page_idx
	r._ready_tail = (r._ready_tail + 1) % len(r._ready_ring)
	r._ready_count += 1
}

@(private)
ready_ring_pop_locked :: #force_inline proc(r: ^Recorder) -> int {
	page_idx := r._ready_ring[r._ready_head]
	r._ready_head = (r._ready_head + 1) % len(r._ready_ring)
	r._ready_count -= 1
	return page_idx
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

	if !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		if owns_strings {
			free_cloned_pair(r, stored_scope, stored_name)
		}
		return
	}

	if r.cfg.string_fallback_copy && !already_cloned {
		stored_scope, stored_name = clone_string_pair(r, scope, name)
		owns_strings = true
	}

	sync.mutex_lock(&r._mu)

	if !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
		sync.mutex_unlock(&r._mu)
		if owns_strings {
			free_cloned_pair(r, stored_scope, stored_name)
		}
		return
	}

	if r._active_page_idx < 0 {
		if !acquire_free_page_locked(r) {
			sync.mutex_unlock(&r._mu)
			if owns_strings {
				free_cloned_pair(r, stored_scope, stored_name)
			}
			_ = intrinsics.atomic_add_explicit(&r._dropped_records, 1, .Relaxed)
			return
		}
	}

	page := &r._pages[r._active_page_idx]
	idx  := page.count
	page.count = idx + 1
	if idx + 1 >= BATCH_CAP {
		rotate_active_page_locked(r)
	}
	sync.mutex_unlock(&r._mu)

	stage_record_into_soa(
		page,
		idx,
		timestamp_ns,
		duration_ns,
		span_id,
		parent_id,
		thread_id,
		kind,
		status,
		stored_scope,
		stored_name,
		value_f64,
		attr_i64,
	)
	_ = intrinsics.atomic_add_explicit(&page.committed, 1, .Release)
}

@(private)
free_page_strings :: proc(r: ^Recorder, page: ^Staging_Page) {
	n := page.count
	if n <= 0 {
		return
	}
	CHUNK :: 128
	for base := 0; base < n; base += CHUNK {
		end := min(base + CHUNK, n)
		sync.mutex_lock(&r._alloc_mu)
		for i in base ..< end {
			sc := page.rows.scope[i]
			nm := page.rows.name[i]
			total := len(sc) + len(nm)
			if total > 0 {
				ptr := raw_data(sc) if len(sc) > 0 else raw_data(nm)
				_ = mem.free_with_size(ptr, total, r.allocator)
				page.rows.scope[i] = ""
				page.rows.name[i]  = ""
			}
		}
		sync.mutex_unlock(&r._alloc_mu)
	}
}

@(private)
rotate_active_page_locked :: proc(r: ^Recorder) {
	was_empty := r._ready_count == 0
	ready_ring_push_locked(r, r._active_page_idx)
	if r._free_top > 0 {
		next_idx := free_stack_pop_locked(r)
		r._pages[next_idx].count = 0
		r._active_page_idx = next_idx
	} else {
		r._active_page_idx = -1
	}
	if r.cfg.backend == .Sleipnir_Actor {
		if was_empty && !r._worker_paused && r._flusher_actor != nil {
			actor.send(r._sched, r._flusher_actor, Flusher_Msg{kind = .Drain_Ready})
		}
	} else if r._worker_sleeping {
		sync.cond_signal(&r._cv_work_ready)
	}
}

@(private)
acquire_free_page_locked :: proc(r: ^Recorder) -> bool {
	for {
		if r._shutdown || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed) {
			return false
		}
		if r._active_page_idx >= 0 {
			return true
		}
		if r._free_top > 0 {
			idx := free_stack_pop_locked(r)
			r._pages[idx].count = 0
			r._active_page_idx  = idx
			return true
		}
		if r.cfg.policy == .Drop {
			return false
		}
		// In .Sleipnir_Actor mode, ask the flusher actor via `actor.call` to drain
		// ready pages while holding an in-flight reference so `recorder_destroy` cannot UAF.
		if r.cfg.backend == .Sleipnir_Actor && !r._worker_paused && r._flusher_actor != nil &&
		   r._sched != nil && r._sched.started && intrinsics.atomic_load_explicit(&r._sched.running, .Acquire) {
			flusher := r._flusher_actor
			sched   := r._sched
			r._inflight_actor_calls += 1
			sync.mutex_unlock(&r._mu)

			_ = actor.call(sched, flusher, Flusher_Msg{kind = .Drain_Sync}, bool)

			sync.mutex_lock(&r._mu)
			r._inflight_actor_calls -= 1
			if r._inflight_actor_calls == 0 && r._shutdown {
				sync.cond_broadcast(&r._cv_actor_idle)
			}
			continue
		}
		r._waiting_producers += 1
		sync.cond_wait(&r._cv_page_free, &r._mu)
		r._waiting_producers -= 1
	}
}

// -----------------------------------------------------------------------------
// Background OdinArrow Serializer (Shared by Dedicated Thread & Sleipnir Actor)
// -----------------------------------------------------------------------------

@(private)
flusher_engine_init :: proc(eng: ^Flusher_Engine, r: ^Recorder) -> bool {
	eng.r        = r
	eng.file_seq = 0
	oa.buffer_pool_init(&eng.pool, r._page_alloc)

	prev_alloc := context.allocator
	context.allocator = oa.buffer_pool_allocator(&eng.pool)
	defer context.allocator = prev_alloc

	schema, schema_err := make_record_arrow_schema()
	if schema_err != nil {
		oa.buffer_pool_destroy(&eng.pool)
		return false
	}
	eng.schema = schema

	batches, b_err := make([dynamic]oa.Record_Batch, 0, r.cfg.batches_per_file, r._page_alloc)
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
	runtime.DEFAULT_TEMP_ALLOCATOR_TEMP_GUARD()

	r   := eng.r
	buf: [512]byte
	filename := fmt.bprintf(buf[:], "%s_%06d.arrow", r.cfg.file_prefix, eng.file_seq)
	written  := oa.ipc_write_file(filename, &eng.schema, eng.segment_batches[:])

	for &b in eng.segment_batches {
		oa.record_batch_free(&b)
	}
	clear(&eng.segment_batches)

	if written {
		eng.file_seq += 1
		_ = intrinsics.atomic_add(&r._flushed_segments, 1)
	}
}

@(private)
flusher_engine_drain_ready_pages :: proc(eng: ^Flusher_Engine, force_flush: bool) {
	if !eng.initialized {
		return
	}
	r := eng.r
	prev_alloc := context.allocator
	context.allocator = oa.buffer_pool_allocator(&eng.pool)
	defer context.allocator = prev_alloc

	sync.mutex_lock(&r._mu)
	flush_gen        := r._flush_target_gen
	bounded_by_flush := (force_flush || r._flush_requested) && !r._shutdown
	max_flush_pages  := r._ready_count
	sync.mutex_unlock(&r._mu)

	drained_pages := 0
	for {
		if bounded_by_flush && drained_pages >= max_flush_pages {
			break
		}
		sync.mutex_lock(&r._mu)
		if r._shutdown {
			bounded_by_flush = false
		} else if !bounded_by_flush && r._flush_requested {
			flush_gen        = r._flush_target_gen
			bounded_by_flush = true
			max_flush_pages  = drained_pages + r._ready_count
		}
		eff_force := force_flush || r._flush_requested || r._shutdown
		if (r._worker_paused && !eff_force) || r._ready_count == 0 {
			sync.mutex_unlock(&r._mu)
			break
		}
		page_idx := ready_ring_pop_locked(r)
		sync.mutex_unlock(&r._mu)
		drained_pages += 1

		page := &r._pages[page_idx]
		expected := page.count
		for intrinsics.atomic_load_explicit(&page.committed, .Acquire) < expected {
			intrinsics.cpu_relax()
		}
		if batch, ok := build_arrow_batch(&eng.schema, page); ok {
			if _, append_err := append(&eng.segment_batches, batch); append_err != nil {
				oa.record_batch_free(&batch)
			}
		}
		if r.cfg.string_fallback_copy {
			free_page_strings(r, page)
		}

		// Return the #soa staging page immediately to producers
		sync.mutex_lock(&r._mu)
		page.count = 0
		intrinsics.atomic_store_explicit(&page.committed, 0, .Relaxed)
		free_stack_push_locked(r, page_idx)
		if r._waiting_producers > 0 {
			sync.cond_broadcast(&r._cv_page_free)
		}
		sync.mutex_unlock(&r._mu)

		if len(eng.segment_batches) >= r.cfg.batches_per_file {
			flusher_engine_flush_segment(eng)
		}
	}

	sync.mutex_lock(&r._mu)
	should_force_flush := force_flush || bounded_by_flush || ((r._flush_requested || r._shutdown) && r._ready_count == 0)
	if flush_gen < r._flush_target_gen && r._ready_count == 0 {
		flush_gen = r._flush_target_gen
	}
	needs_actor_resignal := r.cfg.backend == .Sleipnir_Actor &&
		r._ready_count > 0 &&
		!r._worker_paused &&
		!r._shutdown &&
		r._flusher_actor != nil &&
		r._sched != nil
	resignal_sched   := r._sched
	resignal_flusher := r._flusher_actor
	sync.mutex_unlock(&r._mu)

	if should_force_flush && len(eng.segment_batches) > 0 {
		flusher_engine_flush_segment(eng)
	}

	if should_force_flush && flush_gen > 0 {
		sync.mutex_lock(&r._mu)
		if flush_gen > r._flush_completed_gen {
			r._flush_completed_gen = flush_gen
		}
		r._flush_requested = (r._flush_completed_gen < r._flush_target_gen)
		sync.cond_broadcast(&r._cv_flush_done)
		sync.mutex_unlock(&r._mu)
	}

	if needs_actor_resignal {
		actor.send(resignal_sched, resignal_flusher, Flusher_Msg{kind = .Drain_Ready})
	}
}

@(private)
flusher_actor_receive :: proc(ctx: ^actor.Context, state: ^Flusher_Engine, msg: ^Flusher_Msg) {
	switch msg.kind {
	case .Drain_Ready:
		flusher_engine_drain_ready_pages(state, false)
	case .Drain_Sync:
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
	for {
		sync.mutex_lock(&r._mu)
		for !r._shutdown && !r._flush_requested && (r._ready_count == 0 || r._worker_paused) {
			r._worker_sleeping = true
			sync.cond_wait(&r._cv_work_ready, &r._mu)
			r._worker_sleeping = false
		}
		is_shutting_down := r._shutdown
		is_flushing      := r._flush_requested
		sync.mutex_unlock(&r._mu)

		flusher_engine_drain_ready_pages(&r._eng, is_shutting_down || is_flushing)

		sync.mutex_lock(&r._mu)
		if r._flush_requested && r._ready_count == 0 && len(r._eng.segment_batches) == 0 {
			r._flush_completed_gen = r._flush_target_gen
			r._flush_requested     = false
			sync.cond_broadcast(&r._cv_flush_done)
		}
		done := r._shutdown && r._ready_count == 0
		sync.mutex_unlock(&r._mu)

		if done {
			break
		}
	}
}

@(private)
build_primitive_column_from_soa :: #force_inline proc(
	$T:      typeid,
	dt:      oa.DataType,
	src_ptr: [^]T,
	n:       int,
) -> (arr: oa.Array, ok: bool) {
	buf, err := oa.buffer_make_uninit(BATCH_CAP * size_of(T))
	if err != nil {
		return {}, false
	}
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
	}, true
}

@(private)
build_string_column_from_soa :: #force_inline proc(
	src_strings: [^]string,
	n:           int,
) -> (arr: oa.Array, ok: bool) {
	off_buf, off_err := oa.buffer_make_uninit((BATCH_CAP + 1) * size_of(i32))
	if off_err != nil {
		return {}, false
	}
	off_buf.size = (n + 1) * size_of(i32)
	offsets := cast([^]i32)off_buf.data
	offsets[0] = 0

	total_bytes := 0
	for i in 0 ..< n {
		s_len := len(src_strings[i])
		if s_len < 0 || total_bytes > int(max(i32)) - s_len {
			oa.buffer_free(&off_buf)
			return {}, false
		}
		total_bytes += s_len
		offsets[i + 1] = i32(total_bytes)
	}

	alloc_bytes := max((total_bytes + 4095) & ~int(4095), 4096)
	data_buf, data_err := oa.buffer_make_uninit(alloc_bytes)
	if data_err != nil {
		oa.buffer_free(&off_buf)
		return {}, false
	}
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
	}, true
}

@(private)
decode_string_from_buffers :: #force_inline proc(
	offsets: [^]i32,
	bytes:   [^]u8,
	row_idx: int,
) -> string {
	start := int(offsets[row_idx])
	end   := int(offsets[row_idx + 1])
	if end <= start || bytes == nil {
		return ""
	}
	return string(bytes[start:end])
}
