package heimdall_tests

import "base:intrinsics"
import "base:runtime"
import "core:fmt"
import "core:math"
import "core:mem"
import "core:os"
import "core:path/filepath"
import "core:strings"
import "core:testing"
import "core:thread"
import "core:time"
import "../heimdall"
import codegen "../heimdall/codegen"
import oa "../deps/OdinArrow/src"
import actor "../deps/sleipnir/actor"

// =============================================================================
// Layer 3: Protocol Driver (`tests/driver_heimdall.odin`)
//
// The ONLY test layer that imports `OdinArrow` or directly manipulates
// `heimdall.Recorder`, `.arrow` IPC files, schemas, and record batches.
// =============================================================================

MAX_LOADED_SEGMENTS :: 32

Loaded_Arrow_Segment :: struct {
	loaded:  bool,
	schema:  ^oa.Schema,
	batches: []oa.Record_Batch,
}

Suite_Ctx :: struct {
	t:                    ^testing.T,
	rec:                  heimdall.Recorder,
	rec_active:           bool,
	shared_sched:         actor.Scheduler,
	shared_sched_active:  bool,
	last_flushed_batches: u64,
	last_dropped_records: u64,
	file_prefix:          string,
	segments:             [MAX_LOADED_SEGMENTS]Loaded_Arrow_Segment,
}

@(private)
g_driver_prefix_seq: u64

driver_init :: proc(s: ^Suite_Ctx, t: ^testing.T) {
	s.t = t
	_ = os.make_directory(".tmp")
	seq := intrinsics.atomic_add(&g_driver_prefix_seq, 1)
	s.file_prefix = fmt.aprintf(".tmp/test_heimdall_atdd_%d", seq)
}

@(private)
driver_cleanup_segment_files :: proc(prefix: string) {
	if len(prefix) == 0 {
		return
	}
	buf: [512]byte
	for i in 0 ..< MAX_LOADED_SEGMENTS {
		filename := fmt.bprintf(buf[:], "%s_%06d.arrow", prefix, i)
		if !os.exists(filename) {
			if i > 0 {
				break
			}
			continue
		}
		_ = os.remove(filename)
	}
}

@(private)
driver_unload_segments :: proc(s: ^Suite_Ctx) {
	for i in 0 ..< MAX_LOADED_SEGMENTS {
		if s.segments[i].loaded {
			for &b in s.segments[i].batches {
				oa.record_batch_free(&b)
			}
			delete(s.segments[i].batches)
			oa.schema_free(s.segments[i].schema)
			free(s.segments[i].schema)
			s.segments[i] = {}
		}
	}
}

driver_destroy :: proc(s: ^Suite_Ctx) {
	if heimdall.default_recorder() == &s.rec {
		heimdall.set_default_recorder(nil)
	}
	cfg_buf: [512]byte
	cfg_prefix := ""
	if len(s.rec.cfg.file_prefix) > 0 && s.rec.cfg.file_prefix != s.file_prefix {
		cfg_prefix = fmt.bprintf(cfg_buf[:], "%s", s.rec.cfg.file_prefix)
	}
	if s.rec_active {
		driver_shutdown_recorder(s)
	}
	if s.shared_sched_active {
		actor.scheduler_stop_and_destroy(&s.shared_sched)
		s.shared_sched_active = false
	}
	driver_unload_segments(s)
	if len(cfg_prefix) > 0 {
		driver_cleanup_segment_files(cfg_prefix)
	}
	if len(s.file_prefix) > 0 {
		driver_cleanup_segment_files(s.file_prefix)
		delete(s.file_prefix)
		s.file_prefix = ""
	}
}

driver_shutdown_and_cleanup_prior_recorder :: proc(s: ^Suite_Ctx) {
	if !s.rec_active {
		return
	}
	old_prefix_buf: [512]byte
	old_prefix := ""
	if len(s.rec.cfg.file_prefix) > 0 && s.rec.cfg.file_prefix != s.file_prefix {
		old_prefix = fmt.bprintf(old_prefix_buf[:], "%s", s.rec.cfg.file_prefix)
	}
	driver_shutdown_recorder(s)
	if len(old_prefix) > 0 {
		driver_cleanup_segment_files(old_prefix)
	}
}

driver_configure_recorder :: proc(
	s:                        ^Suite_Ctx,
	batches_per_file:         int,
	ring_pages:               int,
	string_copy:              bool,
	policy:                   heimdall.Backpressure_Policy,
	backend:                  heimdall.Backend_Mode = .Dedicated_Thread,
	shared_scheduler_workers: int = 0,
	custom_prefix:            string = "",
) -> bool {
	if s.rec_active {
		driver_shutdown_and_cleanup_prior_recorder(s)
	}
	driver_unload_segments(s)
	var_sched: ^actor.Scheduler = nil
	if shared_scheduler_workers > 0 {
		if !s.shared_sched_active {
			if actor.scheduler_init_workers(&s.shared_sched, shared_scheduler_workers, true) != .None {
				return false
			}
			actor.scheduler_start(&s.shared_sched)
			s.shared_sched_active = true
		}
		var_sched = &s.shared_sched
	}
	prefix := custom_prefix if len(custom_prefix) > 0 else s.file_prefix
	ephemeral_buf: [512]byte
	ephemeral_prefix := fmt.bprintf(ephemeral_buf[:], "%s", prefix)
	err := heimdall.recorder_init(&s.rec, heimdall.Config{
		file_prefix          = ephemeral_prefix,
		batches_per_file     = batches_per_file,
		ring_page_count      = ring_pages,
		string_fallback_copy = string_copy,
		policy               = policy,
		backend              = backend,
		scheduler            = var_sched,
	})
	// Poison the caller's ephemeral buffer immediately after `recorder_init`
	// to prove `recorder_init` cloned `file_prefix` into `_owned_file_prefix`.
	mem.set(raw_data(ephemeral_buf[:]), 'X', len(ephemeral_buf))
	if err != nil {
		return false
	}
	s.rec_active = true
	return true
}

driver_configure_zero_config_recorder :: proc(s: ^Suite_Ctx) -> bool {
	if s.rec_active {
		driver_shutdown_and_cleanup_prior_recorder(s)
	}
	driver_unload_segments(s)
	err := heimdall.recorder_init(&s.rec)
	if err != nil {
		return false
	}
	s.rec_active = true
	return true
}

driver_set_recorder_enabled :: proc(s: ^Suite_Ctx, enabled: bool) {
	heimdall.recorder_set_enabled(&s.rec, enabled)
}

driver_set_default_recorder :: proc(s: ^Suite_Ctx) -> bool {
	heimdall.set_default_recorder(&s.rec)
	return heimdall.default_recorder() == &s.rec
}

// Simulated Library A: HTTP Router
driver_simulate_http_request :: proc(rec: ^heimdall.Recorder, payload_size: int) {
	heimdall.scoped_span(rec, "http_lib", "handle_request", i64(payload_size))
	heimdall.counter_add(rec, "http_lib", "requests_total", 1.0, i64(payload_size))
	driver_simulate_db_query(rec, 42)
}

// Simulated Library B: Database Driver (called inside `handle_request`)
driver_simulate_db_query :: proc(rec: ^heimdall.Recorder, query_id: i64) {
	heimdall.scoped_span(rec, "db_driver", "execute_query", query_id)
	heimdall.gauge_set(rec, "db_driver", "pool_connections_active", 8.0, query_id)
}

driver_simulate_failing_child_query :: proc(rec: ^heimdall.Recorder, query_id: i64, err_code: i64) {
	heimdall.scoped_span(rec, "db_driver", "query_primary", query_id)
	heimdall.span_set_error(rec, err_code)
}

driver_simulate_fallback_request :: proc(
	rec:              ^heimdall.Recorder,
	initial_http_tag: i64,
	child_err_code:   i64,
	propagate_err:    bool,
	parent_err_code:  i64,
) {
	heimdall.scoped_span(rec, "http_lib", "handle_request", initial_http_tag)
	driver_simulate_failing_child_query(rec, 1, child_err_code)
	driver_simulate_db_query(rec, 2)
	if propagate_err {
		heimdall.span_set_error(rec, parent_err_code)
	}
}

@(private)
Producer_Worker_Args :: struct {
	rec:        ^heimdall.Recorder,
	iterations: int,
	worker_idx: int,
}

@(private)
producer_worker_proc :: proc(args: ^Producer_Worker_Args) {
	for i in 0 ..< args.iterations {
		driver_simulate_http_request(args.rec, args.worker_idx * 1000 + i)
	}
}

driver_run_concurrent_producers :: proc(
	s:                     ^Suite_Ctx,
	thread_count:          int,
	iterations_per_thread: int,
) {
	threads := make([]^thread.Thread, thread_count, context.temp_allocator)
	args    := make([]Producer_Worker_Args, thread_count, context.temp_allocator)

	for i in 0 ..< thread_count {
		args[i] = Producer_Worker_Args{
			rec        = &s.rec,
			iterations = iterations_per_thread,
			worker_idx = i + 1,
		}
		threads[i] = thread.create_and_start_with_poly_data(&args[i], producer_worker_proc)
	}

	for t_handle in threads {
		if t_handle != nil {
			thread.join(t_handle)
			thread.destroy(t_handle)
		}
	}
}

@(private)
Actor_Producer_State :: struct {
	rec:       ^heimdall.Recorder,
	actor_idx: int,
}

@(private)
Actor_Producer_Msg :: struct {
	seq: int,
}

@(private)
actor_producer_receive :: proc(
	ctx:   ^actor.Context,
	state: ^Actor_Producer_State,
	msg:   ^Actor_Producer_Msg,
) {
	driver_simulate_http_request(state.rec, state.actor_idx * 1000 + msg.seq)
	actor.complete_token(ctx, 1)
}

driver_run_sleipnir_actor_producers :: proc(
	s:                  ^Suite_Ctx,
	actor_count:        int,
	messages_per_actor: int,
) -> bool {
	if !s.shared_sched_active || actor_count <= 0 || messages_per_actor <= 0 {
		return false
	}
	actors := make([]^actor.Actor(Actor_Producer_State, Actor_Producer_Msg), actor_count, context.temp_allocator)
	defer {
		for a in actors {
			if a != nil {
				actor.destroy(a)
			}
		}
	}
	for i in 0 ..< actor_count {
		a, err := actor.spawn(
			&s.shared_sched,
			Actor_Producer_State{rec = &s.rec, actor_idx = i + 1},
			actor_producer_receive,
			u32(i + 1),
		)
		if err != .None || a == nil {
			return false
		}
		actors[i] = a
	}

	total_msgs := i64(actor_count * messages_per_actor)
	intrinsics.atomic_store_explicit(&s.shared_sched.pending_tokens, total_msgs, .Release)

	for m in 0 ..< messages_per_actor {
		for i in 0 ..< actor_count {
			actor.send(&s.shared_sched, actors[i], Actor_Producer_Msg{seq = m}, i)
		}
	}

	actor.scheduler_wait_tokens(&s.shared_sched)
	actor.scheduler_wait_idle(&s.shared_sched)
	return true
}

driver_emit_saturation_burst :: proc(s: ^Suite_Ctx, total_events: int) {
	heimdall.recorder_set_worker_paused(&s.rec, true)
	for i in 0 ..< total_events {
		heimdall.event_emit(&s.rec, "burst_lib", "fast_tick", i64(i), .Ok)
	}
	heimdall.recorder_set_worker_paused(&s.rec, false)
}

driver_emit_events_with_live_temp_canary :: proc(
	s:            ^Suite_Ctx,
	total_events: int,
) -> (
	canary_preserved: bool,
) {
	// Allocate a 4 KB canary slice on `context.temp_allocator` BEFORE emitting/flushing/destroying
	canary := make([]u64, 512, context.temp_allocator)
	for i in 0 ..< len(canary) {
		canary[i] = 0xDEAD_BEEF_CAFE_0000 ~ u64(i * 131)
	}

	for i in 0 ..< total_events {
		heimdall.event_emit(&s.rec, "temp_guard_lib", "backpressure_event", i64(i), .Ok)
	}
	heimdall.recorder_flush(&s.rec)
	driver_shutdown_recorder(s)

	canary_preserved = true
	for i in 0 ..< len(canary) {
		if canary[i] != (0xDEAD_BEEF_CAFE_0000 ~ u64(i * 131)) {
			canary_preserved = false
			break
		}
	}
	return
}

driver_cycle_sleipnir_actor_lifecycle_under_load :: proc(
	s:                  ^Suite_Ctx,
	cycles:             int,
	events_per_cycle:   int,
) -> (
	completed_cycles:      int,
	total_flushed_batches: u64,
) {
	if !s.shared_sched_active {
		if actor.scheduler_init_workers(&s.shared_sched, 4, true) != .None {
			return 0, 0
		}
		actor.scheduler_start(&s.shared_sched)
		s.shared_sched_active = true
	}

	// Verify that with `&s.shared_sched` set as the ambient TLS scheduler on this thread,
	// initializing a `.Sleipnir_Actor` recorder with `scheduler = nil` creates its own
	// dedicated `_owned_sched = true` scheduler without hijacking `&s.shared_sched`
	// or clobbering `actor.current_scheduler()`.
	actor.use_scheduler(&s.shared_sched)
	{
		iso_buf: [512]byte
		iso_prefix := fmt.bprintf(iso_buf[:], "%s_ambient_iso", s.file_prefix)
		defer driver_cleanup_segment_files(iso_prefix)

		iso_rec: heimdall.Recorder
		if heimdall.recorder_init(&iso_rec, heimdall.Config{
			file_prefix      = iso_prefix,
			batches_per_file = 1,
			ring_page_count  = 2,
			backend          = .Sleipnir_Actor,
			scheduler        = nil,
		}) != nil {
			return 0, 0
		}
		ambient_preserved := (actor.current_scheduler() == &s.shared_sched) &&
			iso_rec._owned_sched &&
			iso_rec._sched != nil &&
			iso_rec._sched != &s.shared_sched
		heimdall.recorder_destroy(&iso_rec)
		if !ambient_preserved {
			return 0, 0
		}
	}

	buf: [512]byte
	for c in 0 ..< cycles {
		cycle_prefix := fmt.bprintf(buf[:], "%s_cycle_%d", s.file_prefix, c)
		rec: heimdall.Recorder
		if heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = cycle_prefix,
			batches_per_file = 1,
			ring_page_count  = 2,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
			scheduler        = &s.shared_sched,
		}) != nil {
			break
		}

		// Spawn a background producer thread that saturates the 2-page (8,192-row) ring
		// with `events_per_cycle` (10,000) events, triggering `actor.call(..., .Drain_Sync)`
		// inside `acquire_free_page_locked`, while this thread races `recorder_flush` and
		// `recorder_destroy` against the in-flight `.Drain_Sync` calls.
		burst_args := Blocking_Burst_Args{
			rec          = &rec,
			total_events = events_per_cycle,
		}
		prod_thread := thread.create_and_start_with_poly_data(&burst_args, blocking_burst_worker_proc)

		for i in 0 ..< 2048 {
			heimdall.event_emit(&rec, "actor_lifecycle", "rapid_turn", i64(i), .Ok)
		}
		heimdall.recorder_flush(&rec)
		heimdall.recorder_destroy(&rec)

		if prod_thread != nil {
			thread.join(prod_thread)
			thread.destroy(prod_thread)
		}

		flushed, _ := heimdall.recorder_diagnostics(&rec)
		if flushed > 0 {
			total_flushed_batches += flushed
			completed_cycles += 1
		}

		driver_cleanup_segment_files(cycle_prefix)
	}
	return
}

driver_emit_dynamic_string_points :: proc(s: ^Suite_Ctx, count: int, use_default_rec: bool) {
	target := heimdall.default_recorder() if use_default_rec else &s.rec
	for i in 0 ..< count {
		dyn_scope := fmt.aprintf("worker_%d", i)
		dyn_name  := fmt.aprintf("op_%d", i)
		heimdall.histogram_record(target, dyn_scope, dyn_name, f64(i) * 1.5, i64(i))
		heimdall.event_emit(target, dyn_scope, dyn_name, i64(i), .Ok)
		// Immediately free caller strings to verify `string_fallback_copy` cloned them safely
		delete(dyn_scope)
		delete(dyn_name)
	}
}

@(private)
emit_single_dynamic_scoped_span :: proc(rec: ^heimdall.Recorder, idx: int) {
	dyn_scope := fmt.aprintf("span_scope_%d", idx)
	dyn_name  := fmt.aprintf("span_op_%d", idx)
	heimdall.scoped_span(rec, dyn_scope, dyn_name, i64(idx))
	// Free dynamic strings BEFORE `scoped_span` exits at the end of this procedure!
	delete(dyn_scope)
	delete(dyn_name)
}

driver_emit_dynamic_scoped_spans :: proc(s: ^Suite_Ctx, count: int, pause_worker: bool) {
	if pause_worker {
		heimdall.recorder_set_worker_paused(&s.rec, true)
	}
	for i in 0 ..< count {
		emit_single_dynamic_scoped_span(&s.rec, i)
	}
	if pause_worker {
		heimdall.recorder_set_worker_paused(&s.rec, false)
	}
}

driver_test_oom_recorder_init :: proc(
	s:               ^Suite_Ctx,
	arena_bytes:     int,
	requested_pages: int,
) -> (
	failed_with_oom:     bool,
	clean_after_destroy: bool,
) {
	_ = s
	backing := make([]byte, arena_bytes)
	defer delete(backing)

	arena: mem.Arena
	mem.arena_init(&arena, backing)
	arena_alloc := mem.arena_allocator(&arena)

	temp_rec: heimdall.Recorder
	err := heimdall.recorder_init(
		&temp_rec,
		heimdall.Config{
			file_prefix     = ".tmp/oom_never_created",
			ring_page_count = requested_pages,
		},
		arena_alloc,
	)
	failed_with_oom = (err == .Out_Of_Memory)

	// Calling `recorder_destroy` on a failed `recorder_init` must be a safe no-op
	heimdall.recorder_destroy(&temp_rec)
	flushed, dropped := heimdall.recorder_diagnostics(&temp_rec)
	clean_after_destroy = (!heimdall.recorder_is_enabled(&temp_rec) &&
		!heimdall.recorder_is_saturated(&temp_rec) &&
		flushed == 0 &&
		dropped == 0)
	return
}

@(private)
Blocking_Burst_Args :: struct {
	rec:          ^heimdall.Recorder,
	total_events: int,
}

@(private)
blocking_burst_worker_proc :: proc(args: ^Blocking_Burst_Args) {
	for i in 0 ..< args.total_events {
		heimdall.event_emit(args.rec, "block_lib", "guaranteed_delivery", i64(i), .Ok)
	}
}

driver_run_blocking_producer_with_delayed_unpause :: proc(
	s:            ^Suite_Ctx,
	total_events: int,
) -> (
	blocked_while_paused: bool,
) {
	heimdall.recorder_set_worker_paused(&s.rec, true)

	args := Blocking_Burst_Args{
		rec          = &s.rec,
		total_events = total_events,
	}
	producer := thread.create_and_start_with_poly_data(&args, blocking_burst_worker_proc)

	// Wait until the producer fills all ring pages and blocks on `_cv_page_free`
	for _ in 0 ..< 500 {
		if heimdall.recorder_is_saturated(&s.rec) && heimdall.recorder_waiting_producers(&s.rec) > 0 {
			blocked_while_paused = true
			break
		}
		time.sleep(time.Millisecond)
	}

	// Unpause the background worker so it drains pages and wakes the blocked producer
	heimdall.recorder_set_worker_paused(&s.rec, false)

	if producer != nil {
		thread.join(producer)
		thread.destroy(producer)
	}
	return
}

@(private)
driver_level3_leaf :: proc(rec: ^heimdall.Recorder, scope, name: string, tag: i64) {
	heimdall.scoped_span(rec, scope, name, tag)
}

@(private)
driver_level2_auth :: proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "auth", "verify", 201)
}

@(private)
driver_level2_orders :: proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "orders", "create", 202)
	driver_level3_leaf(rec, "cache", "lookup", 301)
	driver_level3_leaf(rec, "db", "insert", 302)
}

@(private)
driver_level1_gateway :: proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "gateway", "ingress", 100)
	driver_level2_auth(rec)
	driver_level2_orders(rec)
}

driver_simulate_three_level_and_sibling_call_tree :: proc(s: ^Suite_Ctx) {
	driver_level1_gateway(&s.rec)
	// Emit an event after the entire 3-level call tree unwinds to verify `tls_active_span_id == 0`
	heimdall.event_emit(&s.rec, "gateway", "after_response", 999, .Ok)
}

driver_record_custom_points :: proc(
	s:             ^Suite_Ctx,
	scope:         string,
	metric_name:   string,
	counter_delta: f64,
	gauge_val:     f64,
	hist_sample:   f64,
	event_attr:    i64,
	event_status:  heimdall.Status,
) {
	heimdall.scoped_span(&s.rec, scope, metric_name, event_attr)
	heimdall.counter_add(&s.rec, scope, metric_name, counter_delta, event_attr)
	heimdall.gauge_set(&s.rec, scope, metric_name, gauge_val, event_attr)
	heimdall.histogram_record(&s.rec, scope, metric_name, hist_sample, event_attr)
	heimdall.event_emit(&s.rec, scope, metric_name, event_attr, event_status)
}

driver_flush_recorder :: proc(s: ^Suite_Ctx) -> (temp_canary_intact: bool) {
	canary := fmt.tprintf("caller_temp_canary_%s", s.file_prefix)
	heimdall.recorder_flush(nil) // Also verify nil flush is a safe no-op
	heimdall.recorder_flush(&s.rec)
	s.last_flushed_batches, s.last_dropped_records = heimdall.recorder_diagnostics(&s.rec)
	// Verify synchronous or actor-driven flush did NOT wipe caller's `context.temp_allocator`
	expected_canary := fmt.tprintf("caller_temp_canary_%s", s.file_prefix)
	temp_canary_intact = (canary == expected_canary)
	return
}

driver_shutdown_recorder :: proc(s: ^Suite_Ctx) {
	if !s.rec_active {
		return
	}
	if heimdall.default_recorder() == &s.rec {
		heimdall.set_default_recorder(nil)
	}
	heimdall.recorder_destroy(&s.rec)
	s.last_flushed_batches, s.last_dropped_records = heimdall.recorder_diagnostics(&s.rec)
	s.rec_active = false
}

driver_segment_file_exists :: proc(s: ^Suite_Ctx, segment_idx: int) -> bool {
	buf: [512]byte
	filename := fmt.bprintf(buf[:], "%s_%06d.arrow", s.file_prefix, segment_idx)
	return os.exists(filename)
}

driver_load_segment :: proc(s: ^Suite_Ctx, segment_idx: int) -> (seg: ^Loaded_Arrow_Segment, ok: bool) {
	if segment_idx < 0 || segment_idx >= MAX_LOADED_SEGMENTS {
		return nil, false
	}
	if s.segments[segment_idx].loaded {
		return &s.segments[segment_idx], true
	}
	buf: [512]byte
	filename := fmt.bprintf(buf[:], "%s_%06d.arrow", s.file_prefix, segment_idx)
	sc, batches, read_ok := oa.ipc_read_file(filename)
	if !read_ok {
		return nil, false
	}
	s.segments[segment_idx] = Loaded_Arrow_Segment{
		loaded  = true,
		schema  = sc,
		batches = batches,
	}
	return &s.segments[segment_idx], true
}

driver_total_rows_across_segments :: proc(s: ^Suite_Ctx) -> int {
	total := 0
	for seg_idx in 0 ..< MAX_LOADED_SEGMENTS {
		if !driver_segment_file_exists(s, seg_idx) {
			break
		}
		seg, ok := driver_load_segment(s, seg_idx)
		if !ok {
			break
		}
		for b in seg.batches {
			total += b.length
		}
	}
	return total
}

driver_read_row :: proc(
	s:           ^Suite_Ctx,
	segment_idx: int,
	batch_idx:   int,
	row_idx:     int,
) -> (rec: heimdall.Record, ok: bool) {
	seg, seg_ok := driver_load_segment(s, segment_idx)
	if !seg_ok || batch_idx < 0 || batch_idx >= len(seg.batches) {
		return {}, false
	}
	b := &seg.batches[batch_idx]
	if row_idx < 0 || row_idx >= b.length {
		return {}, false
	}
	reader := heimdall.record_batch_reader_resolve(b) or_return
	return heimdall.record_batch_reader_decode_row(&reader, row_idx), true
}

driver_read_all_rows :: proc(
	s:         ^Suite_Ctx,
	allocator := context.temp_allocator,
) -> []heimdall.Record {
	total := driver_total_rows_across_segments(s)
	rows  := make([dynamic]heimdall.Record, 0, total, allocator)
	for seg_idx in 0 ..< MAX_LOADED_SEGMENTS {
		if !driver_segment_file_exists(s, seg_idx) {
			break
		}
		seg, ok := driver_load_segment(s, seg_idx)
		if !ok {
			break
		}
		for &b in seg.batches {
			reader, cols_ok := heimdall.record_batch_reader_resolve(&b)
			if !cols_ok {
				continue
			}
			for r_idx in 0 ..< b.length {
				append(&rows, heimdall.record_batch_reader_decode_row(&reader, r_idx))
			}
		}
	}
	return rows[:]
}

Span_Meta :: struct {
	thread_id: u32,
	scope:     string,
	name:      string,
}

driver_verify_multithreaded_span_tree_integrity :: proc(
	s:                   ^Suite_Ctx,
	expected_total_rows: int,
) -> (
	total_rows:          int,
	unique_spans:        int,
	root_spans:          int,
	cross_thread_leaks:  int,
	missing_parents:     int,
	distinct_threads:    int,
) {
	rows       := driver_read_all_rows(s, context.temp_allocator)
	span_map   := make(map[u64]Span_Meta, expected_total_rows, context.temp_allocator)
	thread_set := make(map[u32]bool, 16, context.temp_allocator)

	// Pass 1: Index all spans by `span_id` and verify global uniqueness
	for rec in rows {
		total_rows += 1
		thread_set[rec.thread_id] = true
		if rec.kind == .Span && rec.span_id > 0 {
			if !(rec.span_id in span_map) {
				unique_spans += 1
				span_map[rec.span_id] = Span_Meta{
					thread_id = rec.thread_id,
					scope     = rec.scope,
					name      = rec.name,
				}
			}
			if rec.parent_id == 0 {
				root_spans += 1
			}
		}
	}

	// Pass 2: Verify every child span and metric with `parent_id > 0` points to a span on the SAME `thread_id`
	for rec in rows {
		if rec.parent_id == 0 {
			continue
		}
		parent_meta, found := span_map[rec.parent_id]
		if !found {
			missing_parents += 1
		} else if parent_meta.thread_id != rec.thread_id {
			cross_thread_leaks += 1
		}
	}

	distinct_threads = len(thread_set)
	return
}

driver_set_worker_paused :: proc(s: ^Suite_Ctx, paused: bool) {
	heimdall.recorder_set_worker_paused(&s.rec, paused)
}

driver_simulate_mutated_guard_and_negative_gauge :: proc(
	s:                  ^Suite_Ctx,
	initial_attr:       i64,
	mutated_attr:       i64,
	negative_gauge_val: f64,
) {
	guard := heimdall.span_begin(&s.rec, "guard_lib", "mutated_attr_op", initial_attr)
	// Mutate `guard.attr_i64` directly on the returned `Span_Guard` before `span_end` runs
	guard.attr_i64 = mutated_attr
	defer heimdall.span_end(guard)
	heimdall.gauge_set(&s.rec, "guard_lib", "temperature_celsius", negative_gauge_val, mutated_attr)
}

@(private)
Coop_Callee_State :: struct {
	rec: ^heimdall.Recorder,
}

@(private)
Coop_Caller_State :: struct {
	rec:    ^heimdall.Recorder,
	sched:  ^actor.Scheduler,
	callee: ^actor.Actor(Coop_Callee_State, int),
}

@(private)
coop_callee_receive :: proc(ctx: ^actor.Context, state: ^Coop_Callee_State, msg: ^int) {
	// Executed cooperatively on the SAME OS worker thread while `coop_caller_receive`
	// is waiting inside `actor.call` with an open `scoped_span`!
	heimdall.event_emit(state.rec, "actor_callee", "callee_root_evt", 200, .Ok)
	{
		heimdall.scoped_span(state.rec, "actor_callee", "callee_rpc", 201)
		heimdall.event_emit(state.rec, "actor_callee", "callee_child_evt", 202, .Ok)
	}
	actor.reply(ctx, msg^ + 1)
}

@(private)
coop_caller_receive :: proc(ctx: ^actor.Context, state: ^Coop_Caller_State, msg: ^int) {
	heimdall.scoped_span(state.rec, "actor_caller", "caller_rpc", 100)
	heimdall.event_emit(state.rec, "actor_caller", "before_rpc", 101, .Ok)
	res := actor.call(state.sched, state.callee, msg^, int)
	heimdall.event_emit(state.rec, "actor_caller", "after_rpc", i64(res), .Ok)
	actor.reply(ctx, res)
}

driver_verify_multi_recorder_same_thread_isolation :: proc(
	s: ^Suite_Ctx,
) -> (
	rec_a_rows:           int,
	rec_b_rows:           int,
	cross_recorder_leaks: int,
) {
	buf_a: [512]byte
	buf_b: [512]byte
	prefix_a := fmt.bprintf(buf_a[:], "%s_iso_a", s.file_prefix)
	prefix_b := fmt.bprintf(buf_b[:], "%s_iso_b", s.file_prefix)
	defer {
		driver_cleanup_segment_files(prefix_a)
		driver_cleanup_segment_files(prefix_b)
	}

	rec_a: heimdall.Recorder
	rec_b: heimdall.Recorder
	if heimdall.recorder_init(&rec_a, heimdall.Config{
		file_prefix      = prefix_a,
		batches_per_file = 1,
		ring_page_count  = 2,
	}) != nil {
		return 0, 0, 1
	}
	if heimdall.recorder_init(&rec_b, heimdall.Config{
		file_prefix      = prefix_b,
		batches_per_file = 1,
		ring_page_count  = 2,
	}) != nil {
		heimdall.recorder_destroy(&rec_a)
		return 0, 0, 1
	}

	// Open an outer span on `rec_b` (`span_id == 1` on `rec_b`)
	g_b := heimdall.span_begin(&rec_b, "audit_lib", "audit_tx", 10)
	// Emit a child event on `rec_b` (must have `parent_id == 1`)
	heimdall.event_emit(&rec_b, "audit_lib", "audit_step_1", 11, .Ok)

	// While `g_b` is active on `rec_b`, open a ROOT span on `rec_a` (`span_id == 1, parent_id == 0` on `rec_a`)
	g_a_root := heimdall.span_begin(&rec_a, "app_lib", "http_req", 20)
	// And a child span on `rec_a` (`span_id == 2, parent_id == 1` on `rec_a`)
	g_a_child := heimdall.span_begin(&rec_a, "app_lib", "db_query", 21)
	heimdall.span_end(g_a_child)
	heimdall.span_end(g_a_root)

	// Emit a root event on `rec_a` AFTER `g_a_root` ended while `g_b` is still active on `rec_b` (`parent_id == 0`!)
	heimdall.event_emit(&rec_a, "app_lib", "after_http", 22, .Ok)

	// Emit a second child event on `rec_b` (must STILL have `parent_id == 1` on `rec_b`)
	heimdall.event_emit(&rec_b, "audit_lib", "audit_step_2", 12, .Ok)
	heimdall.span_end(g_b)

	heimdall.recorder_destroy(&rec_a)
	heimdall.recorder_destroy(&rec_b)

	file_a_buf: [512]byte
	file_b_buf: [512]byte
	file_a := fmt.bprintf(file_a_buf[:], "%s_000000.arrow", prefix_a)
	file_b := fmt.bprintf(file_b_buf[:], "%s_000000.arrow", prefix_b)

	sc_a, batches_a, ok_a := oa.ipc_read_file(file_a)
	if !ok_a {
		return 0, 0, 1
	}
	defer {
		for &b in batches_a {
			oa.record_batch_free(&b)
		}
		delete(batches_a)
		oa.schema_free(sc_a)
		free(sc_a)
	}

	sc_b, batches_b, ok_b := oa.ipc_read_file(file_b)
	if !ok_b {
		return 0, 0, 1
	}
	defer {
		for &b in batches_b {
			oa.record_batch_free(&b)
		}
		delete(batches_b)
		oa.schema_free(sc_b)
		free(sc_b)
	}

	if len(batches_a) == 1 {
		cols_a, c_ok := heimdall.record_batch_reader_resolve(&batches_a[0])
		if c_ok {
			rec_a_rows = batches_a[0].length
			for idx in 0 ..< batches_a[0].length {
				row := heimdall.record_batch_reader_decode_row(&cols_a, idx)
				switch row.name {
				case "db_query":
					if row.span_id != 2 || row.parent_id != 1 {
						cross_recorder_leaks += 1
					}
				case "http_req":
					if row.span_id != 1 || row.parent_id != 0 {
						cross_recorder_leaks += 1
					}
				case "after_http":
					if row.parent_id != 0 {
						cross_recorder_leaks += 1
					}
				}
			}
		}
	}

	if len(batches_b) == 1 {
		cols_b, c_ok := heimdall.record_batch_reader_resolve(&batches_b[0])
		if c_ok {
			rec_b_rows = batches_b[0].length
			for idx in 0 ..< batches_b[0].length {
				row := heimdall.record_batch_reader_decode_row(&cols_b, idx)
				switch row.name {
				case "audit_step_1", "audit_step_2":
					if row.parent_id != 1 {
						cross_recorder_leaks += 1
					}
				case "audit_tx":
					if row.span_id != 1 || row.parent_id != 0 {
						cross_recorder_leaks += 1
					}
				}
			}
		}
	}

	// Also verify that two Sleipnir actors stepped cooperatively via `actor.call`
	// on the SAME 1-worker OS thread do not leak `parent_id` across actor turns.
	buf_coop: [512]byte
	prefix_coop := fmt.bprintf(buf_coop[:], "%s_coop_actor", s.file_prefix)
	defer driver_cleanup_segment_files(prefix_coop)

	coop_sched: actor.Scheduler
	if actor.scheduler_init_workers(&coop_sched, 1, true) != .None {
		cross_recorder_leaks += 1
		return
	}
	actor.scheduler_start(&coop_sched)
	defer actor.scheduler_stop_and_destroy(&coop_sched)

	rec_coop: heimdall.Recorder
	if heimdall.recorder_init(&rec_coop, heimdall.Config{
		file_prefix      = prefix_coop,
		batches_per_file = 1,
		ring_page_count  = 2,
	}) != nil {
		cross_recorder_leaks += 1
		return
	}

	callee, c_err := actor.spawn(&coop_sched, Coop_Callee_State{rec = &rec_coop}, coop_callee_receive, 1)
	caller, r_err := actor.spawn(&coop_sched, Coop_Caller_State{rec = &rec_coop, sched = &coop_sched, callee = callee}, coop_caller_receive, 2)
	if c_err != .None || r_err != .None || callee == nil || caller == nil {
		heimdall.recorder_destroy(&rec_coop)
		cross_recorder_leaks += 1
		return
	}

	rpc_res := actor.call(&coop_sched, caller, 41, int)
	if rpc_res != 42 {
		cross_recorder_leaks += 1
	}
	actor.destroy(caller)
	actor.destroy(callee)
	heimdall.recorder_destroy(&rec_coop)

	file_coop_buf: [512]byte
	file_coop := fmt.bprintf(file_coop_buf[:], "%s_000000.arrow", prefix_coop)
	sc_c, batches_c, ok_c := oa.ipc_read_file(file_coop)
	if !ok_c || len(batches_c) != 1 {
		cross_recorder_leaks += 1
		return
	}
	defer {
		for &b in batches_c {
			oa.record_batch_free(&b)
		}
		delete(batches_c)
		oa.schema_free(sc_c)
		free(sc_c)
	}

	cols_c, ok_cols_c := heimdall.record_batch_reader_resolve(&batches_c[0])
	if !ok_cols_c || batches_c[0].length != 6 {
		cross_recorder_leaks += 1
		return
	}

	caller_span_id: u64 = 0
	callee_span_id: u64 = 0
	for idx in 0 ..< batches_c[0].length {
		row := heimdall.record_batch_reader_decode_row(&cols_c, idx)
		switch row.name {
		case "caller_rpc":
			caller_span_id = row.span_id
			if row.parent_id != 0 {
				cross_recorder_leaks += 1
			}
		case "callee_rpc":
			callee_span_id = row.span_id
			if row.parent_id != 0 {
				cross_recorder_leaks += 1
			}
		}
	}
	if caller_span_id == 0 || callee_span_id == 0 || caller_span_id == callee_span_id {
		cross_recorder_leaks += 1
	}
	for idx in 0 ..< batches_c[0].length {
		row := heimdall.record_batch_reader_decode_row(&cols_c, idx)
		switch row.name {
		case "before_rpc", "after_rpc":
			if row.parent_id != caller_span_id {
				cross_recorder_leaks += 1
			}
		case "callee_root_evt":
			if row.parent_id != 0 {
				cross_recorder_leaks += 1
			}
		case "callee_child_evt":
			if row.parent_id != callee_span_id {
				cross_recorder_leaks += 1
			}
		}
	}

	// Verify `Record_Batch_Reader` direct pointer caching with non-zero `Array.offset`
	// (sliced columns via `oa.array_slice(col, 1, 5)`).
	sliced_cols := make([]oa.Array, len(batches_c[0].columns), context.temp_allocator)
	for c_idx in 0 ..< len(batches_c[0].columns) {
		sliced_cols[c_idx] = oa.array_slice(batches_c[0].columns[c_idx], 1, 5)
	}
	sliced_batch := oa.Record_Batch{
		schema  = batches_c[0].schema,
		columns = sliced_cols,
		length  = 4,
	}
	sliced_reader, ok_sliced := heimdall.record_batch_reader_resolve(&sliced_batch)
	if !ok_sliced {
		cross_recorder_leaks += 1
	} else {
		for s_idx in 0 ..< sliced_batch.length {
			orig_row   := heimdall.record_batch_reader_decode_row(&cols_c, s_idx + 1)
			sliced_row := heimdall.record_batch_reader_decode_row(&sliced_reader, s_idx)
			if sliced_row != orig_row {
				cross_recorder_leaks += 1
			}
		}
	}

	// Verify 64-ID TLS span batching across 2 interleaved recorders (`70 > SPAN_ID_BATCH_SIZE :: 64`)
	// AND verify that re-initializing `rec_coop` at the exact same memory address resets `span_id` to 1.
	{
		buf_reinit: [512]byte
		prefix_reinit := fmt.bprintf(buf_reinit[:], "%s_reinit_span", s.file_prefix)
		defer driver_cleanup_segment_files(prefix_reinit)

		if heimdall.recorder_init(&rec_coop, heimdall.Config{
			file_prefix      = prefix_reinit,
			batches_per_file = 1,
			ring_page_count  = 2,
		}) != nil {
			cross_recorder_leaks += 1
			return
		}
		for s_iter in 1 ..= 70 {
			g := heimdall.span_begin(&rec_coop, "batch_lib", "batch_span", i64(s_iter))
			if g.span_id != u64(s_iter) {
				cross_recorder_leaks += 1
			}
			heimdall.span_end(g)
		}
		heimdall.recorder_destroy(&rec_coop)
		driver_cleanup_segment_files(prefix_reinit)

		// Re-initialize at the exact same `&rec_coop` address: `_instance_id` must invalidate
		// the previous `tls_span_id_cache` entry so the first span starts back at `span_id == 1`.
		if heimdall.recorder_init(&rec_coop, heimdall.Config{
			file_prefix      = prefix_reinit,
			batches_per_file = 1,
			ring_page_count  = 2,
		}) != nil {
			cross_recorder_leaks += 1
			return
		}
		g_first := heimdall.span_begin(&rec_coop, "batch_lib", "reinit_first", 1)
		if g_first.span_id != 1 {
			cross_recorder_leaks += 1
		}
		heimdall.span_end(g_first)
		heimdall.recorder_destroy(&rec_coop)
	}

	return
}

driver_f64_approx_eq :: proc(a, b: f64, eps: f64 = 1e-6) -> bool {
	return math.abs(a - b) <= eps
}

driver_analyze_atdd_directories :: proc(
	tests_dir: string,
	sut_dir:   string,
) -> (report: codegen.AST_Contract_Report, codegen_matches: bool, ok: bool) {
	resolved_tests := tests_dir
	resolved_sut   := sut_dir
	if !os.is_dir(resolved_tests) {
		resolved_tests, _ = filepath.join({#directory, "..", tests_dir}, context.temp_allocator)
	}
	if !os.is_dir(resolved_sut) {
		resolved_sut, _ = filepath.join({#directory, "..", sut_dir}, context.temp_allocator)
	}
	return codegen.analyze_atdd_directories(resolved_tests, resolved_sut, context.allocator)
}

driver_analyze_atdd_sources :: proc(
	spec_src: string,
	dsl_src:  string,
) -> (report: codegen.AST_Contract_Report, ok: bool) {
	return codegen.analyze_atdd_sources({spec_src}, dsl_src, nil, context.allocator)
}

driver_analyze_telemetry_call_sites :: proc(
	src: string,
) -> (report: codegen.Telemetry_Call_Site_Report, ok: bool) {
	return codegen.analyze_telemetry_call_sites({src}, context.temp_allocator)
}

@(private)
emit_one_concurrent_dynamic_pair :: proc(rec: ^heimdall.Recorder, worker_idx, iter_idx: int) {
	dyn_scope := fmt.aprintf("mt_scope_%d", worker_idx)
	dyn_name  := fmt.aprintf("mt_op_%d_%d", worker_idx, iter_idx)
	heimdall.scoped_span(rec, dyn_scope, dyn_name, i64(iter_idx))
	heimdall.event_emit(rec, dyn_scope, dyn_name, i64(iter_idx), .Ok)
	// Free caller strings BEFORE `scoped_span` exits at procedure return
	delete(dyn_scope)
	delete(dyn_name)
}

@(private)
concurrent_dynamic_worker_proc :: proc(args: ^Producer_Worker_Args) {
	for i in 0 ..< args.iterations {
		emit_one_concurrent_dynamic_pair(args.rec, args.worker_idx, i)
	}
}

driver_run_concurrent_dynamic_string_producers :: proc(
	s:                     ^Suite_Ctx,
	thread_count:          int,
	iterations_per_thread: int,
) -> (
	total_rows:          int,
	valid_string_rows:   int,
) {
	backing := make([]byte, 8 * 1024 * 1024)
	defer delete(backing)

	// Unsynchronized `mem.Arena` has zero internal locks; `Recorder._alloc_mu`
	// must serialize concurrent producer `clone_string_pair` calls and worker
	// `free_page_strings` / `OdinArrow` buffer allocations.
	arena: mem.Arena
	mem.arena_init(&arena, backing)
	arena_alloc := mem.arena_allocator(&arena)

	prefix_buf: [512]byte
	custom_prefix := fmt.bprintf(prefix_buf[:], "%s_mt_dyn", s.file_prefix)
	defer driver_cleanup_segment_files(custom_prefix)

	rec: heimdall.Recorder
	if heimdall.recorder_init(
		&rec,
		heimdall.Config{
			file_prefix          = custom_prefix,
			batches_per_file     = 1,
			ring_page_count      = 2,
			string_fallback_copy = true,
			policy               = .Block,
		},
		arena_alloc,
	) != nil {
		return 0, 0
	}

	threads := make([]^thread.Thread, thread_count, context.temp_allocator)
	args    := make([]Producer_Worker_Args, thread_count, context.temp_allocator)
	for i in 0 ..< thread_count {
		args[i] = Producer_Worker_Args{
			rec        = &rec,
			iterations = iterations_per_thread,
			worker_idx = i + 1,
		}
		threads[i] = thread.create_and_start_with_poly_data(&args[i], concurrent_dynamic_worker_proc)
	}

	for t_handle in threads {
		if t_handle != nil {
			thread.join(t_handle)
			thread.destroy(t_handle)
		}
	}

	heimdall.recorder_flush(&rec)
	heimdall.recorder_destroy(&rec)

	seg_buf: [512]byte
	for seg_idx in 0 ..< MAX_LOADED_SEGMENTS {
		fname := fmt.bprintf(seg_buf[:], "%s_%06d.arrow", custom_prefix, seg_idx)
		if !os.exists(fname) {
			break
		}
		sc, batches, ok := oa.ipc_read_file(fname)
		if !ok {
			break
		}
		for &b in batches {
			reader, r_ok := heimdall.record_batch_reader_resolve(&b)
			if r_ok {
				for r_idx in 0 ..< b.length {
					row := heimdall.record_batch_reader_decode_row(&reader, r_idx)
					total_rows += 1
					if strings.has_prefix(row.scope, "mt_scope_") && strings.has_prefix(row.name, "mt_op_") {
						valid_string_rows += 1
					}
				}
			}
			oa.record_batch_free(&b)
		}
		delete(batches)
		oa.schema_free(sc)
		free(sc)
	}
	return
}

@(private)
blocking_dynamic_burst_worker_proc :: proc(args: ^Blocking_Burst_Args) {
	for i in 0 ..< args.total_events {
		dyn_scope := fmt.aprintf("disable_wake_%d", i)
		dyn_name  := fmt.aprintf("ev_%d", i)
		heimdall.event_emit(args.rec, dyn_scope, dyn_name, i64(i), .Ok)
		delete(dyn_scope)
		delete(dyn_name)
	}
}

driver_run_blocking_producer_woken_by_disable :: proc(
	s:            ^Suite_Ctx,
	total_events: int,
) -> (
	blocked_while_paused: bool,
	woke_on_disable:      bool,
) {
	heimdall.recorder_set_worker_paused(&s.rec, true)

	args := Blocking_Burst_Args{
		rec          = &s.rec,
		total_events = total_events,
	}
	producer := thread.create_and_start_with_poly_data(&args, blocking_dynamic_burst_worker_proc)

	for _ in 0 ..< 500 {
		if heimdall.recorder_is_saturated(&s.rec) && heimdall.recorder_waiting_producers(&s.rec) > 0 {
			blocked_while_paused = true
			break
		}
		time.sleep(time.Millisecond)
	}

	// Disable the recorder while the worker is STILL paused:
	// `recorder_set_enabled(r, false)` must broadcast `_cv_page_free` so the blocked
	// producer wakes up, aborts `acquire_free_page_locked`, and frees its cloned string pair.
	heimdall.recorder_set_enabled(&s.rec, false)

	if producer != nil {
		thread.join(producer)
		thread.destroy(producer)
		woke_on_disable = true
	}

	heimdall.recorder_set_worker_paused(&s.rec, false)
	return
}

@(private)
Countdown_Failing_Allocator :: struct {
	backing:          mem.Allocator,
	armed:            bool,
	remaining_allocs: int,
	triggered_oom:    bool,
}

@(private)
countdown_failing_allocator_proc :: proc(
	allocator_data: rawptr,
	mode:           mem.Allocator_Mode,
	size:           int,
	alignment:      int,
	old_memory:     rawptr,
	old_size:       int,
	loc := #caller_location,
) -> ([]byte, mem.Allocator_Error) {
	state := (^Countdown_Failing_Allocator)(allocator_data)
	if state.armed && (mode == .Alloc || mode == .Alloc_Non_Zeroed) && size >= 4096 {
		if state.remaining_allocs <= 0 {
			state.triggered_oom = true
			state.armed         = false
			return nil, .Out_Of_Memory
		}
		state.remaining_allocs -= 1
	}
	return state.backing.procedure(state.backing.data, mode, size, alignment, old_memory, old_size, loc)
}

driver_verify_partial_batch_oom_and_string_offset_overflow_rollback :: proc(
	s: ^Suite_Ctx,
) -> (
	partial_oom_clean:      bool,
	overflow_guard_clean:   bool,
) {
	// Part 1: Mid-batch column allocation OOM inside `build_arrow_batch`
	// Arm `Countdown_Failing_Allocator` right before `recorder_flush` with `remaining_allocs = 5`
	// so `cols[0..2]` succeed (`built = 3`) and `cols[3]` fails with `.Out_Of_Memory`,
	// exercising `defer if !ok { for i in 0 ..< built { oa.array_free(&cols[i]) } }` under
	// a bounded `mem.Arena`.
	{
		backing := make([]byte, 2 * 1024 * 1024)
		defer delete(backing)

		arena: mem.Arena
		mem.arena_init(&arena, backing)

		fail_state := Countdown_Failing_Allocator{
			backing = mem.arena_allocator(&arena),
		}
		fail_alloc := mem.Allocator{
			procedure = countdown_failing_allocator_proc,
			data      = &fail_state,
		}

		oom_prefix_buf: [512]byte
		oom_prefix := fmt.bprintf(oom_prefix_buf[:], "%s_mid_batch_oom", s.file_prefix)
		defer driver_cleanup_segment_files(oom_prefix)

		rec: heimdall.Recorder
		if heimdall.recorder_init(
			&rec,
			heimdall.Config{
				file_prefix      = oom_prefix,
				batches_per_file = 1,
				ring_page_count  = 2,
				policy           = .Drop,
				backend          = .Dedicated_Thread,
			},
			fail_alloc,
		) == nil {
			for i in 0 ..< 8 {
				heimdall.event_emit(&rec, "oom_lib", "partial_col", i64(i), .Ok)
			}
			fail_state.remaining_allocs = 3
			fail_state.armed            = true
			heimdall.recorder_flush(&rec)
			fail_state.armed            = false
			flushed, _ := heimdall.recorder_diagnostics(&rec)
			heimdall.recorder_destroy(&rec)
			partial_oom_clean = (fail_state.triggered_oom && flushed == 0)
		}
	}

	// Part 2: 32-bit signed integer offset overflow guard in `build_string_column_from_soa`
	// Pass 1 checks `if s_len < 0 || total_bytes > int(max(i32)) - s_len` before touching `raw_data(s)`
	// and frees `off_buf` + `cols[0..7]` via `build_arrow_batch`'s `defer if !ok`.
	{
		ovf_prefix_buf: [512]byte
		ovf_prefix := fmt.bprintf(ovf_prefix_buf[:], "%s_str_ovf", s.file_prefix)
		defer driver_cleanup_segment_files(ovf_prefix)

		rec: heimdall.Recorder
		if heimdall.recorder_init(
			&rec,
			heimdall.Config{
				file_prefix          = ovf_prefix,
				batches_per_file     = 1,
				ring_page_count      = 2,
				string_fallback_copy = false,
				policy               = .Drop,
				backend              = .Sleipnir_Actor,
			},
		) == nil {
			dummy_byte: u8 = 'x'
			huge_synthetic_str := transmute(string)runtime.Raw_String{
				data = &dummy_byte,
				len  = int(max(i32)),
			}
			heimdall.event_emit(&rec, "ovf_scope", "row_0", 1, .Ok)
			heimdall.event_emit(&rec, "ovf_scope", huge_synthetic_str, 2, .Ok)
			heimdall.recorder_flush(&rec)
			flushed, _ := heimdall.recorder_diagnostics(&rec)
			heimdall.recorder_destroy(&rec)
			overflow_guard_clean = (flushed == 0)
		}
	}

	// Part 3: Deferred `scoped_span` with `string_fallback_copy = true` exiting AFTER `recorder_destroy(&rec)`
	// in the same block must free its cloned strings cleanly using `rec.allocator` without crashing.
	{
		def_prefix_buf: [512]byte
		def_prefix := fmt.bprintf(def_prefix_buf[:], "%s_def_span", s.file_prefix)
		defer driver_cleanup_segment_files(def_prefix)

		rec: heimdall.Recorder
		if heimdall.recorder_init(
			&rec,
			heimdall.Config{
				file_prefix          = def_prefix,
				batches_per_file     = 1,
				ring_page_count      = 2,
				string_fallback_copy = true,
				policy               = .Drop,
				backend              = .Sleipnir_Actor,
			},
		) == nil {
			s_buf: [32]byte
			n_buf: [32]byte
			dyn_scope := fmt.bprintf(s_buf[:], "post_destroy_scope")
			dyn_name  := fmt.bprintf(n_buf[:], "post_destroy_span")
			heimdall.scoped_span(&rec, dyn_scope, dyn_name, 77)
			heimdall.recorder_destroy(&rec)
		} else {
			overflow_guard_clean = false
		}
	}
	return
}

driver_verify_ast_unknown_dsl_poly_codegen_and_custom_schema :: proc() -> (
	unknown_dsl_rejected: bool,
	poly_variadic_ok:     bool,
	custom_schema_cols:   int,
) {
	// 1. Calling an undefined `then.undefined_step(42)` against a known DSL must fail `generate_atdd_from_sources`
	bad_spec :: `
package sample_tests
import "core:testing"
@(test)
spec_calls_missing :: proc(t: ^testing.T) {
	_ = scenario(t)
	then.undefined_step(42)
}
`
	known_dsl :: `
package sample_tests
then_defined_step :: proc(s: ^Suite_Ctx, val: int) {
}
`
	diags := make([dynamic]codegen.Codegen_Diagnostic)
	defer codegen.destroy_diagnostics(&diags)
	_, gen_ok := codegen.generate_atdd_from_sources(
		{known_dsl},
		{"dsl.odin"},
		{bad_spec},
		{"spec.odin"},
		&diags,
		context.temp_allocator,
	)
	unknown_dsl_rejected = (!gen_ok && len(diags) > 0)

	// 2. Polymorphic `$T` and variadic `..` DSL steps with named `loc = #caller_location`,
	//    plus token-boundary import filtering (`uptime_ns: int = 0` must NOT retain unused `import "core:time"`).
	poly_dsl :: `
package sample_tests
import "core:testing"
import "core:time"
upon_variadic_tags :: proc(s: ^Suite_Ctx, tags: ..i64, loc := #caller_location) {
}
then_poly_check_ctx :: proc(s: ^Suite_Ctx, val: $T, uptime_ns: int = 0, loc := #caller_location) {
}
`
	poly_spec :: `
package sample_tests
import "core:testing"
@(test)
spec_uses_poly_and_variadic :: proc(t: ^testing.T) {
	_ = scenario(t)
	upon.variadic_tags(10, 20, 30)
	then_poly_check(42, loc = #caller_location)
}
`
	poly_code, poly_ok := codegen.generate_atdd_from_sources(
		{poly_dsl},
		{"dsl.odin"},
		{poly_spec},
		{"spec.odin"},
		nil,
		context.temp_allocator,
	)
	poly_variadic_ok = poly_ok &&
		strings.contains(poly_code, "then_poly_check_ambient") &&
		strings.contains(poly_code, "then_poly_check :: proc {") &&
		strings.contains(poly_code, "..tags, loc = loc") &&
		!strings.contains(poly_code, `"core:time"`)

	// 3. Custom `@(telemetry_schema)` struct codegen
	custom_schema_src :: `
package custom_telemetry
State_Kind :: enum u8 { Idle = 0, Active = 1 }
@(telemetry_schema)
Custom_Tick :: struct {
	tick_ns:   i64,
	state:     State_Kind,
	subsystem: string,
	load_f64:  f64,
}
`
	schema_code, col_count, schema_ok := codegen.generate_telemetry_schema_from_source(
		custom_schema_src,
		"custom_tick.odin",
		context.temp_allocator,
	)
	if schema_ok &&
		strings.contains(schema_code, "RECORD_COLUMN_COUNT :: 4") &&
		strings.contains(schema_code, "Custom_Tick") &&
		strings.contains(schema_code, "tick_ns_data: [^]i64") &&
		strings.contains(schema_code, "subsystem_offsets: [^]i32") &&
		strings.contains(schema_code, "subsystem_bytes: [^]u8") {
		custom_schema_cols = col_count
	}
	return
}

@(private)
Continuous_Load_Args :: struct {
	rec:          ^heimdall.Recorder,
	stop_flag:    ^b32,
	emitted_rows: int,
}

@(private)
continuous_producer_load_proc :: proc(args: ^Continuous_Load_Args) {
	seq: i64 = 0
	for !intrinsics.atomic_load_explicit(args.stop_flag, .Acquire) || seq < 4096 {
		heimdall.event_emit(args.rec, "cont_load", "stream_ev", seq, .Ok)
		seq += 1
		if seq >= 32768 {
			break
		}
	}
	args.emitted_rows = int(seq)
}

driver_verify_live_flushes_under_continuous_producer_load :: proc(
	s:              ^Suite_Ctx,
	producer_count: int,
	flush_calls:    int,
) -> (
	flushes_completed: bool,
	zero_drops:        bool,
) {
	backends := [2]heimdall.Backend_Mode{.Dedicated_Thread, .Sleipnir_Actor}
	flushes_completed = true
	zero_drops        = true

	for mode, b_idx in backends {
		load_buf: [512]byte
		load_prefix := fmt.bprintf(load_buf[:], "%s_cont_flush_%d", s.file_prefix, b_idx)
		defer driver_cleanup_segment_files(load_prefix)

		rec: heimdall.Recorder
		if heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = load_prefix,
			batches_per_file = 2,
			ring_page_count  = 2,
			policy           = .Block,
			backend          = mode,
		}) != nil {
			return false, false
		}

		stop_flag: b32 = false
		n_prod := clamp(producer_count, 1, 4)
		threads: [4]^thread.Thread
		args:    [4]Continuous_Load_Args
		for i in 0 ..< n_prod {
			args[i] = Continuous_Load_Args{
				rec       = &rec,
				stop_flag = &stop_flag,
			}
			threads[i] = thread.create_and_start_with_poly_data(&args[i], continuous_producer_load_proc)
		}

		for _ in 0 ..< flush_calls {
			heimdall.event_emit(&rec, "cont_load", "flush_marker", 1, .Ok)
			heimdall.recorder_flush(&rec)
		}

		flushed_during_load, _ := heimdall.recorder_diagnostics(&rec)
		intrinsics.atomic_store_explicit(&stop_flag, true, .Release)

		for i in 0 ..< n_prod {
			if threads[i] != nil {
				thread.join(threads[i])
				thread.destroy(threads[i])
			}
		}

		heimdall.recorder_destroy(&rec)
		total_flushed, total_dropped := heimdall.recorder_diagnostics(&rec)
		if flushed_during_load < 1 || total_flushed < u64(flush_calls) {
			flushes_completed = false
		}
		if total_dropped != 0 {
			zero_drops = false
		}
	}
	return
}
