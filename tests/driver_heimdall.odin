package heimdall_tests

import "base:intrinsics"
import "core:fmt"
import "core:math"
import "core:mem"
import "core:os"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"
import "../heimdall"
import codegen "../heimdall/codegen"
import oa "../deps/OdinArrow/src"

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
	last_flushed_batches: u64,
	last_dropped_records: u64,
	file_prefix:          string,
	segments:             [MAX_LOADED_SEGMENTS]Loaded_Arrow_Segment,
}

@(private)
g_driver_prefix_seq: u64

driver_init :: proc(s: ^Suite_Ctx, t: ^testing.T) {
	s.t = t
	seq := intrinsics.atomic_add(&g_driver_prefix_seq, 1)
	s.file_prefix = fmt.aprintf("test_heimdall_atdd_%d", seq)
}

driver_destroy :: proc(s: ^Suite_Ctx) {
	if heimdall.default_recorder() == &s.rec {
		heimdall.set_default_recorder(nil)
	}
	if s.rec_active {
		driver_shutdown_recorder(s)
	}
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
		if len(s.file_prefix) > 0 {
			filename := fmt.tprintf("%s_%06d.arrow", s.file_prefix, i)
			_ = os.remove(filename)
		}
	}
	if len(s.file_prefix) > 0 {
		delete(s.file_prefix)
		s.file_prefix = ""
	}
}

driver_configure_recorder :: proc(
	s:                ^Suite_Ctx,
	batches_per_file: int,
	ring_pages:       int,
	string_copy:      bool,
	policy:           heimdall.Backpressure_Policy,
) -> bool {
	err := heimdall.recorder_init(&s.rec, heimdall.Config{
		file_prefix          = s.file_prefix,
		batches_per_file     = batches_per_file,
		ring_page_count      = ring_pages,
		string_Fallback_copy = string_copy,
		policy               = policy,
	})
	if err != nil {
		return false
	}
	s.rec_active = true
	return true
}

driver_configure_zero_config_recorder :: proc(s: ^Suite_Ctx) -> bool {
	err := heimdall.recorder_init(&s.rec)
	if err != nil {
		return false
	}
	s.rec_active = true
	return true
}

driver_set_recorder_enabled :: proc(s: ^Suite_Ctx, enabled: bool) {
	s.rec.enabled = enabled
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

driver_emit_saturation_burst :: proc(s: ^Suite_Ctx, total_events: int) {
	heimdall.recorder_set_worker_paused(&s.rec, true)
	for i in 0 ..< total_events {
		heimdall.event_emit(&s.rec, "burst_lib", "fast_tick", i64(i), .Ok)
	}
	heimdall.recorder_set_worker_paused(&s.rec, false)
}

driver_emit_dynamic_string_points :: proc(s: ^Suite_Ctx, count: int, use_default_rec: bool) {
	target := heimdall.default_recorder() if use_default_rec else &s.rec
	for i in 0 ..< count {
		dyn_scope := fmt.aprintf("worker_%d", i)
		dyn_name  := fmt.aprintf("op_%d", i)
		heimdall.histogram_record(target, dyn_scope, dyn_name, f64(i) * 1.5, i64(i))
		heimdall.event_emit(target, dyn_scope, dyn_name, i64(i), .Ok)
		// Immediately free caller strings to verify `string_Fallback_copy` cloned them safely
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
			file_prefix     = "oom_never_created",
			ring_page_count = requested_pages,
		},
		arena_alloc,
	)
	failed_with_oom = (err == .Out_Of_Memory)

	// Calling `recorder_destroy` on a failed `recorder_init` must be a safe no-op
	heimdall.recorder_destroy(&temp_rec)
	clean_after_destroy = (temp_rec.pages == nil && !temp_rec.enabled && temp_rec.worker == nil)
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

	// Wait until the producer fills all ring pages and blocks waiting for `cv_page_free`
	for _ in 0 ..< 500 {
		sync.mutex_lock(&s.rec.mu)
		all_pages_queued := (len(s.rec.free_stack) == 0 && s.rec.active_page_idx == -1 && len(s.rec.ready_queue) == s.rec.cfg.ring_page_count)
		sync.mutex_unlock(&s.rec.mu)
		if all_pages_queued {
			blocked_while_paused = true
			break
		}
		time.sleep(time.Millisecond)
	}

	// Unpause the background worker so it drains pages and signals `cv_page_free`
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

driver_flush_recorder :: proc(s: ^Suite_Ctx) {
	heimdall.recorder_flush(nil) // Also verify nil flush is a safe no-op
	heimdall.recorder_flush(&s.rec)
	s.last_flushed_batches = intrinsics.atomic_load(&s.rec.flushed_batches)
	s.last_dropped_records = intrinsics.atomic_load(&s.rec.dropped_records)
}

driver_shutdown_recorder :: proc(s: ^Suite_Ctx) {
	if !s.rec_active {
		return
	}
	if heimdall.default_recorder() == &s.rec {
		heimdall.set_default_recorder(nil)
	}
	heimdall.recorder_destroy(&s.rec)
	s.last_flushed_batches = s.rec.flushed_batches
	s.last_dropped_records = s.rec.dropped_records
	s.rec_active = false
}

driver_segment_file_exists :: proc(s: ^Suite_Ctx, segment_idx: int) -> bool {
	filename := fmt.tprintf("%s_%06d.arrow", s.file_prefix, segment_idx)
	return os.exists(filename)
}

driver_load_segment :: proc(s: ^Suite_Ctx, segment_idx: int) -> (seg: ^Loaded_Arrow_Segment, ok: bool) {
	if segment_idx < 0 || segment_idx >= MAX_LOADED_SEGMENTS {
		return nil, false
	}
	if s.segments[segment_idx].loaded {
		return &s.segments[segment_idx], true
	}
	filename := fmt.tprintf("%s_%06d.arrow", s.file_prefix, segment_idx)
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

	ts_col   := oa.record_batch_column(b, "timestamp_ns")
	dur_col  := oa.record_batch_column(b, "duration_ns")
	sid_col  := oa.record_batch_column(b, "span_id")
	pid_col  := oa.record_batch_column(b, "parent_id")
	tid_col  := oa.record_batch_column(b, "thread_id")
	kind_col := oa.record_batch_column(b, "kind")
	stat_col := oa.record_batch_column(b, "status")
	scp_col  := oa.record_batch_column(b, "scope")
	name_col := oa.record_batch_column(b, "name")
	val_col  := oa.record_batch_column(b, "value_f64")
	attr_col := oa.record_batch_column(b, "attr_i64")

	if ts_col == nil || dur_col == nil || sid_col == nil || pid_col == nil ||
	   tid_col == nil || kind_col == nil || stat_col == nil || scp_col == nil ||
	   name_col == nil || val_col == nil || attr_col == nil {
		return {}, false
	}

	return heimdall.Record{
		timestamp_ns = oa.array_get(ts_col,   row_idx, i64),
		duration_ns  = oa.array_get(dur_col,  row_idx, i64),
		span_id      = oa.array_get(sid_col,  row_idx, u64),
		parent_id    = oa.array_get(pid_col,  row_idx, u64),
		thread_id    = oa.array_get(tid_col,  row_idx, u32),
		kind         = heimdall.Kind(oa.array_get(kind_col, row_idx, u8)),
		status       = heimdall.Status(oa.array_get(stat_col, row_idx, u8)),
		scope        = oa.array_get_string(scp_col,  row_idx),
		name         = oa.array_get_string(name_col, row_idx),
		value_f64    = oa.array_get(val_col,  row_idx, f64),
		attr_i64     = oa.array_get(attr_col, row_idx, i64),
	}, true
}

Span_Meta :: struct {
	thread_id: u32,
	scope:     string,
	name:      string,
}

driver_verify_multithreaded_span_tree_integrity :: proc(
	s:                   ^Suite_Ctx,
	expected_total_rows: int,
	expected_Root_spans: int,
) -> (
	total_rows:          int,
	unique_spans:        int,
	root_spans:          int,
	cross_thread_leaks:  int,
	missing_parents:     int,
	distinct_threads:    int,
) {
	span_map   := make(map[u64]Span_Meta, expected_total_rows, context.temp_allocator)
	thread_set := make(map[u32]bool, 16, context.temp_allocator)

	// Pass 1: Index all spans by `span_id` and verify global uniqueness
	for seg_idx in 0 ..< MAX_LOADED_SEGMENTS {
		if !driver_segment_file_exists(s, seg_idx) {
			break
		}
		seg, ok := driver_load_segment(s, seg_idx)
		if !ok {
			break
		}
		for b_idx in 0 ..< len(seg.batches) {
			b_len := seg.batches[b_idx].length
			for r_idx in 0 ..< b_len {
				rec, r_ok := driver_read_row(s, seg_idx, b_idx, r_idx)
				if !r_ok {
					continue
				}
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
		}
	}

	// Pass 2: Verify every child span and metric with `parent_id > 0` points to a span on the SAME `thread_id`
	for seg_idx in 0 ..< MAX_LOADED_SEGMENTS {
		if !driver_segment_file_exists(s, seg_idx) {
			break
		}
		seg, ok := driver_load_segment(s, seg_idx)
		if !ok {
			break
		}
		for b_idx in 0 ..< len(seg.batches) {
			b_len := seg.batches[b_idx].length
			for r_idx in 0 ..< b_len {
				rec, r_ok := driver_read_row(s, seg_idx, b_idx, r_idx)
				if !r_ok || rec.parent_id == 0 {
					continue
				}
				parent_meta, found := span_map[rec.parent_id]
				if !found {
					missing_parents += 1
				} else if parent_meta.thread_id != rec.thread_id {
					cross_thread_leaks += 1
				}
			}
		}
	}

	_ = expected_Root_spans
	distinct_threads = len(thread_set)
	return
}

driver_f64_approx_eq :: proc(a, b: f64, eps: f64 = 1e-6) -> bool {
	return math.abs(a - b) <= eps
}

driver_analyze_atdd_directories :: proc(
	tests_dir: string,
	sut_dir:   string,
) -> (report: codegen.AST_Contract_Report, codegen_matches: bool, ok: bool) {
	return codegen.analyze_atdd_directories(tests_dir, sut_dir, context.temp_allocator)
}

driver_analyze_atdd_sources :: proc(
	spec_src: string,
	dsl_src:  string,
) -> (report: codegen.AST_Contract_Report, ok: bool) {
	return codegen.analyze_atdd_sources({spec_src}, dsl_src, nil, context.temp_allocator)
}
