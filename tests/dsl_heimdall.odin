package heimdall_tests

import "core:testing"
import "../heimdall"

// =============================================================================
// Layer 2: Domain-Specific Language (`tests/dsl_heimdall.odin`)
//
// Translates domain concepts (`given_*`, `upon_*`, `then_*`) into composable
// calls on Layer 3 (`driver_heimdall.odin`). Scanned by `heimdall_gen`
// (`core:odin/parser`) to emit `tests/dsl_atdd_gen.odin`.
// =============================================================================

given_recorder_configured :: proc(
	s:                        ^Suite_Ctx,
	batches_per_file:         int = 2,
	ring_pages:               int = 3,
	string_copy:              bool = false,
	policy:                   heimdall.Backpressure_Policy = .Block,
	backend:                  heimdall.Backend_Mode = .Dedicated_Thread,
	shared_scheduler_workers: int = 0,
	loc := #caller_location,
) {
	ok := driver_configure_recorder(s, batches_per_file, ring_pages, string_copy, policy, backend, shared_scheduler_workers)
	testing.expect(s.t, ok, "Expected recorder_init to succeed", loc = loc)
}

given_zero_config_recorder_initialized :: proc(
	s:   ^Suite_Ctx,
	loc := #caller_location,
) {
	ok := driver_configure_zero_config_recorder(s)
	testing.expect(s.t, ok, "Expected zero-config recorder_init to succeed", loc = loc)
}

given_recorder_enabled_state :: proc(
	s:       ^Suite_Ctx,
	enabled: bool,
) {
	driver_set_recorder_enabled(s, enabled)
}

given_registered_as_default_recorder :: proc(
	s: ^Suite_Ctx,
	loc := #caller_location,
) {
	ok := driver_set_default_recorder(s)
	testing.expect(s.t, ok, "Expected default_recorder() to point to active recorder", loc = loc)
}

upon_simulating_http_requests_with_nil_recorder :: proc(
	s:             ^Suite_Ctx,
	count:         int,
	payload_bytes: int,
) {
	_ = s
	for _ in 0 ..< count {
		driver_simulate_http_request(nil, payload_bytes)
	}
}

upon_simulating_http_requests :: proc(
	s:                  ^Suite_Ctx,
	count:              int,
	payload_step_bytes: int,
) {
	for i in 0 ..< count {
		driver_simulate_http_request(&s.rec, i * payload_step_bytes)
	}
}

upon_running_concurrent_http_producers :: proc(
	s:                     ^Suite_Ctx,
	thread_count:          int,
	iterations_per_thread: int,
) {
	driver_run_concurrent_producers(s, thread_count, iterations_per_thread)
}

upon_running_sleipnir_actor_producers :: proc(
	s:                  ^Suite_Ctx,
	actor_count:        int,
	messages_per_actor: int,
	loc := #caller_location,
) {
	ok := driver_run_sleipnir_actor_producers(s, actor_count, messages_per_actor)
	testing.expect(s.t, ok, "Expected Sleipnir actor producers to execute and drain on shared Scheduler", loc = loc)
}

upon_emitting_saturation_burst :: proc(
	s:            ^Suite_Ctx,
	total_events: int,
) {
	driver_emit_saturation_burst(s, total_events)
}

upon_emitting_dynamic_string_telemetry :: proc(
	s:                    ^Suite_Ctx,
	count:                int,
	use_default_recorder: bool = true,
) {
	driver_emit_dynamic_string_points(s, count, use_default_recorder)
}

upon_emitting_dynamic_scoped_spans :: proc(
	s:            ^Suite_Ctx,
	count:        int,
	pause_worker: bool = false,
) {
	driver_emit_dynamic_scoped_spans(s, count, pause_worker)
}

upon_running_blocking_producer_with_delayed_worker_unpause :: proc(
	s:            ^Suite_Ctx,
	total_events: int,
	loc := #caller_location,
) {
	blocked := driver_run_blocking_producer_with_delayed_unpause(s, total_events)
	testing.expect(s.t, blocked, "Expected producer thread to block on cv_page_free while worker was paused", loc = loc)
}

upon_simulating_three_level_and_sibling_call_tree :: proc(
	s:          ^Suite_Ctx,
	iterations: int = 1,
) {
	for _ in 0 ..< iterations {
		driver_simulate_three_level_and_sibling_call_tree(s)
	}
}

upon_recording_all_telemetry_kinds :: proc(
	s:             ^Suite_Ctx,
	scope:         string,
	metric_name:   string,
	counter_delta: f64,
	gauge_val:     f64,
	hist_sample:   f64,
	event_attr:    i64,
	event_status:  heimdall.Status = .Ok,
) {
	driver_record_custom_points(s, scope, metric_name, counter_delta, gauge_val, hist_sample, event_attr, event_status)
}

upon_simulating_fallback_http_request :: proc(
	s:                ^Suite_Ctx,
	initial_http_tag: i64,
	child_err_code:   i64,
	propagate_err:    bool = false,
	parent_err_code:  i64  = 0,
) {
	driver_simulate_fallback_request(&s.rec, initial_http_tag, child_err_code, propagate_err, parent_err_code)
}

upon_flushing_live_recorder :: proc(
	s:                         ^Suite_Ctx,
	expected_flushed_segments: u64,
	loc := #caller_location,
) {
	driver_flush_recorder(s)
	testing.expect_value(s.t, s.last_flushed_batches, expected_flushed_segments, loc = loc)
}

upon_flushing_and_shutting_down_recorder :: proc(
	s:           ^Suite_Ctx,
	flush_first: bool = false,
) {
	if flush_first {
		driver_flush_recorder(s)
	}
	driver_shutdown_recorder(s)
}

then_recorder_default_config_is :: proc(
	s:                     ^Suite_Ctx,
	expected_prefix:       string,
	expected_batches_file: int,
	expected_ring_pages:   int,
	loc := #caller_location,
) {
	testing.expect_value(s.t, s.rec.cfg.file_prefix,      expected_prefix,       loc = loc)
	testing.expect_value(s.t, s.rec.cfg.batches_per_file, expected_batches_file, loc = loc)
	testing.expect_value(s.t, s.rec.cfg.ring_page_count,  expected_ring_pages,   loc = loc)
}

then_oom_recorder_init_rolls_back_and_destroys_cleanly :: proc(
	s:               ^Suite_Ctx,
	arena_bytes:     int,
	requested_pages: int,
	loc := #caller_location,
) {
	failed_with_oom, clean := driver_test_oom_recorder_init(s, arena_bytes, requested_pages)
	testing.expect(s.t, failed_with_oom, "Expected recorder_init to return .Out_Of_Memory when arena is exhausted", loc = loc)
	testing.expect(s.t, clean, "Expected failed recorder_init to leave zeroed Recorder safe for recorder_destroy", loc = loc)
}

then_recorder_diagnostics_are :: proc(
	s:                        ^Suite_Ctx,
	expected_flushed_segments: u64,
	expected_dropped_records:  u64,
	loc := #caller_location,
) {
	testing.expect_value(s.t, s.last_flushed_batches, expected_flushed_segments, loc = loc)
	testing.expect_value(s.t, s.last_dropped_records, expected_dropped_records,  loc = loc)
}

then_drop_backpressure_conserves_total_records :: proc(
	s:                       ^Suite_Ctx,
	expected_total_events:   int,
	expected_persisted_rows: int,
	expected_dropped_rows:   int,
	loc := #caller_location,
) {
	persisted_rows := driver_total_rows_across_segments(s)
	dropped_rows   := int(s.last_dropped_records)
	testing.expect_value(s.t, persisted_rows,                expected_persisted_rows, loc = loc)
	testing.expect_value(s.t, dropped_rows,                  expected_dropped_rows,   loc = loc)
	testing.expect_value(s.t, persisted_rows + dropped_rows, expected_total_events,   loc = loc)
}

then_arrow_segment_exists_is :: proc(
	s:              ^Suite_Ctx,
	segment_idx:    int,
	expected_exist: bool,
	loc := #caller_location,
) {
	testing.expect_value(s.t, driver_segment_file_exists(s, segment_idx), expected_exist, loc = loc)
}

then_arrow_segment_batch_count_is :: proc(
	s:                ^Suite_Ctx,
	segment_idx:      int,
	expected_batches: int,
	loc := #caller_location,
) {
	seg, ok := driver_load_segment(s, segment_idx)
	testing.expect(s.t, ok, "Expected valid Arrow IPC segment file on disk", loc = loc)
	if ok {
		testing.expect_value(s.t, len(seg.batches), expected_batches, loc = loc)
		testing.expect_value(s.t, len(seg.schema.fields), 11, loc = loc)
	}
}

then_arrow_batch_row_count_is :: proc(
	s:             ^Suite_Ctx,
	segment_idx:   int,
	batch_idx:     int,
	expected_rows: int,
	loc := #caller_location,
) {
	seg, ok := driver_load_segment(s, segment_idx)
	testing.expect(s.t, ok, "Expected valid Arrow IPC segment file on disk", loc = loc)
	if ok && batch_idx >= 0 && batch_idx < len(seg.batches) {
		testing.expect_value(s.t, seg.batches[batch_idx].length, expected_rows, loc = loc)
	} else if ok {
		testing.expect(s.t, false, "batch_idx out of range for segment", loc = loc)
	}
}

then_telemetry_row_matches :: proc(
	s:                  ^Suite_Ctx,
	segment_idx:        int,
	batch_idx:          int,
	row_idx:            int,
	expected_kind:      heimdall.Kind,
	expected_status:    heimdall.Status,
	expected_scope:     string,
	expected_name:      string,
	expected_span_id:   u64,
	expected_parent_id: u64,
	expected_attr_i64:  i64,
	expected_value_f64: f64 = -1.0,
	loc := #caller_location,
) {
	rec, ok := driver_read_row(s, segment_idx, batch_idx, row_idx)
	testing.expect(s.t, ok, "Expected readable telemetry row in Arrow batch", loc = loc)
	if ok {
		testing.expect_value(s.t, rec.kind,      expected_kind,      loc = loc)
		testing.expect_value(s.t, rec.status,    expected_status,    loc = loc)
		testing.expect_value(s.t, rec.scope,     expected_scope,     loc = loc)
		testing.expect_value(s.t, rec.name,      expected_name,      loc = loc)
		testing.expect_value(s.t, rec.span_id,   expected_span_id,   loc = loc)
		testing.expect_value(s.t, rec.parent_id, expected_parent_id, loc = loc)
		testing.expect_value(s.t, rec.attr_i64,  expected_attr_i64,  loc = loc)
		testing.expect(s.t, rec.timestamp_ns > 0, "Expected positive timestamp_ns", loc = loc)
		testing.expect(s.t, rec.thread_id > 0,    "Expected positive thread_id",    loc = loc)
		if expected_kind == .Span {
			testing.expect(s.t, rec.duration_ns >= 0, "Expected non-negative span duration_ns", loc = loc)
			testing.expect(
				s.t,
				driver_f64_approx_eq(rec.value_f64, f64(rec.duration_ns) / 1e6),
				"Expected span value_f64 to equal duration_ns / 1e6 (ms)",
				loc = loc,
			)
		} else if expected_value_f64 >= 0.0 {
			testing.expect(
				s.t,
				driver_f64_approx_eq(rec.value_f64, expected_value_f64),
				"Expected metric value_f64 to match expected value",
				loc = loc,
			)
		}
	}
}

then_multithreaded_span_trees_are_isolated :: proc(
	s:                    ^Suite_Ctx,
	expected_total_rows:  int,
	expected_unique_spans: int,
	expected_root_spans:  int,
	expected_min_threads: int,
	loc := #caller_location,
) {
	total_rows, unique_spans, root_spans, cross_thread_leaks, missing_parents, distinct_threads :=
		driver_verify_multithreaded_span_tree_integrity(s, expected_total_rows, expected_root_spans)

	testing.expect_value(s.t, total_rows,         expected_total_rows,   loc = loc)
	testing.expect_value(s.t, unique_spans,       expected_unique_spans, loc = loc)
	testing.expect_value(s.t, root_spans,         expected_root_spans,   loc = loc)
	testing.expect_value(s.t, cross_thread_leaks, 0,                     loc = loc)
	testing.expect_value(s.t, missing_parents,    0,                     loc = loc)
	testing.expect(
		s.t,
		distinct_threads >= expected_min_threads,
		"Expected records from multiple distinct OS producer threads",
		loc = loc,
	)
}

then_ast_enforces_four_layer_atdd_contract :: proc(
	s:         ^Suite_Ctx,
	tests_dir: string,
	sut_dir:   string,
	loc := #caller_location,
) {
	report, codegen_matches, ok := driver_analyze_atdd_directories(tests_dir, sut_dir)
	testing.expect(s.t, ok, "Expected Odin AST parser to parse all tests/ and heimdall/ files", loc = loc)
	testing.expect(s.t, codegen_matches, "Expected tests/dsl_atdd_gen.odin to match live AST codegen output", loc = loc)
	testing.expect(s.t, report.spec_procs_checked >= 8, "Expected at least 8 @(test) ATDD scenarios", loc = loc)
	testing.expect(s.t, report.dsl_steps_defined >= 18, "Expected at least 18 Layer 2 DSL steps", loc = loc)
	testing.expect_value(s.t, report.unused_dsl_procedures, 0, loc = loc)
	testing.expect_value(s.t, report.direct_sut_calls,      0, loc = loc)
	testing.expect_value(s.t, report.raw_testing_calls,     0, loc = loc)
	testing.expect_value(s.t, report.zero_arg_mega_steps,   0, loc = loc)
	testing.expect_value(s.t, report.driver_field_leaks,    0, loc = loc)
	testing.expect_value(s.t, report.forbidden_imports,     0, loc = loc)
	testing.expect_value(s.t, report.do_statements,         0, loc = loc)
}

then_ast_detects_layer_violations_in_synthetic_spec :: proc(
	s:                       ^Suite_Ctx,
	synthetic_spec_src:      string,
	synthetic_dsl_src:       string,
	expected_sut_calls:      int,
	expected_testing_calls:  int,
	expected_zero_arg_steps: int,
	expected_driver_leaks:   int,
	expected_forbidden_imps: int,
	expected_do_statements:  int,
	loc := #caller_location,
) {
	report, ok := driver_analyze_atdd_sources(synthetic_spec_src, synthetic_dsl_src)
	testing.expect(s.t, ok, "Expected synthetic spec and DSL sources to parse into AST", loc = loc)
	testing.expect_value(s.t, report.direct_sut_calls,    expected_sut_calls,      loc = loc)
	testing.expect_value(s.t, report.raw_testing_calls,   expected_testing_calls,  loc = loc)
	testing.expect_value(s.t, report.zero_arg_mega_steps, expected_zero_arg_steps, loc = loc)
	testing.expect_value(s.t, report.driver_field_leaks,  expected_driver_leaks,   loc = loc)
	testing.expect_value(s.t, report.forbidden_imports,   expected_forbidden_imps, loc = loc)
	testing.expect_value(s.t, report.do_statements,       expected_do_statements,  loc = loc)
}
