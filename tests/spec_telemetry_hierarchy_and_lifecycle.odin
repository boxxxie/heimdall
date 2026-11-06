package heimdall_tests

import "core:testing"

// =============================================================================
// Layer 1: Executable Specifications (`tests/spec_telemetry_hierarchy_and_lifecycle.odin`)
//
// Written purely in the domain DSL (`given.*`, `upon.*`, `then.*` and `s->step`).
// Zero `OdinArrow` imports, zero direct `heimdall.*` calls, zero `testing.expect*` calls.
// =============================================================================

@(test)
spec_nil_recorder_is_zero_cost_and_nested_spans_link_automatically :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 1. Calling instrumented libraries with a `nil` recorder is a safe, zero-cost no-op
	upon.simulating_http_requests_with_nil_recorder(count = 10, payload_bytes = 1024)

	// 2. Active recorder streams 100 nested HTTP -> DB library calls into a 400-row Arrow batch
	given.recorder_configured(batches_per_file = 2, ring_pages = 3, string_copy = false, policy = .Block)

	upon.simulating_http_requests(count = 100, payload_step_bytes = 64)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 1, expected_dropped_records = 0)
	then.arrow_segment_batch_count_is(segment_idx = 0, expected_batches = 1)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 400)

	// Verify automatic `@(thread_local)` parent_id -> span_id hierarchy on the first request (payload = 0):
	//   Row 0: counter `http_lib.requests_total` emitted inside `handle_request` (parent_id = 1, value_f64 = 1.0)
	//   Row 1: gauge `db_driver.pool_connections_active` emitted inside `execute_query` (parent_id = 2, value_f64 = 8.0)
	//   Row 2: child span `db_driver.execute_query` exits first (span_id = 2, parent_id = 1)
	//   Row 3: parent span `http_lib.handle_request` exits second (span_id = 1, parent_id = 0)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 0,
		expected_kind      = .Counter,
		expected_status    = .Ok,
		expected_scope     = "http_lib",
		expected_name      = "requests_total",
		expected_span_id   = 0,
		expected_parent_id = 1,
		expected_attr_i64  = 0,
		expected_value_f64 = 1.0,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 1,
		expected_kind      = .Gauge,
		expected_status    = .Ok,
		expected_scope     = "db_driver",
		expected_name      = "pool_connections_active",
		expected_span_id   = 0,
		expected_parent_id = 2,
		expected_attr_i64  = 42,
		expected_value_f64 = 8.0,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 2,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "db_driver",
		expected_name      = "execute_query",
		expected_span_id   = 2,
		expected_parent_id = 1,
		expected_attr_i64  = 42,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 3,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "http_lib",
		expected_name      = "handle_request",
		expected_span_id   = 1,
		expected_parent_id = 0,
		expected_attr_i64  = 0,
	)
}

@(test)
spec_dynamic_strings_default_recorder_and_manual_flush_persist_cleanly :: proc(t: ^testing.T) {
	s := scenario(t)

	s->given_recorder_configured(batches_per_file = 1, ring_pages = 2, string_copy = true, policy = .Block)
	s->given_registered_as_default_recorder()

	// Emits 10 pairs of (Histogram, Event) using dynamically allocated strings that are freed immediately
	s->upon_emitting_dynamic_string_telemetry(count = 10, use_default_recorder = true)
	s->upon_flushing_and_shutting_down_recorder(flush_first = true)

	s->then_arrow_segment_batch_count_is(segment_idx = 0, expected_batches = 1)
	s->then_arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 20)

	s->then_telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 0,
		expected_kind      = .Histogram,
		expected_status    = .Ok,
		expected_scope     = "worker_0",
		expected_name      = "op_0",
		expected_span_id   = 0,
		expected_parent_id = 0,
		expected_attr_i64  = 0,
		expected_value_f64 = 0.0,
	)
	s->then_telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 2,
		expected_kind      = .Histogram,
		expected_status    = .Ok,
		expected_scope     = "worker_1",
		expected_name      = "op_1",
		expected_span_id   = 0,
		expected_parent_id = 0,
		expected_attr_i64  = 1,
		expected_value_f64 = 1.5,
	)
	s->then_telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 19,
		expected_kind      = .Event,
		expected_status    = .Ok,
		expected_scope     = "worker_9",
		expected_name      = "op_9",
		expected_span_id   = 0,
		expected_parent_id = 0,
		expected_attr_i64  = 9,
		expected_value_f64 = 0.0,
	)
}

@(test)
spec_all_five_canonical_telemetry_kinds_and_error_status :: proc(t: ^testing.T) {
	_ = scenario(t)

	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = false, policy = .Block)

	upon.recording_all_telemetry_kinds(
		scope         = "auth_svc",
		metric_name   = "verify_token",
		counter_delta = 1.0,
		gauge_val     = 12.0,
		hist_sample   = 3.25,
		event_attr    = 401,
		event_status  = .Error,
	)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.arrow_segment_batch_count_is(segment_idx = 0, expected_batches = 1)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 5)

	// Rows 0..3 are Counter, Gauge, Histogram, Event (with parent_id = 1); Row 4 is Span (span_id = 1)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 1,
		expected_kind      = .Gauge,
		expected_status    = .Ok,
		expected_scope     = "auth_svc",
		expected_name      = "verify_token",
		expected_span_id   = 0,
		expected_parent_id = 1,
		expected_attr_i64  = 401,
		expected_value_f64 = 12.0,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 2,
		expected_kind      = .Histogram,
		expected_status    = .Ok,
		expected_scope     = "auth_svc",
		expected_name      = "verify_token",
		expected_span_id   = 0,
		expected_parent_id = 1,
		expected_attr_i64  = 401,
		expected_value_f64 = 3.25,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 3,
		expected_kind      = .Event,
		expected_status    = .Error,
		expected_scope     = "auth_svc",
		expected_name      = "verify_token",
		expected_span_id   = 0,
		expected_parent_id = 1,
		expected_attr_i64  = 401,
		expected_value_f64 = 0.0,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 4,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "auth_svc",
		expected_name      = "verify_token",
		expected_span_id   = 1,
		expected_parent_id = 0,
		expected_attr_i64  = 401,
	)
}

@(test)
spec_full_page_rotation_and_multi_segment_arrow_file_rolling :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 2 batches per file (2 * 4,096 = 8,192 rows per segment file)
	given.recorder_configured(batches_per_file = 2, ring_pages = 3, string_copy = false, policy = .Block)

	// 2,250 HTTP requests * 4 records/request = 9,000 total records:
	//   - Segment 0 (`_000000.arrow`): 2 full batches of 4,096 rows = 8,192 rows
	//   - Segment 1 (`_000001.arrow`): 1 partial batch of 808 rows drained on shutdown
	upon.simulating_http_requests(count = 2250, payload_step_bytes = 16)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 2, expected_dropped_records = 0)

	then.arrow_segment_exists_is(segment_idx = 0, expected_exist = true)
	then.arrow_segment_batch_count_is(segment_idx = 0, expected_batches = 2)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 4096)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 1, expected_rows = 4096)

	then.arrow_segment_exists_is(segment_idx = 1, expected_exist = true)
	then.arrow_segment_batch_count_is(segment_idx = 1, expected_batches = 1)
	then.arrow_batch_row_count_is(segment_idx = 1, batch_idx = 0, expected_rows = 808)

	then.arrow_segment_exists_is(segment_idx = 2, expected_exist = false)
}

@(test)
spec_drop_backpressure_never_blocks_and_accounts_for_every_record :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 2-page ring (2 * 4,096 = 8,192 rows max in-flight) with `.Drop` backpressure policy
	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = false, policy = .Drop)

	// Emit a 10,000-record burst while worker consumption is paused:
	//   - First 8,192 records fill both #soa ring pages
	//   - Remaining 1,808 records drop immediately without blocking the caller
	upon.emitting_saturation_burst(total_events = 10000)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 1, expected_dropped_records = 1808)
	then.drop_backpressure_conserves_total_records(
		expected_total_events   = 10000,
		expected_persisted_rows = 8192,
		expected_dropped_rows   = 1808,
	)
}

@(test)
spec_multithreaded_producers_preserve_thread_local_span_hierarchy :: proc(t: ^testing.T) {
	_ = scenario(t)

	given.recorder_configured(batches_per_file = 4, ring_pages = 4, string_copy = false, policy = .Block)

	// 4 concurrent OS threads * 150 HTTP requests * 4 records/request = 2,400 rows (1,200 spans, 600 root spans)
	upon.running_concurrent_http_producers(thread_count = 4, iterations_per_thread = 150)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 1, expected_dropped_records = 0)
	then.multithreaded_span_trees_are_isolated(
		expected_total_rows   = 2400,
		expected_unique_spans = 1200,
		expected_root_spans   = 600,
		expected_min_threads  = 2,
	)
}

@(test)
spec_zero_config_defaults_disabled_toggle_and_empty_shutdown_write_no_files :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 1. Zero-value `Config{}` applies canonical defaults (`"telemetry"`, 16 batches/file, 4 ring pages)
	given.zero_config_recorder_initialized()
	then.recorder_default_config_is(
		expected_prefix       = "telemetry",
		expected_batches_file = 16,
		expected_ring_pages   = 4,
	)

	// 2. Disabling `rec.enabled = false` makes all 5 telemetry kinds + flush a zero-record no-op
	given.recorder_enabled_state(enabled = false)
	upon.recording_all_telemetry_kinds(
		scope         = "disabled_svc",
		metric_name   = "ignored_op",
		counter_delta = 5.0,
		gauge_val     = 10.0,
		hist_sample   = 1.25,
		event_attr    = 99,
		event_status  = .Error,
	)
	upon.flushing_and_shutting_down_recorder(flush_first = true)

	// 3. Zero records emitted -> zero segment files written on disk
	then.recorder_diagnostics_are(expected_flushed_segments = 0, expected_dropped_records = 0)
	then.arrow_segment_exists_is(segment_idx = 0, expected_exist = false)
}

@(test)
spec_live_recorder_flush_persists_partial_segments_while_recorder_remains_active :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Configure with `batches_per_file = 16` (65,536 rows/file default threshold)
	given.recorder_configured(batches_per_file = 16, ring_pages = 3, string_copy = false, policy = .Block)

	// 1. Emit 25 HTTP requests (100 rows) and flush live without shutting down
	upon.simulating_http_requests(count = 25, payload_step_bytes = 32)
	upon.flushing_live_recorder(expected_flushed_segments = 1)

	// Segment 0 (`_000000.arrow`) is immediately readable on disk while the recorder stays open
	then.arrow_segment_exists_is(segment_idx = 0, expected_exist = true)
	then.arrow_segment_batch_count_is(segment_idx = 0, expected_batches = 1)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 100)

	// 2. Emit 15 more HTTP requests (60 rows) on the same active recorder and flush Segment 1 (`_000001.arrow`)
	upon.simulating_http_requests(count = 15, payload_step_bytes = 64)
	upon.flushing_live_recorder(expected_flushed_segments = 2)

	then.arrow_segment_exists_is(segment_idx = 1, expected_exist = true)
	then.arrow_segment_batch_count_is(segment_idx = 1, expected_batches = 1)
	then.arrow_batch_row_count_is(segment_idx = 1, batch_idx = 0, expected_rows = 60)

	upon.flushing_and_shutting_down_recorder(flush_first = false)
	then.recorder_diagnostics_are(expected_flushed_segments = 2, expected_dropped_records = 0)
}

@(test)
spec_scoped_span_error_status_and_attr_override_preserve_parent_state :: proc(t: ^testing.T) {
	_ = scenario(t)

	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = false, policy = .Block)

	// Request 1: Child query_primary fails with 503 (`span_set_error`), replica succeeds, parent recovers (stays `.Ok`, tag 200)
	upon.simulating_fallback_http_request(
		initial_http_tag = 200,
		child_err_code   = 503,
		propagate_err    = false,
		parent_err_code  = 0,
	)

	// Request 2: Child query_primary fails with 504, and parent also marks itself `.Error` with status code 500
	upon.simulating_fallback_http_request(
		initial_http_tag = 200,
		child_err_code   = 504,
		propagate_err    = true,
		parent_err_code  = 500,
	)

	upon.flushing_and_shutting_down_recorder(flush_first = false)

	// Each fallback request emits 4 records:
	//   Row 0: child span `db_driver.query_primary` (span_id = 2, parent_id = 1, status = .Error, attr_i64 = 503)
	//   Row 1: gauge `db_driver.pool_connections_active` (parent_id = 3, attr_i64 = 2)
	//   Row 2: child span `db_driver.execute_query` (span_id = 3, parent_id = 1, status = .Ok, attr_i64 = 2)
	//   Row 3: parent span `http_lib.handle_request` (span_id = 1, parent_id = 0, status = .Ok, attr_i64 = 200)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 8)

	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 0,
		expected_kind      = .Span,
		expected_status    = .Error,
		expected_scope     = "db_driver",
		expected_name      = "query_primary",
		expected_span_id   = 2,
		expected_parent_id = 1,
		expected_attr_i64  = 503,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 2,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "db_driver",
		expected_name      = "execute_query",
		expected_span_id   = 3,
		expected_parent_id = 1,
		expected_attr_i64  = 2,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 3,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "http_lib",
		expected_name      = "handle_request",
		expected_span_id   = 1,
		expected_parent_id = 0,
		expected_attr_i64  = 200,
	)

	// Request 2 parent span (Row 7): propagated `.Error` with `attr_i64 = 500`
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 7,
		expected_kind      = .Span,
		expected_status    = .Error,
		expected_scope     = "http_lib",
		expected_name      = "handle_request",
		expected_span_id   = 4,
		expected_parent_id = 0,
		expected_attr_i64  = 500,
	)
}

@(test)
spec_oom_recorder_init_rolls_back_partial_pages_and_destroys_safely :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 1. Page allocation OOM: 1 `#soa[4096]Record` page requires ~352 KB. A 512 KB arena allows
	//    `r._pages` + Page 0 to succeed, then fails with `.Out_Of_Memory` on Page 1.
	then.oom_recorder_init_rolls_back_and_destroys_cleanly(
		arena_bytes     = 512 * 1024,
		requested_pages = 3,
	)

	// 2. Synchronous `Flusher_Engine` init OOM: `688 KB + 512 B` allows both `#soa[4096]Record`
	//    pages (`2 * 344 KB = 688 KB`) + `_free_stack` + `_ready_ring` to succeed, then fails inside
	//    `flusher_engine_init` before spawning a background worker thread.
	then.oom_recorder_init_rolls_back_and_destroys_cleanly(
		arena_bytes     = 688 * 1024 + 512,
		requested_pages = 2,
	)
}

@(test)
spec_dynamic_strings_in_scoped_spans_survive_early_caller_free_and_drop_cleanup :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Configure a 2-page ring (8,192 rows max in-flight) with `string_fallback_copy = true` and `.Drop` policy
	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = true, policy = .Drop)

	// Emit 8,500 `scoped_span` calls while worker is paused, freeing caller strings BEFORE each span exits:
	//   - First 8,192 spans are cloned in `span_begin`, survive caller `delete()`, and persist to Segment 0
	//   - Remaining 308 spans are cloned in `span_begin`, dropped in `span_end`, and their cloned strings are freed immediately
	upon.emitting_dynamic_scoped_spans(count = 8500, pause_worker = true)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 1, expected_dropped_records = 308)
	then.drop_backpressure_conserves_total_records(
		expected_total_events   = 8500,
		expected_persisted_rows = 8192,
		expected_dropped_rows   = 308,
	)

	// Verify the dynamically formatted strings were preserved in the Arrow IPC file
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 0,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "span_scope_0",
		expected_name      = "span_op_0",
		expected_span_id   = 1,
		expected_parent_id = 0,
		expected_attr_i64  = 0,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 1,
		row_idx            = 4095,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "span_scope_8191",
		expected_name      = "span_op_8191",
		expected_span_id   = 8192,
		expected_parent_id = 0,
		expected_attr_i64  = 8191,
	)
}

@(test)
spec_block_backpressure_waits_for_worker_and_persists_all_records_with_zero_drops :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 2-page ring (8,192 rows capacity) with `.Block` backpressure policy
	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = false, policy = .Block)

	// Producer thread emits 10,000 events while worker starts paused:
	//   - Producer fills both 4,096-row pages and blocks on `cv_page_free`
	//   - Worker is unpaused, drains Segment 0 (8,192 rows), wakes producer, and drains Segment 1 (1,808 rows)
	upon.running_blocking_producer_with_delayed_worker_unpause(total_events = 10000)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 2, expected_dropped_records = 0)
	then.drop_backpressure_conserves_total_records(
		expected_total_events   = 10000,
		expected_persisted_rows = 10000,
		expected_dropped_rows   = 0,
	)
}

@(test)
spec_three_level_nested_and_sibling_spans_unwind_thread_local_context_to_root :: proc(t: ^testing.T) {
	_ = scenario(t)

	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = false, policy = .Block)

	// Call tree:
	//   L1 `gateway.ingress` (span_id = 1, parent_id = 0)
	//     ├── L2a `auth.verify` (span_id = 2, parent_id = 1)        -> exits 1st (Row 0)
	//     └── L2b `orders.create` (span_id = 3, parent_id = 1)
	//           ├── L3a `cache.lookup` (span_id = 4, parent_id = 3) -> exits 2nd (Row 1)
	//           └── L3b `db.insert` (span_id = 5, parent_id = 3)    -> exits 3rd (Row 2)
	//         L2b `orders.create`                                   -> exits 4th (Row 3)
	//   L1 `gateway.ingress`                                        -> exits 5th (Row 4)
	//   Root event `gateway.after_response` (parent_id = 0)         -> emitted 6th (Row 5)
	upon.simulating_three_level_and_sibling_call_tree(iterations = 1)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 6)

	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 0,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "auth",
		expected_name      = "verify",
		expected_span_id   = 2,
		expected_parent_id = 1,
		expected_attr_i64  = 201,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 1,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "cache",
		expected_name      = "lookup",
		expected_span_id   = 4,
		expected_parent_id = 3,
		expected_attr_i64  = 301,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 2,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "db",
		expected_name      = "insert",
		expected_span_id   = 5,
		expected_parent_id = 3,
		expected_attr_i64  = 302,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 3,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "orders",
		expected_name      = "create",
		expected_span_id   = 3,
		expected_parent_id = 1,
		expected_attr_i64  = 202,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 4,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "gateway",
		expected_name      = "ingress",
		expected_span_id   = 1,
		expected_parent_id = 0,
		expected_attr_i64  = 100,
	)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 5,
		expected_kind      = .Event,
		expected_status    = .Ok,
		expected_scope     = "gateway",
		expected_name      = "after_response",
		expected_span_id   = 0,
		expected_parent_id = 0,
		expected_attr_i64  = 999,
		expected_value_f64 = 0.0,
	)
}

@(test)
spec_sleipnir_actor_backend_standalone_flushes_and_rotates_multi_segment_arrow_files :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Configure with `backend = .Sleipnir_Actor` and `shared_scheduler_workers = 0` (standalone owned 1-worker Scheduler)
	given.recorder_configured(
		batches_per_file         = 2,
		ring_pages               = 3,
		string_copy              = false,
		policy                   = .Block,
		backend                  = .Sleipnir_Actor,
		shared_scheduler_workers = 0,
	)

	// 2,250 HTTP requests * 4 records/request = 9,000 total records:
	//   - Segment 0 (`_000000.arrow`): 2 full batches of 4,096 rows = 8,192 rows flushed asynchronously via `actor.send(.Drain)`
	//   - Segment 1 (`_000001.arrow`): 1 partial batch of 808 rows flushed synchronously via `actor.call(.Flush)`
	upon.simulating_http_requests(count = 2250, payload_step_bytes = 16)
	upon.flushing_live_recorder(expected_flushed_segments = 2)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 2, expected_dropped_records = 0)

	then.arrow_segment_exists_is(segment_idx = 0, expected_exist = true)
	then.arrow_segment_batch_count_is(segment_idx = 0, expected_batches = 2)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 4096)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 1, expected_rows = 4096)

	then.arrow_segment_exists_is(segment_idx = 1, expected_exist = true)
	then.arrow_segment_batch_count_is(segment_idx = 1, expected_batches = 1)
	then.arrow_batch_row_count_is(segment_idx = 1, batch_idx = 0, expected_rows = 808)
}

@(test)
spec_sleipnir_actor_backend_shares_scheduler_with_application_actors_and_preserves_span_trees :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Configure with `backend = .Sleipnir_Actor` attached to an external 4-worker `actor.Scheduler`
	given.recorder_configured(
		batches_per_file         = 4,
		ring_pages               = 4,
		string_copy              = false,
		policy                   = .Block,
		backend                  = .Sleipnir_Actor,
		shared_scheduler_workers = 4,
	)

	// 4 Sleipnir actors * 150 messages * 4 records/message = 2,400 rows (1,200 spans, 600 root spans)
	// sharing the same 4-worker `actor.Scheduler` as Heimdall's internal `Flusher_Engine` actor
	upon.running_sleipnir_actor_producers(actor_count = 4, messages_per_actor = 150)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 1, expected_dropped_records = 0)
	then.multithreaded_span_trees_are_isolated(
		expected_total_rows   = 2400,
		expected_unique_spans = 1200,
		expected_root_spans   = 600,
		expected_min_threads  = 2,
	)
}

@(test)
spec_caller_temp_allocator_survives_saturation_backpressure_flush_and_destroy :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Configure `.Sleipnir_Actor` with a 2-page ring (`2 * 4,096 = 8,192` rows) and `batches_per_file = 1`
	// so emitting 10,000 events triggers synchronous backpressure drainage inside `acquire_free_page_locked`,
	// followed by `recorder_flush` and `recorder_destroy`, while the caller holds a live `context.temp_allocator` slice.
	given.recorder_configured(
		batches_per_file = 1,
		ring_pages       = 2,
		string_copy      = false,
		policy           = .Block,
		backend          = .Sleipnir_Actor,
	)

	upon.emitting_events_and_flushing_with_live_temp_canary(total_events = 10000)

	then.recorder_diagnostics_are(expected_flushed_segments = 3, expected_dropped_records = 0)
	then.drop_backpressure_conserves_total_records(
		expected_total_events   = 10000,
		expected_persisted_rows = 10000,
		expected_dropped_rows   = 0,
	)
}

@(test)
spec_failed_arrow_file_write_frees_batches_without_incrementing_flushed_segments :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Point `file_prefix` at a non-existent directory so `oa.ipc_write_file` returns `false`.
	// `flusher_engine_flush_segment` must free all `OdinArrow` batches cleanly and keep `flushed_segments == 0`.
	given.recorder_configured(
		batches_per_file = 1,
		ring_pages       = 2,
		string_copy      = false,
		policy           = .Block,
		custom_prefix    = ".tmp/nonexistent_dir_heimdall_spec/seg",
	)

	upon.simulating_http_requests(count = 1250, payload_step_bytes = 8)
	upon.flushing_and_shutting_down_recorder(flush_first = true)

	then.recorder_diagnostics_are(expected_flushed_segments = 0, expected_dropped_records = 0)
}

@(test)
spec_sleipnir_actor_rapid_lifecycle_cycles_on_shared_scheduler_have_zero_uaf :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Verify standalone `.Sleipnir_Actor` (`scheduler = nil`) does not hijack an ambient TLS scheduler,
	// and repeatedly race a background producer thread saturating a 2-page ring (`10,000` events -> `.Drain_Sync`)
	// against `recorder_flush` and `recorder_destroy` (`_inflight_actor_calls` + `_cv_actor_idle`) across 8 cycles.
	then.sleipnir_actor_rapid_lifecycle_cycles_complete_without_uaf(
		cycles                       = 8,
		events_per_cycle             = 10000,
		expected_min_flushed_batches = 8,
	)
}

@(test)
spec_paused_worker_still_completes_live_flush_without_deadlock :: proc(t: ^testing.T) {
	_ = scenario(t)

	given.recorder_configured(
		batches_per_file = 1,
		ring_pages       = 3,
		string_copy      = false,
		policy           = .Block,
		backend          = .Dedicated_Thread,
	)
	given.worker_paused(paused = true)

	// Emit 250 HTTP requests (1,000 rows) while background worker is paused, then call `recorder_flush`:
	// `worker_proc` must wake up on `_flush_requested` even while `_worker_paused` is true.
	upon.simulating_http_requests(count = 250, payload_step_bytes = 8)
	upon.flushing_live_recorder(expected_flushed_segments = 1)
	given.worker_paused(paused = false)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.arrow_segment_exists_is(segment_idx = 0, expected_exist = true)
	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 1000)
}

@(test)
spec_direct_span_guard_attr_mutation_and_negative_metric_values_are_preserved :: proc(t: ^testing.T) {
	_ = scenario(t)

	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = false, policy = .Block)

	upon.mutating_span_guard_attr_and_recording_negative_gauge(
		initial_attr       = 100,
		mutated_attr       = 777,
		negative_gauge_val = -12.5,
	)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 2)

	// Row 0: Negative `.Gauge` metric (`value_f64 = -12.5`) emitted inside `guard_lib.mutated_attr_op` (`parent_id = 1`)
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 0,
		expected_kind      = .Gauge,
		expected_status    = .Ok,
		expected_scope     = "guard_lib",
		expected_name      = "temperature_celsius",
		expected_span_id   = 0,
		expected_parent_id = 1,
		expected_attr_i64  = 777,
		expected_value_f64 = -12.5,
	)

	// Row 1: `.Span` where caller mutated `guard.attr_i64` from `100` to `777` before scope exit
	then.telemetry_row_matches(
		segment_idx        = 0,
		batch_idx          = 0,
		row_idx            = 1,
		expected_kind      = .Span,
		expected_status    = .Ok,
		expected_scope     = "guard_lib",
		expected_name      = "mutated_attr_op",
		expected_span_id   = 1,
		expected_parent_id = 0,
		expected_attr_i64  = 777,
	)
}

@(test)
spec_multiple_recorders_on_same_thread_isolate_active_span_stacks :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Interleave spans and events across two independent `Recorder` instances (`rec_a` and `rec_b`)
	// on the same OS thread and verify zero cross-recorder `parent_id` pollution.
	then.multi_recorder_same_thread_spans_are_isolated(
		expected_rec_a_rows = 3,
		expected_rec_b_rows = 3,
	)
}

@(test)
spec_reconfiguring_recorder_in_same_scenario_shuts_down_previous_recorder_cleanly :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Initialize a zero-config recorder first, then re-configure `s.rec` within the same scenario:
	// `driver_configure_recorder` must shut down the prior recorder before calling `recorder_init`.
	given.zero_config_recorder_initialized()
	given.recorder_configured(batches_per_file = 1, ring_pages = 2, string_copy = false, policy = .Block)

	upon.simulating_http_requests(count = 1, payload_step_bytes = 64)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = 4)
}

@(test)
spec_multithreaded_dynamic_strings_with_unsynchronized_arena_are_serialized_safely :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 4 OS threads * 250 iterations * 2 records/iteration (`scoped_span` + `event_emit`) = 2,000 rows
	// backed by an unsynchronized `mem.Arena` with `string_fallback_copy = true`.
	// `Recorder._alloc_mu` and chunked `free_page_strings` must prevent concurrent allocator races.
	then.multithreaded_dynamic_strings_with_unsynchronized_arena_persist(
		thread_count          = 4,
		iterations_per_thread = 250,
		expected_total_rows   = 2000,
	)
}

@(test)
spec_disabling_recorder_wakes_blocked_producers_and_frees_cloned_strings :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 2-page ring (`2 * 4,096 = 8,192` rows capacity) with `string_fallback_copy = true` and `.Block` policy
	given.recorder_configured(batches_per_file = 2, ring_pages = 2, string_copy = true, policy = .Block)

	// Producer emits 10,000 dynamic-string events while worker is paused:
	//   - First 8,192 events fill both ring pages
	//   - Event #8,193 blocks on `_cv_page_free` holding a cloned string pair
	//   - Calling `recorder_set_enabled(r, false)` wakes the blocked producer, frees its cloned strings,
	//     and fast-paths the remaining events as disabled no-ops
	upon.running_blocking_producer_woken_by_disable(total_events = 10000)
	upon.flushing_and_shutting_down_recorder(flush_first = false)

	then.recorder_diagnostics_are(expected_flushed_segments = 1, expected_dropped_records = 1)
	then.drop_backpressure_conserves_total_records(
		expected_total_events   = 8193,
		expected_persisted_rows = 8192,
		expected_dropped_rows   = 1,
	)
}

@(test)
spec_partial_batch_allocation_failure_and_string_offset_overflow_roll_back_cleanly :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Verifies:
	//   1. Mid-batch column OOM inside `build_arrow_batch` frees all previously built `cols[0 ..< built]`
	//   2. 32-bit string offset overflow (`total_bytes > int(max(i32)) - s_len`) in `build_string_column_from_soa`
	//      aborts Pass 1 before copying and rolls back `off_buf` + `cols[0 ..< 7]`
	then.partial_batch_oom_and_string_offset_overflow_roll_back_cleanly(
		expected_flushed_batches = 0,
	)
}

@(test)
spec_sequential_scoped_scenarios_reclaim_stack_slots_beyond_max_depth :: proc(t: ^testing.T) {
	// Run 6 sequential `{ _ = scenario(t) ... }` blocks inside a single `@(test)` procedure
	// (`6 > MAX_SCENARIO_DEPTH :: 4`) to verify `scenario_scope_end` decrements `_scenario_depth`.
	for iter in 0 ..< 6 {
		_ = scenario(t)
		given.recorder_configured(batches_per_file = 1, ring_pages = 2, string_copy = false, policy = .Block)
		upon.simulating_http_requests(count = iter + 1, payload_step_bytes = 32)
		upon.flushing_and_shutting_down_recorder(flush_first = false)
		then.arrow_batch_row_count_is(segment_idx = 0, batch_idx = 0, expected_rows = (iter + 1) * 4)
	}
}

@(test)
spec_live_flush_completes_under_continuous_multithreaded_producer_load :: proc(t: ^testing.T) {
	_ = scenario(t)

	// Spawn 2 continuous producer threads saturating a 2-page ring while the main thread
	// executes 4 live `recorder_flush` calls, verifying bounded-generation flush completion without starvation.
	then.live_flushes_complete_under_continuous_producer_load(
		producer_count = 2,
		flush_calls    = 4,
	)
}


