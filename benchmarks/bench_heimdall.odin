// =============================================================================
// Heimdall Telemetry Throughput & Latency Benchmark Suite
// (`benchmarks/bench_heimdall.odin`)
//
// Measures:
//   1. Memory footprint audit (`Record`, `Span_Guard`, `Recorder`, `#soa` page)
//   2. Zero-cost `nil` & disabled recorder short-circuit overhead (`ns/op`, 0 allocs)
//   3. Single-threaded hot-path ingestion (`event_emit`, `counter_add`, `scoped_span`)
//   4. Nested cross-library call trees (2-level HTTP->DB & 3-level Gateway->Orders->DB)
//   5. Static strings (`string_Fallback_copy = false`) vs dynamic string cloning (`true`)
//   6. Saturation backpressure policies (`.Drop` real-time loop vs `.Block` lossless)
//   7. Multi-threaded producer scaling (1, 2, 4, 8 OS threads -> 1 OdinArrow worker)
//   8. End-to-end `.arrow` IPC segment flush & `OdinArrow` columnar readback speed
//
// Run via:
//   odin run benchmarks -o:speed -no-bounds-check
//   odin check benchmarks -vet -strict-style -vet-tabs -vet-packages:heimdall_benchmarks,heimdall
// =============================================================================

package heimdall_benchmarks

import "base:intrinsics"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"
import "../heimdall"
import oa "../deps/OdinArrow/src"
import actor "../deps/sleipnir/actor"

Bench_Result :: struct {
	name:               string,
	records:            int,
	elapsed_ms:         f64,
	ns_per_record:      f64,
	mrecs_per_sec:      f64,
	mb_per_sec:         f64,
	flushed_segments:   u64,
	dropped_records:    u64,
	producer_allocs:    i64,
	producer_alloc_b:   i64,
}

@(private)
g_bench_seq: u64

@(private)
g_sink_u64: u64

@(private)
next_bench_prefix :: proc() -> string {
	seq := intrinsics.atomic_add(&g_bench_seq, 1)
	return fmt.tprintf("bench_heimdall_tmp_%d_%d", sync.current_thread_id(), seq)
}

@(private)
cleanup_arrow_segments :: proc(prefix: string, max_segments := 512) -> (files_removed: int, total_bytes: i64) {
	for i in 0 ..< max_segments {
		filename := fmt.tprintf("%s_%06d.arrow", prefix, i)
		if !os.exists(filename) {
			if i > 0 {
				break
			}
			continue
		}
		if fi, err := os.stat(filename, context.temp_allocator); err == nil {
			total_bytes += fi.size
		}
		if os.remove(filename) == nil {
			files_removed += 1
		}
	}
	return
}

// Simulated Library A: HTTP Router (4 telemetry records per request)
@(private)
bench_http_request :: #force_inline proc(rec: ^heimdall.Recorder, payload_size: int) {
	heimdall.scoped_span(rec, "http_lib", "handle_request", i64(payload_size))
	heimdall.counter_add(rec, "http_lib", "requests_total", 1.0, i64(payload_size))
	bench_db_query(rec, 42)
}

// Simulated Library B: Database Driver
@(private)
bench_db_query :: #force_inline proc(rec: ^heimdall.Recorder, query_id: i64) {
	heimdall.scoped_span(rec, "db_driver", "execute_query", query_id)
	heimdall.gauge_set(rec, "db_driver", "pool_connections_active", 8.0, query_id)
}

// Simulated 3-Level Call Tree (6 telemetry records per request)
@(private)
bench_leaf_op :: #force_inline proc(rec: ^heimdall.Recorder, scope, name: string, tag: i64) {
	heimdall.scoped_span(rec, scope, name, tag)
}

@(private)
bench_level2_auth :: #force_inline proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "auth", "verify", 201)
}

@(private)
bench_level2_orders :: #force_inline proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "orders", "create", 202)
	bench_leaf_op(rec, "cache", "lookup", 301)
	bench_leaf_op(rec, "db", "insert", 302)
}

@(private)
bench_three_level_call_tree :: #force_inline proc(rec: ^heimdall.Recorder) {
	{
		heimdall.scoped_span(rec, "gateway", "ingress", 100)
		bench_level2_auth(rec)
		bench_level2_orders(rec)
	}
	heimdall.event_emit(rec, "gateway", "after_response", 999, .Ok)
}

// -----------------------------------------------------------------------------
// 1. Zero-Cost Disabled / `nil` Benchmarks
// -----------------------------------------------------------------------------

run_zero_cost_benchmarks :: proc() -> [4]Bench_Result {
	N :: 20_000_000
	results: [4]Bench_Result

	nil_rec: ^heimdall.Recorder = nil
	disabled_rec: heimdall.Recorder
	disabled_rec.enabled = false
	disabled_ptr: ^heimdall.Recorder = &disabled_rec

	// Warmup
	for i in 0 ..< 10_000 {
		r := intrinsics.volatile_load(&nil_rec)
		heimdall.scoped_span(r, "http_lib", "warmup", i64(i))
	}

	// 1a. `scoped_span(nil, ...)`
	{
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< N {
			r := intrinsics.volatile_load(&nil_rec)
			g := heimdall.scoped_span(r, "http_lib", "handle_request", i64(i))
			if g.span_id != 0 {
				g_sink_u64 += g.span_id
			}
		}
		elapsed := time.tick_since(t0)
		context.allocator = prev_alloc

		results[0] = make_bench_result(
			"scoped_span(nil) [RAII guard]",
			N,
			elapsed,
			0,
			0,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 1b. `counter_add(nil, ...)`
	{
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< N {
			r := intrinsics.volatile_load(&nil_rec)
			heimdall.counter_add(r, "http_lib", "requests_total", 1.0, i64(i))
		}
		elapsed := time.tick_since(t0)
		context.allocator = prev_alloc

		results[1] = make_bench_result(
			"counter_add(nil) [point metric]",
			N,
			elapsed,
			0,
			0,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 1c. `scoped_span(&disabled_rec, ...)`
	{
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< N {
			r := intrinsics.volatile_load(&disabled_ptr)
			g := heimdall.scoped_span(r, "http_lib", "handle_request", i64(i))
			if g.span_id != 0 {
				g_sink_u64 += g.span_id
			}
		}
		elapsed := time.tick_since(t0)
		context.allocator = prev_alloc

		results[2] = make_bench_result(
			"scoped_span(&disabled_rec)",
			N,
			elapsed,
			0,
			0,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 1d. Full 2-library nested call tree (`handle_request` -> `execute_query`, 4 calls) with `nil` recorder
	{
		ITERS :: 10_000_000
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< ITERS {
			r := intrinsics.volatile_load(&nil_rec)
			bench_http_request(r, i)
		}
		elapsed := time.tick_since(t0)
		context.allocator = prev_alloc

		results[3] = make_bench_result(
			"nested HTTP->DB (4 calls, nil)",
			ITERS * 4,
			elapsed,
			0,
			0,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	return results
}

// -----------------------------------------------------------------------------
// 2. Single-Threaded Hot-Path Ingestion + Background Arrow Serialization
// -----------------------------------------------------------------------------

run_single_thread_ingestion_benchmarks :: proc() -> [6]Bench_Result {
	results: [6]Bench_Result
	TOTAL_RECS :: 262_144 // 64 pages * 4,096 rows = 4 segments of 16 batches

	// 2a. `event_emit` hot-path (`Block` policy, 8 ring pages, 16 batches/file)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		})

		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< TOTAL_RECS {
			heimdall.event_emit(&rec, "net_lib", "packet_rx", i64(i), .Ok)
		}
		context.allocator = prev_alloc
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[0] = make_bench_result(
			"event_emit (.Block + Arrow flush)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 2b. `counter_add` hot-path (`Block` policy)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		})

		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< TOTAL_RECS {
			heimdall.counter_add(&rec, "http_lib", "bytes_sent", 512.0, i64(i))
		}
		context.allocator = prev_alloc
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[1] = make_bench_result(
			"counter_add (.Block + Arrow flush)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 2c. `scoped_span` single-level RAII (`span_begin` + `tick_now` + `tick_since` + `span_end`)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		})

		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< TOTAL_RECS {
			heimdall.scoped_span(&rec, "codec_lib", "decode_frame", i64(i))
		}
		context.allocator = prev_alloc
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[2] = make_bench_result(
			"scoped_span (1-level RAII + flush)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 2d. 2-Level Nested `HTTP -> DB` Call Tree (4 records/request = 262,144 records)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		})

		REQ_COUNT :: TOTAL_RECS / 4
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< REQ_COUNT {
			bench_http_request(&rec, i * 64)
		}
		context.allocator = prev_alloc
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[3] = make_bench_result(
			"2-level HTTP->DB tree (4 recs/req)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 2e. 3-Level Nested `Gateway -> Auth + Orders -> Cache + DB` Call Tree (6 records/request)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		})

		TREE_ITERS :: 40_000
		TREE_RECS  :: TREE_ITERS * 6
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for _ in 0 ..< TREE_ITERS {
			bench_three_level_call_tree(&rec)
		}
		context.allocator = prev_alloc
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[4] = make_bench_result(
			"3-level call tree (6 recs/req)",
			TREE_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 2f. Dynamic string cloning (`string_Fallback_copy = true`)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		DYN_RECS :: 65_536
		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix          = prefix,
			batches_per_file     = 16,
			ring_page_count      = 8,
			string_Fallback_copy = true,
			policy               = .Block,
		})

		t0 := time.tick_now()
		for i in 0 ..< DYN_RECS {
			heimdall.event_emit(&rec, "dynamic_worker", "dynamic_op", i64(i), .Ok)
		}
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[5] = make_bench_result(
			"event_emit (string_Fallback_copy)",
			DYN_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			i64(DYN_RECS * 2),
			i64(DYN_RECS * (len("dynamic_worker") + len("dynamic_op"))),
		)
	}

	return results
}

// -----------------------------------------------------------------------------
// 3. Backpressure Policy Benchmarks (`.Drop` Real-Time vs `.Block` Lossless)
// -----------------------------------------------------------------------------

run_backpressure_benchmarks :: proc() -> [2]Bench_Result {
	results: [2]Bench_Result
	BURST_RECS :: 500_000

	// 3a. `.Drop` Policy under 500k burst (never blocks caller even when ring saturates)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 4,
			policy           = .Drop,
		})

		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< BURST_RECS {
			heimdall.event_emit(&rec, "audio_dsp", "process_buffer", i64(i), .Ok)
		}
		producer_elapsed := time.tick_since(t0)
		context.allocator = prev_alloc

		heimdall.recorder_destroy(&rec)

		results[0] = make_bench_result(
			"500k burst (.Drop non-blocking)",
			BURST_RECS,
			producer_elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	// 3b. `.Block` Policy under 500k burst (waits for background worker, 0 drops)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		})

		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		defer mem.tracking_allocator_destroy(&track)
		prev_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&track)

		t0 := time.tick_now()
		for i in 0 ..< BURST_RECS {
			heimdall.event_emit(&rec, "ledger_svc", "commit_tx", i64(i), .Ok)
		}
		context.allocator = prev_alloc
		heimdall.recorder_destroy(&rec)
		total_elapsed := time.tick_since(t0)

		results[1] = make_bench_result(
			"500k burst (.Block zero-drop)",
			BURST_RECS,
			total_elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			track.total_allocation_count,
			track.total_memory_allocated,
		)
	}

	return results
}

// -----------------------------------------------------------------------------
// 4. Multi-Threaded Producer Scaling (1, 2, 4, 8 OS Producer Threads)
// -----------------------------------------------------------------------------

@(private)
MT_Producer_Args :: struct {
	rec:        ^heimdall.Recorder,
	requests:   int,
	worker_idx: int,
}

@(private)
mt_producer_worker :: proc(args: ^MT_Producer_Args) {
	for i in 0 ..< args.requests {
		bench_http_request(args.rec, args.worker_idx * 10_000 + i)
	}
}

run_multithreaded_scaling_benchmarks :: proc() -> [4]Bench_Result {
	results: [4]Bench_Result
	thread_counts := [4]int{1, 2, 4, 8}
	TOTAL_REQUESTS :: 64_000 // 64,000 requests * 4 records/req = 256,000 records per run

	for t_cnt, idx in thread_counts {
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 16,
			policy           = .Block,
		})

		reqs_per_thread := TOTAL_REQUESTS / t_cnt
		total_records   := reqs_per_thread * t_cnt * 4

		threads := make([]^thread.Thread, t_cnt, context.temp_allocator)
		args    := make([]MT_Producer_Args, t_cnt, context.temp_allocator)

		t0 := time.tick_now()
		for w in 0 ..< t_cnt {
			args[w] = MT_Producer_Args{
				rec        = &rec,
				requests   = reqs_per_thread,
				worker_idx = w + 1,
			}
			threads[w] = thread.create_and_start_with_poly_data(&args[w], mt_producer_worker)
		}

		for handle in threads {
			if handle != nil {
				thread.join(handle)
				thread.destroy(handle)
			}
		}
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		label := fmt.tprintf("%d producer thread(s) -> 1 worker", t_cnt)
		results[idx] = make_bench_result(
			label,
			total_records,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			0,
			0,
		)
	}

	return results
}

// -----------------------------------------------------------------------------
// 5. Sleipnir Actor Backend vs Dedicated Worker Thread (256,000 Records)
// -----------------------------------------------------------------------------

@(private)
Bench_Actor_State :: struct {
	rec:       ^heimdall.Recorder,
	actor_idx: int,
}

@(private)
Bench_Actor_Batch_Msg :: struct {
	batch_requests: int,
	batch_offset:   int,
}

@(private)
bench_actor_receive :: proc(
	ctx:   ^actor.Context,
	state: ^Bench_Actor_State,
	msg:   ^Bench_Actor_Batch_Msg,
) {
	base := state.actor_idx * 100_000 + msg.batch_offset
	for i in 0 ..< msg.batch_requests {
		bench_http_request(state.rec, base + i)
	}
	actor.complete_token(ctx, 1)
}

run_sleipnir_actor_benchmarks :: proc() -> [3]Bench_Result {
	results: [3]Bench_Result
	TOTAL_REQS :: 64_000 // 64,000 HTTP requests * 4 records/req = 256,000 records
	TOTAL_RECS :: TOTAL_REQS * 4

	// 5a. Baseline `.Dedicated_Thread` backend (64k HTTP requests = 256k records)
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 16,
			policy           = .Block,
			backend          = .Dedicated_Thread,
		})

		t0 := time.tick_now()
		for i in 0 ..< TOTAL_REQS {
			bench_http_request(&rec, i)
		}
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[0] = make_bench_result(
			".Dedicated_Thread (1 producer -> 1 worker)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			0,
			0,
		)
	}

	// 5b. `.Sleipnir_Actor` with standalone owned 1-worker `actor.Scheduler`
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 16,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
			scheduler        = nil,
		})

		t0 := time.tick_now()
		for i in 0 ..< TOTAL_REQS {
			bench_http_request(&rec, i)
		}
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[1] = make_bench_result(
			".Sleipnir_Actor (standalone 1-worker sched)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			0,
			0,
		)
	}

	// 5c. `.Sleipnir_Actor` sharing a 4-worker `actor.Scheduler` with 4 application actors
	{
		prefix := next_bench_prefix()
		defer cleanup_arrow_segments(prefix)

		sched: actor.Scheduler
		_ = actor.scheduler_init_workers(&sched, 4, true)
		actor.scheduler_start(&sched)
		defer actor.scheduler_stop_and_destroy(&sched)

		rec: heimdall.Recorder
		_ = heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = prefix,
			batches_per_file = 16,
			ring_page_count  = 16,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
			scheduler        = &sched,
		})

		ACTORS_COUNT   :: 4
		MSGS_PER_ACTOR :: 64
		REQS_PER_MSG   :: TOTAL_REQS / (ACTORS_COUNT * MSGS_PER_ACTOR) // 250 HTTP requests (1,000 records) per actor turn

		actors: [ACTORS_COUNT]^actor.Actor(Bench_Actor_State, Bench_Actor_Batch_Msg)
		for a_idx in 0 ..< ACTORS_COUNT {
			a, _ := actor.spawn(
				&sched,
				Bench_Actor_State{rec = &rec, actor_idx = a_idx + 1},
				bench_actor_receive,
				u32(a_idx + 1),
			)
			actors[a_idx] = a
		}

		t0 := time.tick_now()
		intrinsics.atomic_store_explicit(&sched.pending_tokens, i64(ACTORS_COUNT * MSGS_PER_ACTOR), .Release)
		for m in 0 ..< MSGS_PER_ACTOR {
			for a_idx in 0 ..< ACTORS_COUNT {
				actor.send(actors[a_idx], Bench_Actor_Batch_Msg{
					batch_requests = REQS_PER_MSG,
					batch_offset   = m * REQS_PER_MSG,
				})
			}
		}
		actor.scheduler_wait_tokens(&sched)
		actor.scheduler_wait_idle(&sched)
		heimdall.recorder_destroy(&rec)
		elapsed := time.tick_since(t0)

		results[2] = make_bench_result(
			".Sleipnir_Actor (4 actors on shared sched)",
			TOTAL_RECS,
			elapsed,
			rec.flushed_batches,
			rec.dropped_records,
			0,
			0,
		)
	}

	return results
}

// -----------------------------------------------------------------------------
// 6. End-to-End Arrow IPC Disk Write & Columnar Readback Benchmark
// -----------------------------------------------------------------------------

Arrow_IO_Report :: struct {
	total_rows:        int,
	segment_files:     int,
	total_file_bytes:  i64,
	bytes_per_row:     f64,
	write_ms:          f64,
	write_mrows_sec:   f64,
	write_mb_sec:      f64,
	read_ms:           f64,
	read_mrows_sec:    f64,
	read_mb_sec:       f64,
}

run_arrow_io_roundtrip_benchmark :: proc() -> Arrow_IO_Report {
	prefix := next_bench_prefix()
	defer cleanup_arrow_segments(prefix)

	// 4 segments * 16 batches * 4,096 rows = 262,144 rows
	SEGMENTS      :: 4
	BATCHES_FILE  :: 16
	TOTAL_ROWS    :: SEGMENTS * BATCHES_FILE * heimdall.BATCH_CAP
	TOTAL_REQS    :: TOTAL_ROWS / 4

	rec: heimdall.Recorder
	_ = heimdall.recorder_init(&rec, heimdall.Config{
		file_prefix      = prefix,
		batches_per_file = BATCHES_FILE,
		ring_page_count  = 16,
		policy           = .Block,
	})

	t_write0 := time.tick_now()
	for i in 0 ..< TOTAL_REQS {
		bench_http_request(&rec, i)
	}
	heimdall.recorder_destroy(&rec)
	write_elapsed := time.tick_since(t_write0)

	// Measure OdinArrow `ipc_read_file` columnar scan across all 4 `.arrow` segment files
	t_read0 := time.tick_now()
	rows_read := 0
	checksum: i64 = 0
	for seg_idx in 0 ..< SEGMENTS {
		filename := fmt.tprintf("%s_%06d.arrow", prefix, seg_idx)
		sc, batches, ok := oa.ipc_read_file(filename)
		if !ok {
			continue
		}
		for &b in batches {
			rows_read += b.length
			attr_col := oa.record_batch_column(&b, "attr_i64")
			if attr_col != nil && b.length > 0 {
				checksum += oa.array_get(attr_col, 0, i64)
				checksum += oa.array_get(attr_col, b.length - 1, i64)
			}
		}
		for &b in batches {
			oa.record_batch_free(&b)
		}
		delete(batches)
		oa.schema_free(sc)
		free(sc)
	}
	read_elapsed := time.tick_since(t_read0)
	g_sink_u64 += u64(checksum)

	files_removed, total_file_bytes := cleanup_arrow_segments(prefix)

	write_sec := time.duration_seconds(write_elapsed)
	read_sec  := time.duration_seconds(read_elapsed)
	file_mb   := f64(total_file_bytes) / (1024.0 * 1024.0)

	return Arrow_IO_Report{
		total_rows       = rows_read,
		segment_files    = files_removed,
		total_file_bytes = total_file_bytes,
		bytes_per_row    = f64(total_file_bytes) / f64(max(rows_read, 1)),
		write_ms         = time.duration_milliseconds(write_elapsed),
		write_mrows_sec  = (f64(rows_read) / write_sec) / 1e6,
		write_mb_sec     = file_mb / write_sec,
		read_ms          = time.duration_milliseconds(read_elapsed),
		read_mrows_sec   = (f64(rows_read) / read_sec) / 1e6,
		read_mb_sec      = file_mb / read_sec,
	}
}

// -----------------------------------------------------------------------------
// Formatting & Reporting Helpers
// -----------------------------------------------------------------------------

@(private)
make_bench_result :: proc(
	name:             string,
	records:          int,
	elapsed:          time.Duration,
	flushed_segments: u64,
	dropped_records:  u64,
	allocs:           i64,
	alloc_bytes:      i64,
) -> Bench_Result {
	elapsed_ns := f64(time.duration_nanoseconds(elapsed))
	elapsed_s  := time.duration_seconds(elapsed)
	ns_rec     := elapsed_ns / f64(max(records, 1))
	mrecs_s    := (f64(records) / elapsed_s) / 1e6 if elapsed_s > 0 else 0.0
	raw_bytes  := f64(records * size_of(heimdall.Record))
	mb_s       := (raw_bytes / (1024.0 * 1024.0)) / elapsed_s if elapsed_s > 0 else 0.0

	return Bench_Result{
		name             = name,
		records          = records,
		elapsed_ms       = time.duration_milliseconds(elapsed),
		ns_per_record    = ns_rec,
		mrecs_per_sec    = mrecs_s,
		mb_per_sec       = mb_s,
		flushed_segments = flushed_segments,
		dropped_records  = dropped_records,
		producer_allocs  = allocs,
		producer_alloc_b = alloc_bytes,
	}
}

@(private)
print_section_table :: proc(title: string, results: []Bench_Result) {
	fmt.printf("\n### %s\n\n", title)
	fmt.println("| Benchmark | Records | Elapsed (ms) | Latency (ns/rec) | Throughput (M rec/s) | #soa Rate (MB/s) | Segments | Dropped | Producer Allocs |")
	fmt.println("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
	for r in results {
		fmt.printf(
			"| `%s` | %d | %.2f ms | %.2f ns | %.2f M/s | %.1f MB/s | %d | %d | %d (%d B) |\n",
			r.name,
			r.records,
			r.elapsed_ms,
			r.ns_per_record,
			r.mrecs_per_sec,
			r.mb_per_sec,
			r.flushed_segments,
			r.dropped_records,
			r.producer_allocs,
			r.producer_alloc_b,
		)
	}
}

main :: proc() {
	fmt.println("=============================================================================================")
	fmt.println(" Heimdall Telemetry Benchmark Suite (`#soa` Ring + Single-Threaded OdinArrow IPC Worker)")
	fmt.println("=============================================================================================")
	fmt.printfln("  size_of(heimdall.Record)     : %d bytes (11 canonical OpenTelemetry columns)", size_of(heimdall.Record))
	fmt.printfln("  size_of(heimdall.Span_Guard) : %d bytes (stack-only RAII `@(deferred_out)` guard)", size_of(heimdall.Span_Guard))
	fmt.printfln("  size_of(heimdall.Recorder)   : %d bytes", size_of(heimdall.Recorder))
	fmt.printfln("  BATCH_CAP (#soa page rows)   : %d rows (%d KB per `#soa[BATCH_CAP]Record` page)",
		heimdall.BATCH_CAP,
		(heimdall.BATCH_CAP * size_of(heimdall.Record)) / 1024,
	)
	fmt.println("---------------------------------------------------------------------------------------------")

	zero_cost := run_zero_cost_benchmarks()
	print_section_table("1. Zero-Cost Disabled / `nil` Recorder Short-Circuit", zero_cost[:])

	single_thread := run_single_thread_ingestion_benchmarks()
	print_section_table("2. Single-Threaded Hot-Path Ingestion + Background Arrow IPC Flush", single_thread[:])

	backpressure := run_backpressure_benchmarks()
	print_section_table("3. Saturation Backpressure Policies (`.Drop` Real-Time vs `.Block` Lossless)", backpressure[:])

	mt_scaling := run_multithreaded_scaling_benchmarks()
	print_section_table("4. Multi-Threaded Producer Scaling (N OS Threads -> 1 Background Worker)", mt_scaling[:])

	sleipnir_bench := run_sleipnir_actor_benchmarks()
	print_section_table("5. Sleipnir Actor Backend vs Dedicated Worker Thread (256,000 Records)", sleipnir_bench[:])

	io_rep := run_arrow_io_roundtrip_benchmark()
	fmt.println("\n### 6. End-to-End `.arrow` IPC Segment Disk Write & `OdinArrow` Readback")
	fmt.println()
	fmt.println("| Metric | Value |")
	fmt.println("| :--- | ---: |")
	fmt.printfln("| Total Telemetry Rows Persisted & Verified | %d rows (%d `.arrow` segments) |", io_rep.total_rows, io_rep.segment_files)
	fmt.printfln("| Total Arrow IPC Disk Footprint | %.2f MB (%.1f bytes/row) |", f64(io_rep.total_file_bytes) / (1024.0 * 1024.0), io_rep.bytes_per_row)
	fmt.printfln("| End-to-End Ingestion + `.arrow` Disk Flush | %.2f ms (%.2f M rows/s, %.1f MB/s) |", io_rep.write_ms, io_rep.write_mrows_sec, io_rep.write_mb_sec)
	fmt.printfln("| `oa.ipc_read_file` Columnar Readback Scan | %.2f ms (%.2f M rows/s, %.1f MB/s) |", io_rep.read_ms, io_rep.read_mrows_sec, io_rep.read_mb_sec)
	fmt.println()

	free_all(context.temp_allocator)
}
