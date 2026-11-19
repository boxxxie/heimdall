// =============================================================================
// Heimdall Telemetry Throughput & Latency Benchmark Suite
// (`benchmarks/bench_heimdall.odin`)
//
// Measures (via `hugi`):
//   1. `zero_cost`: Zero-cost `nil` & disabled recorder short-circuit overhead (`0` allocs)
//   2. `single_thread`: Single-threaded `#soa` ingestion + Arrow IPC flush (static & dynamic)
//   3. `backpressure`: Saturation backpressure policies (`.Drop` real-time vs `.Block` lossless)
//   4. `mt_scaling`: Multi-threaded producer scaling (1, 2, 4, 8 OS threads -> 1 Skadi worker)
//   5. `sleipnir_vs_thread`: Sleipnir actor backend vs dedicated OS worker thread (256k records)
//   6. `auto_backend`: `.Auto` backend selection vs suboptimal forced overrides (256k records)
//   7. `arrow_io_roundtrip`: End-to-end `.arrow` IPC segment flush & `Skadi` columnar readback
//
// Run via:
//   hugi run benchmarks --runs 5
//   odin check benchmarks -collection:heimdall=./heimdall -collection:skadi=./deps/skadi/skadi -collection:actor=./deps/sleipnir/actor -collection:hugi=../hugi/hugi -vet -strict-style -vet-tabs -disallow-do -custom-attribute:telemetry_schema -vet-packages:heimdall,heimdall_benchmarks
// =============================================================================

package heimdall_benchmarks

import "base:intrinsics"
import "core:fmt"
import "core:os"
import "core:sync"
import "core:thread"
import hugi "hugi:."
import "../heimdall"
import oa "../deps/skadi/skadi"
import actor "../deps/sleipnir/actor"

@(private = "file")
g_bench_seq: u64

@(private = "file")
next_bench_prefix :: proc(buf: []u8) -> string {
	_ = os.make_directory(".tmp")
	seq := intrinsics.atomic_add(&g_bench_seq, 1)
	return fmt.bprintf(buf, ".tmp/bench_heimdall_tmp_%d_%d", sync.current_thread_id(), seq)
}

@(private = "file")
cleanup_arrow_segments :: proc(prefix: string, max_segments := 512) {
	file_buf: [256]u8
	for i in 0 ..< max_segments {
		filename := fmt.bprintf(file_buf[:], "%s_%06d.arrow", prefix, i)
		if !os.exists(filename) {
			if i > 0 {
				break
			}
			continue
		}
		_ = os.remove(filename)
	}
}

@(private = "file")
measure_arrow_segments :: proc(
	prefix: string,
	max_segments := 512,
) -> (
	segment_files: int,
	total_bytes: i64,
) {
	file_buf: [256]u8
	for i in 0 ..< max_segments {
		filename := fmt.bprintf(file_buf[:], "%s_%06d.arrow", prefix, i)
		if !os.exists(filename) {
			if i > 0 {
				break
			}
			continue
		}
		segment_files += 1
		if fi, err := os.stat(filename, context.temp_allocator); err == nil {
			total_bytes += fi.size
		}
	}
	return
}

// Simulated Library A: HTTP Router (4 telemetry records per request)
@(private = "file")
bench_http_request :: #force_inline proc(rec: ^heimdall.Recorder, payload_size: int) {
	heimdall.scoped_span(rec, "http_lib", "handle_request", i64(payload_size))
	heimdall.counter_add(rec, "http_lib", "requests_total", 1.0, i64(payload_size))
	bench_db_query(rec, 42)
}

// Simulated Library B: Database Driver
@(private = "file")
bench_db_query :: #force_inline proc(rec: ^heimdall.Recorder, query_id: i64) {
	heimdall.scoped_span(rec, "db_driver", "execute_query", query_id)
	heimdall.gauge_set(rec, "db_driver", "pool_connections_active", 8.0, query_id)
}

// Simulated 3-Level Call Tree (6 telemetry records per request)
@(private = "file")
bench_leaf_op :: #force_inline proc(rec: ^heimdall.Recorder, scope, name: string, tag: i64) {
	heimdall.scoped_span(rec, scope, name, tag)
}

@(private = "file")
bench_level2_auth :: #force_inline proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "auth", "verify", 201)
}

@(private = "file")
bench_level2_orders :: #force_inline proc(rec: ^heimdall.Recorder) {
	heimdall.scoped_span(rec, "orders", "create", 202)
	bench_leaf_op(rec, "cache", "lookup", 301)
	bench_leaf_op(rec, "db", "insert", 302)
}

@(private = "file")
bench_three_level_call_tree :: #force_inline proc(rec: ^heimdall.Recorder) {
	{
		heimdall.scoped_span(rec, "gateway", "ingress", 100)
		bench_level2_auth(rec)
		bench_level2_orders(rec)
	}
	heimdall.event_emit(rec, "gateway", "after_response", 999, .Ok)
}

// =============================================================================
// 1. Zero-Cost Disabled / `nil` Micro-Benchmarks
// =============================================================================

@(private = "file")
bench_zero_scoped_span_nil :: proc(b: ^hugi.Bencher) {
	nil_rec: ^heimdall.Recorder = nil
	hugi.set_bytes(b, size_of(heimdall.Record))
	hugi.expect_zero_allocs(b)
	i := 0
	for hugi.step(b) {
		r := intrinsics.volatile_load(&nil_rec)
		g := heimdall.scoped_span(r, "http_lib", "handle_request", i64(i))
		_ = hugi.black_box(g.span_id)
		i += 1
	}
}

@(private = "file")
bench_zero_counter_add_nil :: proc(b: ^hugi.Bencher) {
	nil_rec: ^heimdall.Recorder = nil
	hugi.set_bytes(b, size_of(heimdall.Record))
	hugi.expect_zero_allocs(b)
	i := 0
	for hugi.step(b) {
		r := intrinsics.volatile_load(&nil_rec)
		heimdall.counter_add(r, "http_lib", "requests_total", 1.0, i64(i))
		i += 1
	}
}

@(private = "file")
bench_zero_scoped_span_disabled :: proc(b: ^hugi.Bencher) {
	disabled_rec: heimdall.Recorder
	disabled_rec.enabled = false
	disabled_ptr: ^heimdall.Recorder = &disabled_rec
	hugi.set_bytes(b, size_of(heimdall.Record))
	hugi.expect_zero_allocs(b)
	i := 0
	for hugi.step(b) {
		r := intrinsics.volatile_load(&disabled_ptr)
		g := heimdall.scoped_span(r, "http_lib", "handle_request", i64(i))
		_ = hugi.black_box(g.span_id)
		i += 1
	}
}

@(private = "file")
bench_zero_nested_http_db_nil :: proc(b: ^hugi.Bencher) {
	nil_rec: ^heimdall.Recorder = nil
	hugi.set_items(b, 4)
	hugi.set_bytes(b, 4 * size_of(heimdall.Record))
	hugi.expect_zero_allocs(b)
	i := 0
	for hugi.step(b) {
		r := intrinsics.volatile_load(&nil_rec)
		bench_http_request(r, i)
		i += 1
	}
}

// =============================================================================
// Shared Ingestion Helper
// =============================================================================

@(private = "file")
run_ingestion_phase :: proc(
	p:             ^hugi.Pipeline,
	phase_name:    string,
	total_recs:    int,
	cfg:           heimdall.Config,
	body:          proc(rec: ^heimdall.Recorder),
	measure_flush: bool = true,
) {
	prefix_buf: [128]u8
	prefix := next_bench_prefix(prefix_buf[:])
	defer cleanup_arrow_segments(prefix)

	run_cfg := cfg
	run_cfg.file_prefix = prefix

	rec: heimdall.Recorder
	init_err := heimdall.recorder_init(&rec, run_cfg)
	assert(init_err == nil)
	defer heimdall.recorder_destroy(&rec)

	ring_bytes := u64(rec.cfg.ring_page_count * heimdall.BATCH_CAP * size_of(heimdall.Record))
	if rec.cfg.string_fallback_copy {
		ring_bytes += u64(rec.cfg.ring_page_count * heimdall.PAGE_STRING_SLAB_CAP)
	}

	{
		ph := hugi.scoped_phase(
			p,
			phase_name,
			ops = total_recs,
			bytes_per_op = size_of(heimdall.Record),
		)
		body(&rec)
		if measure_flush {
			heimdall.recorder_flush(&rec)
		}
		flushed, dropped := heimdall.recorder_diagnostics(&rec)
		hugi.phase_set_footprint(ph, ring_bytes)
		hugi.phase_metric(ph, f64(flushed), "segments")
		if dropped > 0 {
			hugi.phase_metric(ph, f64(dropped), "dropped")
		}
	}
}

// =============================================================================
// 2. Single-Threaded Hot-Path Ingestion + Background Arrow Serialization
// =============================================================================

@(private = "file")
pipeline_single_thread :: proc(p: ^hugi.Pipeline) {
	TOTAL_RECS :: 262_144 // 64 pages * 4,096 rows = 4 segments of 16 batches
	REQ_COUNT  :: TOTAL_RECS / 4
	TREE_ITERS :: 40_000
	TREE_RECS  :: TREE_ITERS * 6
	DYN_RECS   :: 65_536

	base_cfg := heimdall.Config{
		batches_per_file = 16,
		ring_page_count  = 8,
		policy           = .Block,
	}

	run_ingestion_phase(
		p,
		"event_emit_block",
		TOTAL_RECS,
		base_cfg,
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_RECS {
				heimdall.event_emit(rec, "net_lib", "packet_rx", i64(i), .Ok)
			}
		},
	)

	run_ingestion_phase(
		p,
		"counter_add_block",
		TOTAL_RECS,
		base_cfg,
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_RECS {
				heimdall.counter_add(rec, "http_lib", "bytes_sent", 512.0, i64(i))
			}
		},
	)

	run_ingestion_phase(
		p,
		"scoped_span_raii",
		TOTAL_RECS,
		base_cfg,
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_RECS {
				heimdall.scoped_span(rec, "codec_lib", "decode_frame", i64(i))
			}
		},
	)

	run_ingestion_phase(
		p,
		"http_db_tree_2level",
		TOTAL_RECS,
		base_cfg,
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< REQ_COUNT {
				bench_http_request(rec, i * 64)
			}
		},
	)

	run_ingestion_phase(
		p,
		"call_tree_3level",
		TREE_RECS,
		base_cfg,
		proc(rec: ^heimdall.Recorder) {
			for _ in 0 ..< TREE_ITERS {
				bench_three_level_call_tree(rec)
			}
		},
	)

	dyn_cfg := base_cfg
	dyn_cfg.string_fallback_copy = true
	run_ingestion_phase(
		p,
		"event_emit_dynamic_copy",
		DYN_RECS,
		dyn_cfg,
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< DYN_RECS {
				heimdall.event_emit(rec, "dynamic_worker", "dynamic_op", i64(i), .Ok)
			}
		},
	)
}

// =============================================================================
// 3. Backpressure Policy Pipelines (`.Drop` Real-Time vs `.Block` Lossless)
// =============================================================================

@(private = "file")
pipeline_backpressure :: proc(p: ^hugi.Pipeline) {
	BURST_RECS :: 500_000

	run_ingestion_phase(
		p,
		"drop_500k_burst",
		BURST_RECS,
		heimdall.Config{
			batches_per_file = 16,
			ring_page_count  = 4,
			policy           = .Drop,
		},
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< BURST_RECS {
				heimdall.event_emit(rec, "audio_dsp", "process_buffer", i64(i), .Ok)
			}
		},
		measure_flush = false,
	)

	run_ingestion_phase(
		p,
		"block_500k_burst",
		BURST_RECS,
		heimdall.Config{
			batches_per_file = 16,
			ring_page_count  = 8,
			policy           = .Block,
		},
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< BURST_RECS {
				heimdall.event_emit(rec, "ledger_svc", "commit_tx", i64(i), .Ok)
			}
		},
	)
}

// =============================================================================
// 4. Multi-Threaded Producer Scaling (1, 2, 4, 8 OS Producer Threads)
// =============================================================================

@(private = "file")
MT_Producer_Args :: struct {
	rec:        ^heimdall.Recorder,
	requests:   int,
	worker_idx: int,
}

@(private = "file")
mt_producer_worker :: proc(args: ^MT_Producer_Args) {
	for i in 0 ..< args.requests {
		bench_http_request(args.rec, args.worker_idx * 10_000 + i)
	}
}

@(private = "file")
run_mt_scaling_phase :: proc(p: ^hugi.Pipeline, phase_name: string, thread_count: int) {
	TOTAL_REQUESTS :: 64_000 // 64,000 requests * 4 records/req = 256,000 records per run

	prefix_buf: [128]u8
	prefix := next_bench_prefix(prefix_buf[:])
	defer cleanup_arrow_segments(prefix)

	rec: heimdall.Recorder
	init_err := heimdall.recorder_init(&rec, heimdall.Config{
		file_prefix      = prefix,
		batches_per_file = 16,
		ring_page_count  = 16,
		policy           = .Block,
	})
	assert(init_err == nil)
	defer heimdall.recorder_destroy(&rec)

	reqs_per_thread := TOTAL_REQUESTS / thread_count
	total_records   := reqs_per_thread * thread_count * 4
	ring_bytes      := u64(rec.cfg.ring_page_count * heimdall.BATCH_CAP * size_of(heimdall.Record))

	threads := make([]^thread.Thread, thread_count)
	defer delete(threads)
	args := make([]MT_Producer_Args, thread_count)
	defer delete(args)

	{
		ph := hugi.scoped_phase(
			p,
			phase_name,
			ops = total_records,
			bytes_per_op = size_of(heimdall.Record),
		)
		for w in 0 ..< thread_count {
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

		flushed, _ := heimdall.recorder_diagnostics(&rec)
		hugi.phase_set_footprint(ph, ring_bytes)
		hugi.phase_metric(ph, f64(flushed), "segments")
	}
}

@(private = "file")
pipeline_mt_scaling :: proc(p: ^hugi.Pipeline) {
	run_mt_scaling_phase(p, "threads_1", 1)
	run_mt_scaling_phase(p, "threads_2", 2)
	run_mt_scaling_phase(p, "threads_4", 4)
	run_mt_scaling_phase(p, "threads_8", 8)
}

// =============================================================================
// 5. Sleipnir Actor Backend vs Dedicated Worker Thread (256,000 Records)
// =============================================================================

@(private = "file")
Bench_Actor_State :: struct {
	rec:       ^heimdall.Recorder,
	actor_idx: int,
}

@(private = "file")
Bench_Actor_Batch_Msg :: struct {
	batch_requests: int,
	batch_offset:   int,
}

@(private = "file")
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

@(private = "file")
run_sleipnir_shared_actors_phase :: proc(
	p:          ^hugi.Pipeline,
	phase_name: string,
	ring_pages: int,
) {
	TOTAL_REQS     :: 64_000
	TOTAL_RECS     :: TOTAL_REQS * 4
	ACTORS_COUNT   :: 4
	MSGS_PER_ACTOR :: 64
	REQS_PER_MSG   :: TOTAL_REQS / (ACTORS_COUNT * MSGS_PER_ACTOR)

	prefix_buf: [128]u8
	prefix := next_bench_prefix(prefix_buf[:])
	defer cleanup_arrow_segments(prefix)

	sched: actor.Scheduler
	sched_err := actor.scheduler_init_workers(&sched, 4, true)
	assert(sched_err == nil)
	actor.scheduler_start(&sched)
	defer actor.scheduler_stop_and_destroy(&sched)

	rec: heimdall.Recorder
	init_err := heimdall.recorder_init(&rec, heimdall.Config{
		file_prefix      = prefix,
		batches_per_file = 16,
		ring_page_count  = ring_pages,
		policy           = .Block,
		backend          = .Sleipnir_Actor,
		scheduler        = &sched,
	})
	assert(init_err == nil)
	defer heimdall.recorder_destroy(&rec)

	actors: [ACTORS_COUNT]^actor.Actor(Bench_Actor_State, Bench_Actor_Batch_Msg)
	defer {
		for a in actors {
			if a != nil {
				actor.destroy(a)
			}
		}
	}
	for a_idx in 0 ..< ACTORS_COUNT {
		a, spawn_err := actor.spawn(
			&sched,
			Bench_Actor_State{rec = &rec, actor_idx = a_idx + 1},
			bench_actor_receive,
			u32(a_idx + 1),
		)
		assert(spawn_err == nil)
		actors[a_idx] = a
	}

	ring_bytes := u64(rec.cfg.ring_page_count * heimdall.BATCH_CAP * size_of(heimdall.Record))
	{
		ph := hugi.scoped_phase(
			p,
			phase_name,
			ops = TOTAL_RECS,
			bytes_per_op = size_of(heimdall.Record),
		)
		intrinsics.atomic_store_explicit(
			&sched.pending_tokens,
			i64(ACTORS_COUNT * MSGS_PER_ACTOR),
			.Release,
		)
		for m in 0 ..< MSGS_PER_ACTOR {
			for a_idx in 0 ..< ACTORS_COUNT {
				actor.send(&sched, actors[a_idx], Bench_Actor_Batch_Msg{
					batch_requests = REQS_PER_MSG,
					batch_offset   = m * REQS_PER_MSG,
				}, a_idx)
			}
		}
		actor.scheduler_wait_tokens(&sched)
		actor.scheduler_wait_idle(&sched)
		heimdall.recorder_destroy(&rec)

		flushed, _ := heimdall.recorder_diagnostics(&rec)
		hugi.phase_set_footprint(ph, ring_bytes)
		hugi.phase_metric(ph, f64(flushed), "segments")
	}
}

@(private = "file")
pipeline_sleipnir_vs_thread :: proc(p: ^hugi.Pipeline) {
	TOTAL_REQS :: 64_000 // 64,000 HTTP requests * 4 records/req = 256,000 records
	TOTAL_RECS :: TOTAL_REQS * 4

	run_ingestion_phase(
		p,
		"dedicated_thread_1p_1w",
		TOTAL_RECS,
		heimdall.Config{
			batches_per_file = 16,
			ring_page_count  = 16,
			policy           = .Block,
			backend          = .Dedicated_Thread,
		},
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_REQS {
				bench_http_request(rec, i)
			}
		},
	)

	run_ingestion_phase(
		p,
		"sleipnir_actor_standalone_1w",
		TOTAL_RECS,
		heimdall.Config{
			batches_per_file = 16,
			ring_page_count  = 16,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
			scheduler        = nil,
		},
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_REQS {
				bench_http_request(rec, i)
			}
		},
	)

	run_sleipnir_shared_actors_phase(p, "sleipnir_actor_shared_4w_4actors", 16)

	run_ingestion_phase(
		p,
		"sleipnir_actor_flush_sync_x8",
		TOTAL_RECS,
		heimdall.Config{
			batches_per_file = 8,
			ring_page_count  = 8,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
			scheduler        = nil,
		},
		proc(rec: ^heimdall.Recorder) {
			CHUNK :: TOTAL_REQS / 8
			for c in 0 ..< 8 {
				base := c * CHUNK
				for i in 0 ..< CHUNK {
					bench_http_request(rec, base + i)
				}
				heimdall.recorder_flush(rec)
			}
		},
	)

	run_ingestion_phase(
		p,
		"sleipnir_actor_standalone_2page_block",
		TOTAL_RECS,
		heimdall.Config{
			batches_per_file = 16,
			ring_page_count  = 2,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
			scheduler        = nil,
		},
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_REQS {
				bench_http_request(rec, i)
			}
		},
	)

	run_sleipnir_shared_actors_phase(p, "sleipnir_actor_shared_2page_block", 2)

	// 5g. `.Sleipnir_Actor` rapid lifecycle churn x16 on shared 4-worker scheduler
	{
		CYCLES         :: 16
		REQS_PER_CYCLE :: TOTAL_REQS / CYCLES

		sched: actor.Scheduler
		sched_err := actor.scheduler_init_workers(&sched, 4, true)
		assert(sched_err == nil)
		actor.scheduler_start(&sched)
		defer actor.scheduler_stop_and_destroy(&sched)

		prefix_bufs: [CYCLES][128]u8
		prefixes: [CYCLES]string
		for c in 0 ..< CYCLES {
			prefixes[c] = next_bench_prefix(prefix_bufs[c][:])
		}
		defer {
			for c in 0 ..< CYCLES {
				cleanup_arrow_segments(prefixes[c])
			}
		}

		ring_bytes := u64(4 * heimdall.BATCH_CAP * size_of(heimdall.Record))
		total_flushed: u64
		{
			ph := hugi.scoped_phase(
				p,
				"sleipnir_actor_lifecycle_shutdown_x16",
				ops = TOTAL_RECS,
				bytes_per_op = size_of(heimdall.Record),
			)
			for c in 0 ..< CYCLES {
				rec: heimdall.Recorder
				init_err := heimdall.recorder_init(&rec, heimdall.Config{
					file_prefix      = prefixes[c],
					batches_per_file = 4,
					ring_page_count  = 4,
					policy           = .Block,
					backend          = .Sleipnir_Actor,
					scheduler        = &sched,
				})
				assert(init_err == nil)
				for i in 0 ..< REQS_PER_CYCLE {
					bench_http_request(&rec, c * REQS_PER_CYCLE + i)
				}
				heimdall.recorder_flush(&rec)
				heimdall.recorder_destroy(&rec)
				flushed, _ := heimdall.recorder_diagnostics(&rec)
				total_flushed += flushed
			}
			hugi.phase_set_footprint(ph, ring_bytes)
			hugi.phase_metric(ph, f64(total_flushed), "segments")
		}
	}
}

// =============================================================================
// 6. Automatic Backend Selection (`.Auto` vs Suboptimal Forced Overrides)
// =============================================================================

@(private = "file")
Auto_Actor_Churn_State :: struct {
	prefixes: ^[64]string,
	mode:     heimdall.Backend_Mode,
}

@(private = "file")
Auto_Actor_Churn_Reply :: struct {
	flushed:          u64,
	dropped:          u64,
	resolved_backend: heimdall.Backend_Mode,
}

@(private = "file")
auto_actor_churn_receive :: proc(
	ctx:   ^actor.Context,
	state: ^Auto_Actor_Churn_State,
	msg:   ^int,
) {
	reqs_per_cycle := msg^
	var_flushed: u64
	var_dropped: u64
	resolved: heimdall.Backend_Mode
	for c in 0 ..< 64 {
		rec: heimdall.Recorder
		init_err := heimdall.recorder_init(&rec, heimdall.Config{
			file_prefix      = state.prefixes[c],
			batches_per_file = 1,
			ring_page_count  = 2,
			policy           = .Block,
			backend          = state.mode,
		})
		assert(init_err == nil)
		resolved = rec.cfg.backend
		for i in 0 ..< reqs_per_cycle {
			bench_http_request(&rec, c * reqs_per_cycle + i)
		}
		heimdall.recorder_flush(&rec)
		heimdall.recorder_destroy(&rec)
		flushed, dropped := heimdall.recorder_diagnostics(&rec)
		var_flushed += flushed
		var_dropped += dropped
	}
	actor.reply(ctx, Auto_Actor_Churn_Reply{
		flushed          = var_flushed,
		dropped          = var_dropped,
		resolved_backend = resolved,
	})
}

@(private = "file")
Auto_1W_Producer_State :: struct {
	prefix: string,
	mode:   heimdall.Backend_Mode,
}

@(private = "file")
auto_1w_producer_receive :: proc(
	ctx:   ^actor.Context,
	state: ^Auto_1W_Producer_State,
	msg:   ^int,
) {
	total_reqs := msg^
	rec: heimdall.Recorder
	init_err := heimdall.recorder_init(&rec, heimdall.Config{
		file_prefix      = state.prefix,
		batches_per_file = 4,
		ring_page_count  = 8,
		policy           = .Block,
		backend          = state.mode,
	})
	assert(init_err == nil)
	resolved := rec.cfg.backend

	acc: u64 = 0xcbf29ce484222325
	for i in 0 ..< total_reqs {
		// Simulate lightweight per-request actor business logic (~150 ns CPU work)
		// alongside telemetry emission so `.Dedicated_Thread` can overlap disk I/O
		// in parallel while forced `.Sleipnir_Actor` on a 1-worker scheduler serializes both.
		h := acc ~ u64(i)
		for _ in 0 ..< 48 {
			h = (h ~ (h >> 30)) * 0xbf58476d1ce4e5b9
		}
		acc = h
		bench_http_request(&rec, int(h & 0x7fff_ffff))
	}
	_ = hugi.black_box(acc)
	heimdall.recorder_flush(&rec)
	heimdall.recorder_destroy(&rec)
	flushed, dropped := heimdall.recorder_diagnostics(&rec)
	actor.reply(ctx, Auto_Actor_Churn_Reply{
		flushed          = flushed,
		dropped          = dropped,
		resolved_backend = resolved,
	})
}

@(private = "file")
run_auto_churn_phase :: proc(
	p:          ^hugi.Pipeline,
	phase_name: string,
	mode:       heimdall.Backend_Mode,
) {
	TOTAL_REQS     :: 64_000
	TOTAL_RECS     :: TOTAL_REQS * 4
	CYCLES         :: 64
	REQS_PER_CYCLE :: TOTAL_REQS / CYCLES

	sched: actor.Scheduler
	sched_err := actor.scheduler_init_workers(&sched, 4, true)
	assert(sched_err == nil)
	actor.scheduler_start(&sched)
	defer actor.scheduler_stop_and_destroy(&sched)

	prefix_bufs: [CYCLES][128]u8
	prefixes: [CYCLES]string
	for c in 0 ..< CYCLES {
		prefixes[c] = next_bench_prefix(prefix_bufs[c][:])
	}
	defer {
		for c in 0 ..< CYCLES {
			cleanup_arrow_segments(prefixes[c])
		}
	}

	churn_actor, spawn_err := actor.spawn(
		&sched,
		Auto_Actor_Churn_State{
			prefixes = &prefixes,
			mode     = mode,
		},
		auto_actor_churn_receive,
		u32(800 + int(mode)),
	)
	assert(spawn_err == nil)
	defer actor.destroy(churn_actor)

	ring_bytes := u64(2 * heimdall.BATCH_CAP * size_of(heimdall.Record))
	{
		ph := hugi.scoped_phase(
			p,
			phase_name,
			ops = TOTAL_RECS,
			bytes_per_op = size_of(heimdall.Record),
		)
		rep := actor.call(&sched, churn_actor, int(REQS_PER_CYCLE), Auto_Actor_Churn_Reply)
		if mode == .Auto {
			assert(rep.resolved_backend == .Sleipnir_Actor)
		}
		hugi.phase_set_footprint(ph, ring_bytes)
		hugi.phase_metric(ph, f64(rep.flushed), "segments")
	}
}

@(private = "file")
run_auto_1w_phase :: proc(
	p:          ^hugi.Pipeline,
	phase_name: string,
	mode:       heimdall.Backend_Mode,
) {
	TOTAL_REQS :: 64_000
	TOTAL_RECS :: TOTAL_REQS * 4

	prefix_buf: [128]u8
	prefix := next_bench_prefix(prefix_buf[:])
	defer cleanup_arrow_segments(prefix)

	sched: actor.Scheduler
	sched_err := actor.scheduler_init_workers(&sched, 1, false)
	assert(sched_err == nil)
	actor.scheduler_start(&sched)
	defer actor.scheduler_stop_and_destroy(&sched)

	prod_actor, spawn_err := actor.spawn(
		&sched,
		Auto_1W_Producer_State{prefix = prefix, mode = mode},
		auto_1w_producer_receive,
		u32(810 + int(mode)),
	)
	assert(spawn_err == nil)
	defer actor.destroy(prod_actor)

	ring_bytes := u64(8 * heimdall.BATCH_CAP * size_of(heimdall.Record))
	{
		ph := hugi.scoped_phase(
			p,
			phase_name,
			ops = TOTAL_RECS,
			bytes_per_op = size_of(heimdall.Record),
		)
		rep := actor.call(&sched, prod_actor, int(TOTAL_REQS), Auto_Actor_Churn_Reply)
		if mode == .Auto {
			assert(rep.resolved_backend == .Dedicated_Thread)
		}
		hugi.phase_set_footprint(ph, ring_bytes)
		hugi.phase_metric(ph, f64(rep.flushed), "segments")
	}
}

@(private = "file")
pipeline_auto_backend :: proc(p: ^hugi.Pipeline) {
	TOTAL_REQS :: 64_000
	TOTAL_RECS :: TOTAL_REQS * 4

	// Regime 1: 4-worker actor lifecycle churn (x64)
	run_auto_churn_phase(p, "forced_dedicated_thread_in_4w_actor_x64", .Dedicated_Thread)
	run_auto_churn_phase(p, "auto_in_4w_actor_x64", .Auto)

	// Regime 2: 1-worker actor scheduler under `.Block` policy
	run_auto_1w_phase(p, "forced_sleipnir_actor_1w_block", .Sleipnir_Actor)
	run_auto_1w_phase(p, "auto_on_1w_sched_block", .Auto)

	// Regime 3: Plain OS thread producer
	run_ingestion_phase(
		p,
		"forced_sleipnir_actor_os_thread",
		TOTAL_RECS,
		heimdall.Config{
			batches_per_file = 4,
			ring_page_count  = 8,
			policy           = .Block,
			backend          = .Sleipnir_Actor,
		},
		proc(rec: ^heimdall.Recorder) {
			for i in 0 ..< TOTAL_REQS {
				bench_http_request(rec, i)
			}
		},
	)
	run_ingestion_phase(
		p,
		"auto_on_os_thread",
		TOTAL_RECS,
		heimdall.Config{
			batches_per_file = 4,
			ring_page_count  = 8,
			policy           = .Block,
			backend          = .Auto,
		},
		proc(rec: ^heimdall.Recorder) {
			assert(rec.cfg.backend == .Dedicated_Thread)
			for i in 0 ..< TOTAL_REQS {
				bench_http_request(rec, i)
			}
		},
	)
}

// =============================================================================
// 7. End-to-End Arrow IPC Disk Write & Columnar Readback Pipeline
// =============================================================================

@(private = "file")
pipeline_arrow_io_roundtrip :: proc(p: ^hugi.Pipeline) {
	SEGMENTS     :: 4
	BATCHES_FILE :: 16
	TOTAL_ROWS   :: SEGMENTS * BATCHES_FILE * heimdall.BATCH_CAP
	TOTAL_REQS   :: TOTAL_ROWS / 4

	prefix_buf: [128]u8
	prefix := next_bench_prefix(prefix_buf[:])
	defer cleanup_arrow_segments(prefix)

	rec: heimdall.Recorder
	init_err := heimdall.recorder_init(&rec, heimdall.Config{
		file_prefix      = prefix,
		batches_per_file = BATCHES_FILE,
		ring_page_count  = 16,
		policy           = .Block,
	})
	assert(init_err == nil)
	defer heimdall.recorder_destroy(&rec)

	ring_bytes := u64(rec.cfg.ring_page_count * heimdall.BATCH_CAP * size_of(heimdall.Record))
	{
		ph := hugi.scoped_phase(
			p,
			"arrow_write_262k",
			ops = TOTAL_ROWS,
			bytes_per_op = size_of(heimdall.Record),
		)
		for i in 0 ..< TOTAL_REQS {
			bench_http_request(&rec, i)
		}
		heimdall.recorder_destroy(&rec)
		flushed, _ := heimdall.recorder_diagnostics(&rec)
		hugi.phase_set_footprint(ph, ring_bytes)
		hugi.phase_metric(ph, f64(flushed), "segments")
	}

	segment_files, total_file_bytes := measure_arrow_segments(prefix, SEGMENTS)
	assert(segment_files == SEGMENTS && total_file_bytes > 0)
	bytes_per_row_disk := u64(total_file_bytes) / u64(TOTAL_ROWS)

	{
		ph := hugi.scoped_phase(
			p,
			"arrow_read_262k",
			ops = TOTAL_ROWS,
			bytes_per_op = max(bytes_per_row_disk, 1),
		)
		rows_read := 0
		checksum: i64 = 0
		file_buf: [256]u8
		for seg_idx in 0 ..< SEGMENTS {
			filename := fmt.bprintf(file_buf[:], "%s_%06d.arrow", prefix, seg_idx)
			sc, batches, ok := oa.ipc_read_file(filename)
			if !ok {
				continue
			}
			for &batch in batches {
				rows_read += batch.length
				reader, r_ok := heimdall.record_batch_reader_resolve(&batch)
				if r_ok && batch.length > 0 {
					checksum += reader.attr_i64_data[0]
					checksum += reader.attr_i64_data[batch.length - 1]
				}
			}
			for &batch in batches {
				oa.record_batch_free(&batch)
			}
			delete(batches)
			oa.schema_free(sc)
			free(sc)
		}
		assert(rows_read == TOTAL_ROWS)
		_ = hugi.black_box(checksum)
		hugi.phase_set_footprint(ph, u64(total_file_bytes))
		hugi.phase_metric(ph, f64(total_file_bytes) / f64(max(rows_read, 1)), "B/row")
	}
}

// =============================================================================
// Suite Entrypoint
// =============================================================================

main :: proc() {
	ok := false
	{
		suite := hugi.suite_create("heimdall")
		defer hugi.suite_destroy(&suite)

		zero_group := hugi.group_add(&suite, "zero_cost")
		hugi.group_bench(zero_group, "scoped_span_nil", bench_zero_scoped_span_nil, zero_alloc = true)
		hugi.group_bench(zero_group, "counter_add_nil", bench_zero_counter_add_nil, zero_alloc = true)
		hugi.group_bench(zero_group, "scoped_span_disabled", bench_zero_scoped_span_disabled, zero_alloc = true)
		hugi.group_bench(zero_group, "nested_http_db_nil", bench_zero_nested_http_db_nil, zero_alloc = true)

		hugi.group_pipeline(&suite, "single_thread", pipeline_single_thread)
		hugi.group_pipeline(&suite, "backpressure", pipeline_backpressure)
		hugi.group_pipeline(&suite, "mt_scaling", pipeline_mt_scaling)
		hugi.group_pipeline(&suite, "sleipnir_vs_thread", pipeline_sleipnir_vs_thread)
		hugi.group_pipeline(&suite, "auto_backend", pipeline_auto_backend)
		hugi.group_pipeline(&suite, "arrow_io_roundtrip", pipeline_arrow_io_roundtrip)

		ok = hugi.suite_run(&suite)
	}

	free_all(context.temp_allocator)
	if !ok {
		os.exit(1)
	}
}
