# Heimdall Benchmarks

Pure-Odin throughput, latency, scaling, and Arrow IPC I/O benchmark suite for **Heimdall** (`benchmarks/bench_heimdall.odin`), executed and persisted via `hugi`.

## Covered Groups

* **`zero_cost`** — `nil` and disabled `^heimdall.Recorder` short-circuit micro-benchmarks (`scoped_span_nil`, `counter_add_nil`, `scoped_span_disabled`, `nested_http_db_nil`), enforced with `0 B` heap allocations (`zero_alloc = true`).
* **`single_thread`** — Single-threaded `#soa` staging ring ingestion and Arrow IPC segment flush (`event_emit_block`, `counter_add_block`, `scoped_span_raii`, `http_db_tree_2level`, `call_tree_3level`, and `event_emit_dynamic_copy` with `string_fallback_copy = true`).
* **`backpressure`** — 500k burst saturation under `.Drop` (non-blocking real-time loop) vs. `.Block` (lossless zero-drop pipeline).
* **`mt_scaling`** — Multi-threaded producer scaling across 1, 2, 4, and 8 OS producer threads (`threads_1`, `threads_2`, `threads_4`, `threads_8`) feeding a single background Skadi worker.
* **`sleipnir_vs_thread`** — `.Dedicated_Thread` vs. `.Sleipnir_Actor` (standalone scheduler, shared 4-worker scheduler with 4 producer actors, live `.Flush_Sync` checkpoints, tight 2-page `.Block` saturation, and 16x rapid lifecycle churn).
* **`auto_backend`** — `Backend_Mode.Auto` adaptive selection vs. suboptimal forced `.Dedicated_Thread` / `.Sleipnir_Actor` overrides across 4-worker actor churn, 1-worker `.Block` actor turns, and plain OS threads.
* **`arrow_io_roundtrip`** — End-to-end 262,144-row `.arrow` IPC segment write (`arrow_write_262k`) and `Skadi` columnar `ipc_read_file` scan (`arrow_read_262k`).

## Running & Comparing Benchmarks

Run the suite and append results to `benchmarks/history.hugidb`:

```bash
hugi run benchmarks --runs 5
```

Useful filters and inspection commands:

```bash
# Run a subset of groups without writing to the store
hugi run benchmarks --no-db --runs 1 --groups zero_cost,single_thread

# Inspect stored commits and pooled metrics
hugi history --db benchmarks/history.hugidb
hugi inspect --db benchmarks/history.hugidb

# Statistically compare the last two commits (Mann-Whitney U / Welch t-test)
hugi compare --last

# Refresh the Markdown table below from the compressed history store
hugi export --update-readme benchmarks/README.md
```

## Latest Results

<!-- BEGIN_BENCHMARK_TABLE -->
| Benchmark | Runs | Samples | Median | P95 | Ops/s | CV% | Footprint | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `zero_cost::scoped_span_nil` | 1 | 50 | **1.51 ns** | 1.53 ns | 663941849 | 1.2% | - | 0 |
| `zero_cost::counter_add_nil` | 1 | 50 | **1.51 ns** | 1.54 ns | 664094408 | 6.9% | - | 0 |
| `zero_cost::scoped_span_disabled` | 1 | 50 | **1.51 ns** | 1.53 ns | 663391723 | 2.5% | - | 0 |
| `zero_cost::nested_http_db_nil` | 1 | 50 | **5.66 ns** | 5.69 ns | 176728904 | 0.7% | - | 0 |
| `single_thread::event_emit_block` | 1 | 5 | **71.48 ns** | 72.26 ns | 13989992 | 1.0% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::counter_add_block` | 1 | 5 | **72.70 ns** | 73.96 ns | 13755609 | 1.0% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::scoped_span_raii` | 1 | 5 | **111.09 ns** | 111.34 ns | 9002057 | 0.2% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::http_db_tree_2level` | 1 | 5 | **91.80 ns** | 92.10 ns | 10893592 | 0.3% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::call_tree_3level` | 1 | 5 | **100.49 ns** | 101.79 ns | 9951588 | 0.8% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::event_emit_dynamic_copy` | 1 | 5 | **107.02 ns** | 114.08 ns | 9344077 | 3.5% | 3.75 MiB | 0.0 (107 B) |
| `backpressure::drop_500k_burst` | 1 | 5 | **53.97 ns** | 54.75 ns | 18529554 | 0.8% | 1.38 MiB | 0.0 (13 B) |
| `backpressure::block_500k_burst` | 1 | 5 | **67.02 ns** | 67.66 ns | 14921001 | 1.1% | 2.75 MiB | 0.0 (13 B) |
| `mt_scaling::threads_1` | 1 | 5 | **89.13 ns** | 89.55 ns | 11219850 | 0.3% | 5.50 MiB | 0.0 (26 B) |
| `mt_scaling::threads_2` | 1 | 5 | **147.78 ns** | 149.80 ns | 6767011 | 1.9% | 5.50 MiB | 0.0 (28 B) |
| `mt_scaling::threads_4` | 1 | 5 | **188.30 ns** | 193.54 ns | 5310729 | 1.6% | 5.50 MiB | 0.0 (28 B) |
| `mt_scaling::threads_8` | 1 | 5 | **297.97 ns** | 306.70 ns | 3356061 | 2.6% | 5.50 MiB | 0.0 (29 B) |
| `sleipnir_vs_thread::dedicated_thread_1p_1w` | 1 | 5 | **88.68 ns** | 89.25 ns | 11276296 | 0.5% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_1w` | 1 | 5 | **88.26 ns** | 88.74 ns | 11329609 | 0.3% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_4w_4actors` | 1 | 5 | **259.03 ns** | 267.10 ns | 3860578 | 3.4% | 5.50 MiB | 0.0 (29 B) |
| `sleipnir_vs_thread::sleipnir_actor_flush_sync_x8` | 1 | 5 | **120.66 ns** | 121.08 ns | 8287473 | 1.0% | 2.75 MiB | 0.0 (13 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_2page_block` | 1 | 5 | **112.46 ns** | 113.33 ns | 8891882 | 0.5% | 704.00 KiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_2page_block` | 1 | 5 | **260.98 ns** | 268.75 ns | 3831735 | 3.1% | 704.00 KiB | 0.0 (30 B) |
| `sleipnir_vs_thread::sleipnir_actor_lifecycle_shutdown_x16` | 1 | 5 | **137.51 ns** | 143.88 ns | 7272138 | 2.5% | 1.38 MiB | 0.0 (192 B) |
| `auto_backend::forced_dedicated_thread_in_4w_actor_x64` | 1 | 5 | **227.51 ns** | 230.57 ns | 4395456 | 1.6% | 704.00 KiB | 0.0 (283 B) |
| `auto_backend::auto_in_4w_actor_x64` | 1 | 5 | **146.87 ns** | 150.22 ns | 6808723 | 1.8% | 704.00 KiB | 0.0 (285 B) |
| `auto_backend::forced_sleipnir_actor_1w_block` | 1 | 5 | **149.93 ns** | 151.01 ns | 6669714 | 1.3% | 2.75 MiB | 0.0 (19 B) |
| `auto_backend::auto_on_1w_sched_block` | 1 | 5 | **100.21 ns** | 100.54 ns | 9978617 | 0.3% | 2.75 MiB | 0.0 (18 B) |
| `auto_backend::forced_sleipnir_actor_os_thread` | 1 | 5 | **82.20 ns** | 82.94 ns | 12165850 | 0.5% | 2.75 MiB | 0.0 (7 B) |
| `auto_backend::auto_on_os_thread` | 1 | 5 | **82.78 ns** | 84.35 ns | 12080860 | 1.1% | 2.75 MiB | 0.0 (7 B) |
| `arrow_io_roundtrip::arrow_write_262k` | 1 | 5 | **89.60 ns** | 89.75 ns | 11160551 | 0.3% | 5.50 MiB | 0.0 (26 B) |
| `arrow_io_roundtrip::arrow_read_262k` | 1 | 5 | **5.16 ns** | 5.40 ns | 193670462 | 2.6% | 21.67 MiB | 0.0 (0 B) |
<!-- END_BENCHMARK_TABLE -->
