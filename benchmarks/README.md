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
| `zero_cost::scoped_span_nil` | 1 | 50 | **1.51 ns** | 1.51 ns | 664286749 | 0.5% | - | 0 |
| `zero_cost::counter_add_nil` | 1 | 50 | **1.51 ns** | 1.55 ns | 660466210 | 1.3% | - | 0 |
| `zero_cost::scoped_span_disabled` | 1 | 50 | **1.51 ns** | 1.52 ns | 664047222 | 1.0% | - | 0 |
| `zero_cost::nested_http_db_nil` | 1 | 50 | **5.65 ns** | 5.67 ns | 177103431 | 0.3% | - | 0 |
| `single_thread::event_emit_block` | 1 | 5 | **73.70 ns** | 74.52 ns | 13567602 | 0.7% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::counter_add_block` | 1 | 5 | **74.53 ns** | 77.21 ns | 13417365 | 2.0% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::scoped_span_raii` | 1 | 5 | **111.50 ns** | 112.03 ns | 8968799 | 0.4% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::http_db_tree_2level` | 1 | 5 | **92.49 ns** | 92.95 ns | 10811494 | 0.4% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::call_tree_3level` | 1 | 5 | **100.90 ns** | 102.68 ns | 9910890 | 1.0% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::event_emit_dynamic_copy` | 1 | 5 | **109.00 ns** | 110.75 ns | 9174583 | 1.5% | 3.75 MiB | 0.0 (107 B) |
| `backpressure::drop_500k_burst` | 1 | 5 | **53.91 ns** | 56.18 ns | 18548865 | 2.4% | 1.38 MiB | 0.0 (13 B) |
| `backpressure::block_500k_burst` | 1 | 5 | **69.45 ns** | 72.08 ns | 14398198 | 2.2% | 2.75 MiB | 0.0 (13 B) |
| `mt_scaling::threads_1` | 1 | 5 | **90.33 ns** | 90.76 ns | 11069976 | 0.4% | 5.50 MiB | 0.0 (26 B) |
| `mt_scaling::threads_2` | 1 | 5 | **146.87 ns** | 149.32 ns | 6808658 | 2.6% | 5.50 MiB | 0.0 (27 B) |
| `mt_scaling::threads_4` | 1 | 5 | **194.06 ns** | 203.74 ns | 5153018 | 2.8% | 5.50 MiB | 0.0 (29 B) |
| `mt_scaling::threads_8` | 1 | 5 | **285.66 ns** | 298.83 ns | 3500702 | 3.3% | 5.50 MiB | 0.0 (29 B) |
| `sleipnir_vs_thread::dedicated_thread_1p_1w` | 1 | 5 | **89.75 ns** | 100.38 ns | 11142128 | 6.3% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_1w` | 1 | 5 | **88.91 ns** | 100.86 ns | 11247184 | 7.3% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_4w_4actors` | 1 | 5 | **239.01 ns** | 242.05 ns | 4183950 | 1.0% | 5.50 MiB | 0.0 (30 B) |
| `sleipnir_vs_thread::sleipnir_actor_flush_sync_x8` | 1 | 5 | **125.18 ns** | 126.24 ns | 7988708 | 1.5% | 2.75 MiB | 0.0 (13 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_2page_block` | 1 | 5 | **115.02 ns** | 115.58 ns | 8694008 | 0.4% | 704.00 KiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_2page_block` | 1 | 5 | **236.37 ns** | 249.35 ns | 4230627 | 2.9% | 704.00 KiB | 0.0 (30 B) |
| `sleipnir_vs_thread::sleipnir_actor_lifecycle_shutdown_x16` | 1 | 5 | **139.88 ns** | 140.56 ns | 7149237 | 1.0% | 1.38 MiB | 0.0 (192 B) |
| `auto_backend::forced_dedicated_thread_in_4w_actor_x64` | 1 | 5 | **257.14 ns** | 261.85 ns | 3888867 | 2.0% | 704.00 KiB | 0.0 (283 B) |
| `auto_backend::auto_in_4w_actor_x64` | 1 | 5 | **149.22 ns** | 151.86 ns | 6701307 | 1.1% | 704.00 KiB | 0.0 (285 B) |
| `auto_backend::forced_sleipnir_actor_1w_block` | 1 | 5 | **151.84 ns** | 152.16 ns | 6585784 | 0.4% | 2.75 MiB | 0.0 (19 B) |
| `auto_backend::auto_on_1w_sched_block` | 1 | 5 | **100.29 ns** | 100.73 ns | 9970962 | 0.5% | 2.75 MiB | 0.0 (18 B) |
| `auto_backend::forced_sleipnir_actor_os_thread` | 1 | 5 | **82.12 ns** | 82.22 ns | 12177452 | 0.2% | 2.75 MiB | 0.0 (7 B) |
| `auto_backend::auto_on_os_thread` | 1 | 5 | **82.36 ns** | 82.45 ns | 12142246 | 0.1% | 2.75 MiB | 0.0 (7 B) |
| `arrow_io_roundtrip::arrow_write_262k` | 1 | 5 | **109.65 ns** | 110.98 ns | 9120318 | 1.2% | 5.50 MiB | 0.0 (26 B) |
| `arrow_io_roundtrip::arrow_read_262k` | 1 | 5 | **5.15 ns** | 5.25 ns | 194167933 | 1.5% | 21.67 MiB | 0.0 (0 B) |
<!-- END_BENCHMARK_TABLE -->
