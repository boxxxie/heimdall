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
| `zero_cost::scoped_span_nil` | 1 | 50 | **718.28 ps** | 946.13 ps | 1392212797 | 12.1% | - | 0 |
| `zero_cost::counter_add_nil` | 1 | 50 | **706.13 ps** | 710.00 ps | 1416167822 | 0.6% | - | 0 |
| `zero_cost::scoped_span_disabled` | 1 | 50 | **790.11 ps** | 796.47 ps | 1265643352 | 1.1% | - | 0 |
| `zero_cost::nested_http_db_nil` | 1 | 50 | **4.32 ns** | 4.34 ns | 231694726 | 0.6% | - | 0 |
| `single_thread::event_emit_block` | 1 | 7 | **51.99 ns** | 53.67 ns | 19235958 | 4.4% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::counter_add_block` | 1 | 7 | **51.13 ns** | 52.98 ns | 19558212 | 4.2% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::scoped_span_raii` | 1 | 7 | **70.83 ns** | 74.71 ns | 14118376 | 3.0% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::http_db_tree_2level` | 1 | 7 | **64.65 ns** | 67.94 ns | 15467405 | 4.9% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::call_tree_3level` | 1 | 7 | **70.03 ns** | 74.67 ns | 14280452 | 3.4% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::event_emit_dynamic_copy` | 1 | 7 | **73.69 ns** | 85.72 ns | 13570916 | 7.6% | 3.75 MiB | 0.0 (107 B) |
| `backpressure::drop_500k_burst` | 1 | 7 | **31.44 ns** | 32.33 ns | 31810987 | 1.4% | 1.38 MiB | 0.0 (13 B) |
| `backpressure::block_500k_burst` | 1 | 7 | **46.13 ns** | 47.69 ns | 21677916 | 3.5% | 2.75 MiB | 0.0 (13 B) |
| `mt_scaling::threads_1` | 1 | 7 | **57.85 ns** | 61.21 ns | 17284860 | 3.5% | 5.50 MiB | 0.0 (26 B) |
| `mt_scaling::threads_2` | 1 | 7 | **94.61 ns** | 99.59 ns | 10569841 | 2.9% | 5.50 MiB | 0.0 (28 B) |
| `mt_scaling::threads_4` | 1 | 7 | **128.32 ns** | 130.10 ns | 7792847 | 1.0% | 5.50 MiB | 0.0 (29 B) |
| `mt_scaling::threads_8` | 1 | 7 | **181.13 ns** | 184.03 ns | 5520886 | 1.7% | 5.50 MiB | 0.0 (29 B) |
| `sleipnir_vs_thread::dedicated_thread_1p_1w` | 1 | 7 | **55.51 ns** | 57.57 ns | 18013473 | 2.2% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_1w` | 1 | 7 | **54.99 ns** | 57.57 ns | 18185418 | 2.6% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_4w_4actors` | 1 | 7 | **161.35 ns** | 165.70 ns | 6197552 | 1.5% | 5.50 MiB | 0.0 (29 B) |
| `sleipnir_vs_thread::sleipnir_actor_flush_sync_x8` | 1 | 7 | **79.18 ns** | 84.41 ns | 12629542 | 3.5% | 2.75 MiB | 0.0 (13 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_2page_block` | 1 | 7 | **76.37 ns** | 79.93 ns | 13093833 | 3.3% | 704.00 KiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_2page_block` | 1 | 7 | **167.12 ns** | 169.28 ns | 5983622 | 2.1% | 704.00 KiB | 0.0 (29 B) |
| `sleipnir_vs_thread::sleipnir_actor_lifecycle_shutdown_x16` | 1 | 7 | **99.70 ns** | 103.19 ns | 10029804 | 2.5% | 1.38 MiB | 0.0 (192 B) |
| `auto_backend::forced_dedicated_thread_in_4w_actor_x64` | 1 | 7 | **199.91 ns** | 219.05 ns | 5002227 | 10.1% | 704.00 KiB | 0.0 (283 B) |
| `auto_backend::auto_in_4w_actor_x64` | 1 | 7 | **98.68 ns** | 105.47 ns | 10133296 | 4.0% | 704.00 KiB | 0.0 (285 B) |
| `auto_backend::forced_sleipnir_actor_1w_block` | 1 | 7 | **101.48 ns** | 105.40 ns | 9854021 | 2.4% | 2.75 MiB | 0.0 (19 B) |
| `auto_backend::auto_on_1w_sched_block` | 1 | 7 | **74.79 ns** | 77.65 ns | 13371248 | 5.5% | 2.75 MiB | 0.0 (18 B) |
| `auto_backend::forced_sleipnir_actor_os_thread` | 1 | 7 | **49.73 ns** | 50.31 ns | 20106658 | 0.8% | 2.75 MiB | 0.0 (7 B) |
| `auto_backend::auto_on_os_thread` | 1 | 7 | **50.35 ns** | 53.82 ns | 19861828 | 3.7% | 2.75 MiB | 0.0 (7 B) |
| `arrow_io_roundtrip::arrow_write_262k` | 1 | 7 | **61.74 ns** | 77.77 ns | 16197171 | 13.6% | 5.50 MiB | 0.0 (26 B) |
| `arrow_io_roundtrip::arrow_read_262k` | 1 | 7 | **4.12 ns** | 4.77 ns | 242906109 | 13.3% | 21.67 MiB | 0.0 (0 B) |
<!-- END_BENCHMARK_TABLE -->
