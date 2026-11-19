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
| `zero_cost::scoped_span_nil` | 1 | 50 | **708.31 ps** | 754.59 ps | 1411807226 | 9.1% | - | 0 |
| `zero_cost::counter_add_nil` | 1 | 50 | **696.17 ps** | 739.70 ps | 1436439010 | 8.0% | - | 0 |
| `zero_cost::scoped_span_disabled` | 1 | 50 | **769.98 ps** | 831.00 ps | 1298738406 | 7.7% | - | 0 |
| `zero_cost::nested_http_db_nil` | 1 | 50 | **4.27 ns** | 4.41 ns | 234290033 | 1.9% | - | 0 |
| `single_thread::event_emit_block` | 1 | 7 | **53.53 ns** | 54.72 ns | 18679389 | 2.5% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::counter_add_block` | 1 | 7 | **52.37 ns** | 54.95 ns | 19094553 | 3.5% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::scoped_span_raii` | 1 | 7 | **70.82 ns** | 75.71 ns | 14120341 | 3.6% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::http_db_tree_2level` | 1 | 7 | **64.89 ns** | 70.19 ns | 15410593 | 6.0% | 2.75 MiB | 0.0 (26 B) |
| `single_thread::call_tree_3level` | 1 | 7 | **68.26 ns** | 70.38 ns | 14649619 | 3.4% | 2.75 MiB | 0.0 (24 B) |
| `single_thread::event_emit_dynamic_copy` | 1 | 7 | **75.47 ns** | 90.12 ns | 13250137 | 9.1% | 3.75 MiB | 0.0 (107 B) |
| `backpressure::drop_500k_burst` | 1 | 7 | **31.40 ns** | 31.71 ns | 31842970 | 0.5% | 1.38 MiB | 0.0 (13 B) |
| `backpressure::block_500k_burst` | 1 | 7 | **48.30 ns** | 52.55 ns | 20701920 | 5.2% | 2.75 MiB | 0.0 (13 B) |
| `mt_scaling::threads_1` | 1 | 7 | **59.03 ns** | 64.29 ns | 16940724 | 4.4% | 5.50 MiB | 0.0 (26 B) |
| `mt_scaling::threads_2` | 1 | 7 | **96.85 ns** | 98.71 ns | 10325082 | 2.2% | 5.50 MiB | 0.0 (28 B) |
| `mt_scaling::threads_4` | 1 | 7 | **127.23 ns** | 130.22 ns | 7859983 | 2.0% | 5.50 MiB | 0.0 (28 B) |
| `mt_scaling::threads_8` | 1 | 7 | **182.44 ns** | 184.79 ns | 5481389 | 1.5% | 5.50 MiB | 0.0 (29 B) |
| `sleipnir_vs_thread::dedicated_thread_1p_1w` | 1 | 7 | **56.39 ns** | 69.28 ns | 17733335 | 10.6% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_1w` | 1 | 7 | **56.77 ns** | 77.62 ns | 17614087 | 15.9% | 5.50 MiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_4w_4actors` | 1 | 7 | **164.45 ns** | 173.85 ns | 6080874 | 3.1% | 5.50 MiB | 0.0 (31 B) |
| `sleipnir_vs_thread::sleipnir_actor_flush_sync_x8` | 1 | 7 | **80.92 ns** | 91.83 ns | 12358276 | 7.6% | 2.75 MiB | 0.0 (13 B) |
| `sleipnir_vs_thread::sleipnir_actor_standalone_2page_block` | 1 | 7 | **80.48 ns** | 85.14 ns | 12425234 | 4.9% | 704.00 KiB | 0.0 (26 B) |
| `sleipnir_vs_thread::sleipnir_actor_shared_2page_block` | 1 | 7 | **162.92 ns** | 169.72 ns | 6138053 | 3.5% | 704.00 KiB | 0.0 (30 B) |
| `sleipnir_vs_thread::sleipnir_actor_lifecycle_shutdown_x16` | 1 | 7 | **101.26 ns** | 105.65 ns | 9875424 | 2.5% | 1.38 MiB | 0.0 (192 B) |
| `auto_backend::forced_dedicated_thread_in_4w_actor_x64` | 1 | 7 | **193.71 ns** | 222.18 ns | 5162288 | 7.7% | 704.00 KiB | 0.0 (283 B) |
| `auto_backend::auto_in_4w_actor_x64` | 1 | 7 | **100.60 ns** | 105.93 ns | 9940759 | 3.8% | 704.00 KiB | 0.0 (285 B) |
| `auto_backend::forced_sleipnir_actor_1w_block` | 1 | 7 | **104.01 ns** | 108.26 ns | 9614168 | 3.7% | 2.75 MiB | 0.0 (19 B) |
| `auto_backend::auto_on_1w_sched_block` | 1 | 7 | **62.91 ns** | 64.55 ns | 15894866 | 1.3% | 2.75 MiB | 0.0 (18 B) |
| `auto_backend::forced_sleipnir_actor_os_thread` | 1 | 7 | **50.88 ns** | 53.25 ns | 19652375 | 3.0% | 2.75 MiB | 0.0 (7 B) |
| `auto_backend::auto_on_os_thread` | 1 | 7 | **52.57 ns** | 55.02 ns | 19023568 | 4.1% | 2.75 MiB | 0.0 (7 B) |
| `arrow_io_roundtrip::arrow_write_262k` | 1 | 7 | **78.03 ns** | 82.05 ns | 12814794 | 4.5% | 5.50 MiB | 0.0 (26 B) |
| `arrow_io_roundtrip::arrow_read_262k` | 1 | 7 | **3.43 ns** | 4.17 ns | 291564911 | 10.6% | 21.67 MiB | 0.0 (0 B) |
<!-- END_BENCHMARK_TABLE -->
