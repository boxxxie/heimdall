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
<!-- END_BENCHMARK_TABLE -->
