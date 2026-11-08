# Heimdall Usage Rules for Coding Agents & Contributors

This file defines mandatory usage rules, idioms, and architectural constraints for AI coding agents and developers working with or contributing to **Heimdall** (`package heimdall`, `package heimdall_codegen`, and `package heimdall_tests`).

---

## 1. Instrumentation Rules (`package heimdall`)

### 1.1 `scoped_span` Placement (CRITICAL)
`heimdall.scoped_span` is annotated with `@(deferred_out = span_end)`. It begins timing immediately at the call site and automatically calls `span_end` when the enclosing lexical block exits.

* **DO** place `heimdall.scoped_span(rec, "scope", "name", attr_i64)` at the **top** of the procedure or lexical block whose work you want to measure:
  ```odin
  process_frame :: proc(rec: ^heimdall.Recorder, frame_id: i64) {
      heimdall.scoped_span(rec, "renderer", "process_frame", frame_id)
      // ... work measured by the span ...
  }
  ```
* **NEVER** prefix `scoped_span` with `defer`:
  ```odin
  // WRONG: Delays `span_begin` until scope exit and records ~0 ns duration!
  defer heimdall.scoped_span(rec, "renderer", "process_frame", frame_id)
  ```
* **NEVER** place `scoped_span` as the final statement of a block, as the sole statement inside an `if`/`for`/`switch` body, or immediately before a `return`, `break`, or `continue`:
  ```odin
  // WRONG: Scope exits immediately after `scoped_span`, wrapping zero work!
  if trace_enabled {
      heimdall.scoped_span(rec, "renderer", "process_frame", frame_id)
  }
  ```
  *Note: `heimdall_codegen.analyze_telemetry_call_sites` statically flags all of these as `misplaced_scoped_spans`.*

### 1.2 Mutating Active Span Status & Attributes
* **DO** use `heimdall.span_set_error(rec, err_code)` or `heimdall.span_set_status(rec, status, attr_i64, update_attr)` while inside an active `scoped_span` to mark the current span as `.Error` or override its `attr_i64` tag before scope exit.
* **DO** use manual `g := heimdall.span_begin(...)` and `defer heimdall.span_end(g)` only when you need to hold or mutate the `Span_Guard` value directly across conditional branches.

### 1.3 String Lifetimes (`string_fallback_copy`)
* **By default (`Config.string_fallback_copy == false`):** Heimdall stores `scope` and `name` string slices by reference in the `#soa` staging page until the page is serialized to Arrow IPC.
  * **DO** pass string literals or compile-time constants (`SCOPE_HTTP :: "net.http"`) when `string_fallback_copy == false`.
  * **NEVER** pass `fmt.tprintf(...)` or heap-allocated strings that are freed before the segment flushes unless `Config.string_fallback_copy = true`.
* **When passing dynamic strings:** Set `Config.string_fallback_copy = true`. Heimdall will clone `scope` and `name` into a single contiguous allocation under `Recorder._alloc_mu` and free them automatically after Arrow serialization (or upon drop).

### 1.4 Library-Friendly `nil` Handling
* **NEVER** wrap `heimdall.scoped_span`, `counter_add`, `gauge_set`, `histogram_record`, or `event_emit` in `if rec != nil && rec.enabled` checks at call sites. Every Heimdall instrumentation procedure already performs an inlined `r == nil || !intrinsics.atomic_load_explicit(&r.enabled, .Relaxed)` fast-path check.

---

## 2. Recorder Lifecycle & Concurrency Rules

### 2.1 Initialization & Teardown
* **ALWAYS** pair a successful `heimdall.recorder_init(&rec, cfg)` with `heimdall.recorder_destroy(&rec)` before `rec` goes out of scope or its backing memory is reused.
* **DO** leave `Config.backend = .Auto` (the zero-value default) unless you have a specific reason to force `.Dedicated_Thread` or `.Sleipnir_Actor`.
* **DO** use `Backpressure_Policy.Drop` for real-time threads (audio DSP, rendering, high-frequency tick loops) that must never block on disk I/O, and `Backpressure_Policy.Block` for lossless batch/server workloads.
* **NEVER** mutate `rec.allocator` after `recorder_init` has been called while the background worker thread or flusher actor is running. Pass your custom `allocator` directly as the 3rd argument to `recorder_init(&rec, cfg, allocator)`.

---

## 3. Code Generation & Schema Rules (`tools/heimdall_gen`)

* **NEVER** manually edit generated files ending in `_gen.odin`:
  * `heimdall/schema_gen.odin`
  * `tests/dsl_atdd_gen.odin`
* **ALWAYS** regenerate them after modifying `@(telemetry_schema) Record :: struct` in `heimdall/heimdall.odin` or any `given_*` / `upon_*` / `then_*` step in `tests/dsl_*.odin`:
  ```bash
  odin run tools/heimdall_gen -- tests
  ```
* **ALWAYS** pass `-custom-attribute:telemetry_schema` when invoking `odin check`, `odin test`, or `odin build` on packages that include `heimdall`.

---

## 4. Four-Layer ATDD Architecture Rules (`tests/`)

The test suite enforces a strict **4-Layer Acceptance Test-Driven Development (ATDD)** architecture verified by `spec_ast_architecture.odin`:

1. **Layer 1 — Executable Specifications (`tests/spec_*.odin`):**
   * **ONLY** import `"core:testing"`. Importing `heimdall`, `OdinArrow`, `sleipnir`, or `core:os` is **forbidden**.
   * **ONLY** declare `@(test)` procedures. Helper procedures or file-scope constants in `spec_*.odin` are **forbidden**.
   * **ALWAYS** begin a scenario with `_ = scenario(t)` or `s := scenario(t)`.
   * **ONLY** call Layer 2 DSL steps via `given.<step>(...)`, `upon.<step>(...)`, `then.<step>(...)`, or `s-><step>(...)`.
   * **NEVER** call `heimdall.*`, `driver_*`, or `testing.expect*` directly inside `spec_*.odin`.
   * **NEVER** access fields on `s` (`s.rec`, `s.rec_active`, etc.) inside `spec_*.odin`.
   * **NEVER** define or call zero-argument `upon` or `then` "mega-steps" (every `upon` and `then` step must take at least one explicit domain parameter in addition to `s: ^Suite_Ctx` and `loc := #caller_location`).
2. **Layer 2 — Domain-Specific Language (`tests/dsl_heimdall.odin`):**
   * Define `given_*`, `upon_*`, and `then_*` procedures taking `(s: ^Suite_Ctx, ..., loc := #caller_location)`.
   * Translate domain assertions into `driver_*` calls and `testing.expect*` checks forwarding `loc = loc`.
   * Every DSL step defined in `dsl_heimdall.odin` **MUST** be called by at least one `@(test)` scenario in `spec_*.odin` ($0$ unused DSL procedures allowed).
3. **Layer 3 — Protocol Driver (`tests/driver_heimdall.odin`):**
   * The **ONLY** test layer allowed to import `OdinArrow`, `sleipnir/actor`, `heimdall/codegen`, and `heimdall` internals, or manage `.arrow` files on disk.
4. **Layer 4 — System Under Test (`heimdall/`):**
   * Core telemetry engine and AST codegen packages.

---

## 5. Mandatory Odin Style & Compiler Vet Flags

All code in `heimdall/`, `tests/`, and `benchmarks/` must pass:
```bash
odin check heimdall/codegen -no-entry-point -vet -strict-style -vet-tabs -disallow-do
odin check tests -no-entry-point -vet -strict-style -vet-tabs -custom-attribute:telemetry_schema -vet-packages:heimdall,heimdall_codegen,heimdall_tests
```
* **NEVER** use `do` statements (`if cond do ...` or `for x in xs do ...`). Always use explicit `{ ... }` braces (enforced across `heimdall/` and `tests/` by `spec_ast_architecture.odin`).
* **ALWAYS** use tabs for indentation (`-vet-tabs`) and adhere to `-strict-style` and `-vet` (no unused imports, variables, or parameters).
