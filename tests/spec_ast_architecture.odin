package heimdall_tests

import "core:testing"

// =============================================================================
// Layer 1: AST Architecture & Codegen Contract Specification
// (`tests/spec_ast_architecture.odin`)
// =============================================================================

@(test)
spec_ast_parser_enforces_four_layer_atdd_architecture_and_codegen_sync :: proc(t: ^testing.T) {
	_ = scenario(t)

	// 1. Parse every `.odin` file in `tests/` and `heimdall/` into Odin ASTs
	//    (`core:odin/parser` + `core:odin/ast` + `ast.walk`) and verify:
	//    - `tests/dsl_atdd_gen.odin` matches the live AST codegen output from `tests/dsl_heimdall.odin`
	//    - Every `tests/spec_*.odin` file has zero forbidden `OdinArrow` imports
	//    - Every `@(scenario)` procedure in `tests/spec_*.odin` has zero direct `heimdall.*` or `driver_*` calls
	//    - Every `@(scenario)` procedure in `tests/spec_*.odin` has zero raw `testing.expect*` calls
	//    - Every `upon.*` / `then.*` call in `tests/spec_*.odin` passes explicit domain arguments (no 0-arg mega-steps)
	//    - Every `tests/spec_*.odin` file has zero direct `s.*` driver field accesses
	//    - Every `.odin` file in `tests/` and `heimdall/` has zero `do` statements (`ast.Block_Stmt.uses_do == false`)
	//    - 100% of `given_*` / `upon_*` / `then_*` steps defined in `tests/dsl_heimdall.odin` are exercised
	then.ast_enforces_four_layer_atdd_contract("tests", "heimdall")

	// 2. Verify the AST analyzer catches every category of 4-layer & style violation on a synthetic bad spec
	bad_spec_src :: `
package heimdall_tests

import "core:testing"
import "core:os"
import "../heimdall"
import oa "../deps/OdinArrow/src"

forbidden_file_const :: 42

sneaky_helper :: proc(s: ^Suite_Ctx) {
	if true do _ = s.rec_active
}

@(test)
spec_bad_leaky_scenario :: proc(t: ^testing.T) {
	drv := scenario(t)
	heimdall.recorder_flush(&drv.rec)
	_ = os.exists("foo")
	_ = current_scenario().rec_active
	drv.rec.custom_call()
	sneaky_helper(drv)
	testing.expect(t, true)
	then.opaque_mega_step()
}
`
	bad_dsl_src :: `
package heimdall_tests

then_opaque_mega_step :: proc(s: ^Suite_Ctx) {
}
`
	then.ast_detects_layer_violations_in_synthetic_spec(
		synthetic_spec_src      = bad_spec_src,
		synthetic_dsl_src       = bad_dsl_src,
		expected_non_test_procs = 2, // forbidden_file_const + sneaky_helper decl lacks @(test)
		expected_sut_calls      = 4, // heimdall.recorder_flush + os.exists + drv.rec.custom_call + sneaky_helper call
		expected_testing_calls  = 1,
		expected_zero_arg_steps = 2, // 1 in dsl decl + 1 at spec call site
		expected_driver_leaks   = 4, // s.rec_active in helper + drv.rec (x2) + current_scenario().rec_active
		expected_forbidden_imps = 3, // "core:os" + "../heimdall" + "../deps/OdinArrow/src"
		expected_do_statements  = 1,
	)

	// 3. Verify that calling a 1-arg `*_ambient` DSL procedure directly in a Layer 1 spec
	//    is NOT misclassified as a zero-arg mega-step (`expected_zero_arg_steps == 0`),
	//    and an unrelated local variable named `s` inside `_ = scenario(t)` is NOT
	//    misflagged as a `driver_field_leaks` violation.
	valid_ambient_spec_src :: `
package heimdall_tests

import "core:testing"

@(test)
spec_valid_ambient_step_call :: proc(t: ^testing.T) {
	_ = scenario(t)
	s := struct{ count: int }{ count = 42 }
	upon_valid_step_ambient(s.count)
	then_valid_outcome_ambient(s.count)
}
`
	valid_ambient_dsl_src :: `
package heimdall_tests

upon_valid_step :: proc(s: ^Suite_Ctx, count: int) {
}

then_valid_outcome :: proc(s: ^Suite_Ctx, expected: int) {
}
`
	then.ast_detects_layer_violations_in_synthetic_spec(
		synthetic_spec_src      = valid_ambient_spec_src,
		synthetic_dsl_src       = valid_ambient_dsl_src,
		expected_non_test_procs = 0,
		expected_sut_calls      = 0,
		expected_testing_calls  = 0,
		expected_zero_arg_steps = 0,
		expected_driver_leaks   = 0,
		expected_forbidden_imps = 0,
		expected_do_statements  = 0,
	)

	// 4. Verify the AST telemetry call-site analyzer extracts deduplicated static
	//    `scope`/`name` string literals, detects uncloned dynamic `fmt.tprintf`
	//    string arguments, and catches misplaced `heimdall.scoped_span` calls
	//    (sole statement inside `if` block or last statement of a procedure body).
	instrumented_app_src :: `
package sample_app

import "core:fmt"
import "../heimdall"

SCOPE_HTTP :: "net.http"
METRIC_REQ :: "requests_total"

handle_request :: proc(rec: ^heimdall.Recorder, user_id: int, trace_enabled: bool) {
	heimdall.scoped_span(rec, SCOPE_HTTP, "handle_request", i64(user_id))
	heimdall.counter_add(rec, SCOPE_HTTP, METRIC_REQ, 1.0, i64(user_id))
	heimdall.counter_add(rec, "net.http", "requests_total", 1.0, i64(user_id))
	heimdall.event_emit(rec, attr_i64 = i64(user_id), name = "named_verify", scope = "auth.jwt")
	heimdall.event_emit(rec, "auth.jwt", fmt.tprintf("user_%d", user_id), i64(user_id))

	if user_id > 1000 {
		METRIC_REQ := fmt.tprintf("shadowed_%d", user_id)
		heimdall.counter_add(rec, SCOPE_HTTP, METRIC_REQ, 1.0, i64(user_id))
	}

	if trace_enabled {
		heimdall.scoped_span(rec, "net.http", "misplaced_if_span", 1)
	}

	if user_id < 0 {
		heimdall.scoped_span(rec, "net.http", "misplaced_before_return", 4)
		return
	}

	switch user_id {
	case 0:
		heimdall.scoped_span(rec, "net.http", "misplaced_case_span", 3)
	}

	for i in 0 ..< 2 {
		if i == 1 {
			heimdall.scoped_span(rec, "net.http", "misplaced_before_break", 5)
			break
		}
	}

	defer heimdall.scoped_span(rec, "net.http", "misplaced_defer_span", 6)

	heimdall.scoped_span(rec, "db.sql", "misplaced_tail_span", 2)
}

worker_actor_receive :: proc(ctx: ^actor.Context, state: ^Worker_State, msg: ^int) {
	heimdall.scoped_span(state.rec, "net.http", "handle_request", i64(msg^))
	heimdall.counter_add(state.rec, "net.http", "requests_total", 1.0, i64(msg^))
}
`
	then.ast_analyzes_telemetry_call_sites(
		instrumented_src         = instrumented_app_src,
		expected_total_calls     = 14,
		expected_unique_scopes   = 3, // "auth.jwt", "db.sql", "net.http"
		expected_unique_names    = 9, // "handle_request", "misplaced_before_break", "misplaced_before_return", "misplaced_case_span", "misplaced_defer_span", "misplaced_if_span", "misplaced_tail_span", "named_verify", "requests_total"
		expected_dynamic_hazards = 2, // fmt.tprintf("user_%d", user_id) + shadowed local `METRIC_REQ := fmt.tprintf(...)`
		expected_misplaced_spans = 6, // sole stmt in `if` + before `return` + `switch` `case` + before `break` + `defer scoped_span` + last stmt in proc body
		expected_actor_calls     = 2, // 2 calls inside `worker_actor_receive(ctx: ^actor.Context, ...)`
	)

	// 5. Verify the AST codegen rejects calls to undefined Layer 2 DSL steps (`unknown_dsl_calls`),
	//    emits polymorphic `$T` and variadic `..` step wrappers (with named `loc = #caller_location`),
	//    and generates custom `@(telemetry_schema)` Arrow serializers from arbitrary structs.
	then.ast_validates_unknown_dsl_steps_poly_codegen_and_custom_schema(
		expected_custom_schema_cols = 4,
	)
}
