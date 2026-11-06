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
import "../heimdall"
import oa "../deps/OdinArrow/src"

sneaky_helper :: proc(s: ^Suite_Ctx) {
	if true do _ = s.rec_active
}

@(test)
spec_bad_leaky_scenario :: proc(t: ^testing.T) {
	drv := scenario(t)
	heimdall.recorder_flush(&drv.rec)
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
		expected_sut_calls      = 3, // sneaky_helper decl + heimdall.recorder_flush + sneaky_helper call
		expected_testing_calls  = 1,
		expected_zero_arg_steps = 2, // 1 in dsl decl + 1 at spec call site
		expected_driver_leaks   = 2, // s.rec_active in helper + drv.rec in spec
		expected_forbidden_imps = 1,
		expected_do_statements  = 1,
	)
}
