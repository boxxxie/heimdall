// =============================================================================
// Heimdall AST Metaprogramming CLI Generator (`tools/heimdall_gen`)
//
// 1. Scans `heimdall/` for `@(telemetry_schema)` structs using `core:odin/parser`
//    + `core:odin/ast` and emits `heimdall/schema_gen.odin` (`make_record_arrow_schema`,
//    `stage_record_into_soa`, `build_arrow_batch`, `Record_Batch_Reader`).
// 2. Scans an ATDD test directory (`dsl_*.odin` and `spec_*.odin`), validates
//    Layer 1 `@(scenario)` purity rules, and emits `<dir>/dsl_atdd_gen.odin`
//    with `@(deferred_out)` RAII `scenario(t)` constructors, `s->step(...)`
//    vtables, and `given.*` / `upon.*` / `then.*` ambient DSL namespaces.
//
// Usage:
//   odin run tools/heimdall_gen -- tests
// =============================================================================

package main

import codegen "../../heimdall/codegen"
import "core:fmt"
import "core:os"

print_and_exit_on_diagnostics :: proc(diags: ^[dynamic]codegen.Codegen_Diagnostic) {
	for d in diags^ {
		if d.line > 0 {
			fmt.eprintf("%s(%d:%d): Error: %s\n", d.file, d.line, d.column, d.message)
		} else {
			fmt.eprintf("%s: Error: %s\n", d.file, d.message)
		}
	}
	codegen.destroy_diagnostics(diags)
	os.exit(1)
}

main :: proc() {
	args := os.args[1:]
	if len(args) > 0 && args[0] == "--atdd" {
		args = args[1:]
	}
	if len(args) == 0 {
		fmt.eprintln("Usage: odin run tools/heimdall_gen -- <tests_dir>")
		os.exit(1)
	}

	if os.is_dir("heimdall") {
		schema_diags := make([dynamic]codegen.Codegen_Diagnostic)
		schema_out, schema_ok := codegen.generate_telemetry_schema_directory("heimdall", "schema_gen.odin", &schema_diags)
		if !schema_ok {
			print_and_exit_on_diagnostics(&schema_diags)
		}
		codegen.destroy_diagnostics(&schema_diags)
		fmt.printfln("Generated Heimdall Telemetry Schema -> %s", schema_out)
		delete(schema_out)
	}

	for target_dir in args {
		if target_dir == "heimdall" {
			continue
		}
		diags := make([dynamic]codegen.Codegen_Diagnostic)
		out_path, ok := codegen.generate_atdd_directory(target_dir, "dsl_atdd_gen.odin", &diags)
		if !ok {
			print_and_exit_on_diagnostics(&diags)
		}
		codegen.destroy_diagnostics(&diags)
		fmt.printfln("Generated Heimdall ATDD DSL -> %s", out_path)
		delete(out_path)
	}
}
