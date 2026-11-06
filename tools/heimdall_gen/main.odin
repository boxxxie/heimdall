// =============================================================================
// Heimdall AST Metaprogramming CLI Generator (`tools/heimdall_gen`)
//
// Scans an ATDD test directory (`dsl_*.odin` and `spec_*.odin`) using
// `core:odin/parser` + `core:odin/ast`, validates Layer 1 `@(scenario)` purity
// rules, and emits `<dir>/dsl_atdd_gen.odin` with `@(deferred_out)` RAII
// `scenario(t)` constructors, `s->step(...)` vtables, and `given.*` /
// `upon.*` / `then.*` ambient DSL namespaces.
//
// Usage:
//   odin run tools/heimdall_gen -- tests
// =============================================================================

package main

import codegen "../../heimdall/codegen"
import "core:fmt"
import "core:os"

main :: proc() {
	args := os.args[1:]
	if len(args) > 0 && args[0] == "--atdd" {
		args = args[1:]
	}
	if len(args) == 0 {
		fmt.eprintln("Usage: odin run tools/heimdall_gen -- <tests_dir>")
		os.exit(1)
	}

	for target_dir in args {
		diags := make([dynamic]codegen.Codegen_Diagnostic)
		out_path, ok := codegen.generate_atdd_directory(target_dir, "dsl_atdd_gen.odin", &diags)
		if !ok {
			for d in diags {
				if d.line > 0 {
					fmt.eprintf("%s(%d:%d): Error: %s\n", d.file, d.line, d.column, d.message)
				} else {
					fmt.eprintf("%s: Error: %s\n", d.file, d.message)
				}
			}
			codegen.destroy_diagnostics(&diags)
			os.exit(1)
		}
		codegen.destroy_diagnostics(&diags)
		fmt.printfln("Generated Heimdall ATDD DSL -> %s", out_path)
		delete(out_path)
	}
}
