package heimdall_codegen

import "core:fmt"
import "core:mem"
import "core:odin/ast"
import "core:odin/parser"
import "core:odin/tokenizer"
import "core:os"
import "core:path/filepath"
import "core:slice"
import "core:strings"

// =============================================================================
// Heimdall ATDD AST Metaprogramming & 4-Layer Purity Enforcer (`core:odin/parser`)
//
// 1. Scans Layer 2 DSL files (`dsl_*.odin`) for `given_*`, `upon_*`, and `then_*`
//    procedures taking `(s: ^Suite_Ctx, ...)` (with optional `loc := #caller_location`).
// 2. Generates `<dir>/dsl_atdd_gen.odin` containing:
//    - Ambient `@(thread_local)` scenario context management (`current_scenario`)
//    - `@(deferred_out = scenario_scope_end)` RAII `scenario(t)` constructor
//      that automatically initializes `Suite_Ctx` and cleans up `Recorder`,
//      Arrow batches/schemas, and temporary `.arrow` files on scope exit
//    - `Suite_Ctx_VTable` so `s->given_*`, `s->upon_*`, `s->then_*` selector syntax works
//    - `@(rodata)` `given`, `upon`, `then` ambient namespace structs (`Given_DSL`,
//      `Upon_DSL`, `Then_DSL`) and `<step>_ambient` procedures preserving
//      `#caller_location` and default parameter values
// 3. Static AST Purity Linter (`analyze_atdd_sources` / `analyze_atdd_directories`):
//    - Rejects forbidden `OdinArrow`, `sleipnir`, and `heimdall` imports in Layer 1 (`spec_*.odin`)
//    - Rejects non-`@(test)` procedures and direct `heimdall.*` or `driver_*` calls in Layer 1
//    - Rejects raw `testing.expect*` calls in Layer 1 `@(test)` scenario procs
//    - Rejects zero-argument "Macro Theater" `upon` / `then` steps in Layer 1 & Layer 2
//    - Rejects direct `s.*` driver field accesses in Layer 1 `@(test)` scenario procs
//    - Rejects `do` statements (`ast.Block_Stmt.uses_do`) across `tests/` and `heimdall/`
//    - Verifies 100% of defined Layer 2 DSL steps are exercised by Layer 1 specs
// =============================================================================

Codegen_Diagnostic :: struct {
	file:    string,
	line:    int,
	column:  int,
	message: string,
}

destroy_diagnostics :: proc(diags: ^[dynamic]Codegen_Diagnostic) {
	if diags == nil {
		return
	}
	for d in diags^ {
		delete(d.file)
		delete(d.message)
	}
	delete(diags^)
	diags^ = nil
}

ATDD_Param :: struct {
	name:          string,
	type_expr:     string,
	default_expr:  string,
	is_variadic:   bool,
	is_poly_type:  bool,
	is_caller_loc: bool,
}

ATDD_Step_Spec :: struct {
	proc_name:   string, // Canonical step name (e.g. `upon_simulating_http_requests`)
	impl_name:   string, // Actual procedure name in `dsl_*.odin`
	ctx_type:    string,
	has_loc:     bool,
	loc_param:   string,
	return_type: string,
	params:      [dynamic]ATDD_Param, // Parameters AFTER `s: ^Suite_Ctx`
}

ATDD_Module :: struct {
	package_name: string,
	imports:      [dynamic]Import_Decl_Spec,
	steps:        [dynamic]ATDD_Step_Spec,
	ast_arena:    mem.Dynamic_Arena,
}

AST_Contract_Report :: struct {
	parsed_files:           int,
	spec_procs_checked:     int,
	non_test_procs_in_spec: int,
	dsl_steps_defined:      int,
	dsl_steps_called:       int,
	unused_dsl_procedures:  int,
	unknown_dsl_calls:      int,
	direct_sut_calls:       int,
	raw_testing_calls:      int,
	zero_arg_mega_steps:    int,
	driver_field_leaks:     int,
	forbidden_imports:      int,
	do_statements:          int,
}

@(private)
Import_Decl_Spec :: struct {
	alias:    string,
	raw_decl: string,
}

init_atdd_module :: proc(m: ^ATDD_Module) {
	m.imports = make([dynamic]Import_Decl_Spec)
	m.steps   = make([dynamic]ATDD_Step_Spec)
	mem.dynamic_arena_init(&m.ast_arena)
}

destroy_atdd_module :: proc(m: ^ATDD_Module) {
	for &st in m.steps {
		delete(st.params)
	}
	delete(m.steps)
	delete(m.imports)
	mem.dynamic_arena_destroy(&m.ast_arena)
}

@(private)
append_diagnostic :: proc(
	diags: ^[dynamic]Codegen_Diagnostic,
	file: string,
	line, col: int,
	msg: string,
) {
	if diags == nil {
		return
	}
	append(diags, Codegen_Diagnostic{
		file    = strings.clone(file),
		line    = line,
		column  = col,
		message = strings.clone(msg),
	})
}

@(private)
unquote_ast_string :: proc(raw: string) -> string {
	if len(raw) >= 2 {
		if (raw[0] == '"' && raw[len(raw) - 1] == '"') || (raw[0] == '`' && raw[len(raw) - 1] == '`') {
			return raw[1 : len(raw) - 1]
		}
	}
	return raw
}

@(private)
get_decl_attribute :: proc(
	decl: ^ast.Value_Decl,
	attr_name: string,
) -> (val: string, pos: tokenizer.Pos, found: bool) {
	for attr in decl.attributes {
		for elem in attr.elems {
			#partial switch e in elem.derived {
			case ^ast.Ident:
				if e.name == attr_name {
					return "", e.pos, true
				}
			case ^ast.Field_Value:
				if ident, ok := e.field.derived.(^ast.Ident); ok && ident.name == attr_name {
					if lit, lit_ok := e.value.derived.(^ast.Basic_Lit); lit_ok {
						return unquote_ast_string(lit.tok.text), e.pos, true
					}
					if v_ident, v_ok := e.value.derived.(^ast.Ident); v_ok {
						return v_ident.name, e.pos, true
					}
					return "", e.pos, true
				}
			}
		}
	}
	return "", {}, false
}

@(private)
node_source :: proc(src: string, node: ^ast.Node) -> string {
	if node == nil {
		return ""
	}
	start := node.pos.offset
	end   := node.end.offset
	if start >= 0 && end >= start && end <= len(src) {
		return strings.trim_space(src[start:end])
	}
	return ""
}

@(private)
import_local_alias :: proc(imp: ^ast.Import_Decl) -> string {
	if imp.name.text != "" {
		return imp.name.text
	}
	clean := strings.trim(imp.fullpath, "\"")
	if colon := strings.last_index_byte(clean, ':'); colon >= 0 {
		clean = clean[colon + 1:]
	}
	if slash := strings.last_index_byte(clean, '/'); slash >= 0 {
		clean = clean[slash + 1:]
	}
	return clean
}

@(private)
extract_pointer_ident :: proc(expr: ^ast.Expr) -> (string, bool) {
	if expr == nil {
		return "", false
	}
	if ptr, ok := expr.derived.(^ast.Pointer_Type); ok {
		if ident, ident_ok := ptr.elem.derived.(^ast.Ident); ident_ok {
			return ident.name, true
		}
	}
	return "", false
}

@(private)
is_dsl_step_name :: proc(name: string) -> bool {
	return strings.has_prefix(name, "given_") ||
	       strings.has_prefix(name, "upon_") ||
	       strings.has_prefix(name, "then_")
}

@(private)
parse_odin_source_into_ast :: proc(
	src:       string,
	filename:  string,
	allocator: mem.Allocator,
) -> (file: ^ast.File, ok: bool) {
	p := parser.default_parser()
	prev_alloc := context.allocator
	context.allocator = allocator
	defer context.allocator = prev_alloc

	file = ast.new(ast.File, tokenizer.Pos{}, tokenizer.Pos{})
	file.src      = src
	file.fullpath = filename
	ok = parser.parse_file(&p, file) && p.error_count == 0
	return file, ok
}

@(private)
populate_atdd_module_from_dsl_ast :: proc(file: ^ast.File, mod: ^ATDD_Module) {
	src := file.src
	arena_alloc := mem.dynamic_arena_allocator(&mod.ast_arena)
	if len(mod.package_name) == 0 && file.pkg_name != "" {
		mod.package_name = file.pkg_name
	}

	for decl_stmt in file.decls {
		#partial switch d in decl_stmt.derived {
		case ^ast.Import_Decl:
			alias := import_local_alias(d)
			already := false
			for existing in mod.imports {
				if existing.alias == alias {
					already = true
					break
				}
			}
			if !already {
				append(&mod.imports, Import_Decl_Spec{
					alias    = alias,
					raw_decl = node_source(src, d),
				})
			}

		case ^ast.Value_Decl:
			if d.is_mutable || len(d.names) == 0 || len(d.values) == 0 {
				continue
			}
			ident, ok := d.names[0].derived.(^ast.Ident)
			if !ok || !is_dsl_step_name(ident.name) {
				continue
			}
			if _, _, is_priv := get_decl_attribute(d, "private"); is_priv {
				continue
			}

			step_name := strings.trim_suffix(ident.name, "_ctx")

			proc_lit, is_proc := d.values[0].derived.(^ast.Proc_Lit)
			if !is_proc || proc_lit.type == nil || proc_lit.type.params == nil || len(proc_lit.type.params.list) == 0 {
				continue
			}

			fields := proc_lit.type.params.list
			first_field := fields[0]
			ctx_type, ptr_ok := extract_pointer_ident(first_field.type)
			if !ptr_ok || ctx_type != "Suite_Ctx" || len(first_field.names) != 1 {
				continue
			}

			ret_type := ""
			if proc_lit.type.results != nil && len(proc_lit.type.results.list) > 0 {
				res_list := proc_lit.type.results.list
				if len(res_list) == 1 && len(res_list[0].names) == 0 && res_list[0].type != nil {
					ret_type = node_source(src, res_list[0].type)
				} else {
					raw_res := strings.trim_space(node_source(src, proc_lit.type.results))
					if strings.has_prefix(raw_res, "(") {
						ret_type = raw_res
					} else {
						ret_type = fmt.aprintf("(%s)", raw_res, allocator = arena_alloc)
					}
				}
			}

			step := ATDD_Step_Spec{
				proc_name   = step_name,
				impl_name   = ident.name,
				ctx_type    = ctx_type,
				return_type = ret_type,
				params      = make([dynamic]ATDD_Param),
			}

			for f_idx := 1; f_idx < len(fields); f_idx += 1 {
				field := fields[f_idx]
				type_str := node_source(src, field.type)
				def_str  := node_source(src, field.default_value)
				is_ell := false
				if field.type != nil {
					_, is_ell = field.type.derived.(^ast.Ellipsis)
				}
				is_var := is_ell || (.Ellipsis in field.flags) || strings.has_prefix(type_str, "..")
				if is_var && !strings.has_prefix(type_str, "..") {
					type_str = fmt.aprintf("..%s", type_str, allocator = arena_alloc)
				}
				is_poly := strings.contains(type_str, "$")
				is_loc  := def_str == "#caller_location"

				for name_expr in field.names {
					p_name    := node_source(src, name_expr)
					p_is_poly := is_poly || strings.contains(p_name, "$")
					if is_loc {
						step.has_loc   = true
						step.loc_param = p_name
					}
					append(&step.params, ATDD_Param{
						name          = p_name,
						type_expr     = type_str,
						default_expr  = def_str,
						is_variadic   = is_var,
						is_poly_type  = p_is_poly,
						is_caller_loc = is_loc,
					})
				}
			}

			append(&mod.steps, step)
		}
	}
}

@(private)
parse_atdd_dsl_source :: proc(
	src:      string,
	filename: string,
	mod:      ^ATDD_Module,
	diags:    ^[dynamic]Codegen_Diagnostic,
) -> (file: ^ast.File, ok: bool) {
	arena_alloc := mem.dynamic_arena_allocator(&mod.ast_arena)
	file, ok = parse_odin_source_into_ast(src, filename, arena_alloc)
	if !ok {
		append_diagnostic(diags, filename, 1, 1, "failed to parse ATDD DSL file")
		return nil, false
	}
	populate_atdd_module_from_dsl_ast(file, mod)
	return file, true
}

// =============================================================================
// Layer 1 `@(test)` Scenario AST Purity Validator & 4-Layer Contract Analyzer
// =============================================================================

@(private)
validate_spec_report :: proc(
	report:   AST_Contract_Report,
	filename: string,
	diags:    ^[dynamic]Codegen_Diagnostic,
) -> bool {
	valid := true
	if report.forbidden_imports > 0 {
		append_diagnostic(diags, filename, 1, 1, "forbidden Layer 3/4 import in Layer 1 spec file")
		valid = false
	}
	if report.non_test_procs_in_spec > 0 {
		append_diagnostic(diags, filename, 1, 1, "non-@(test) helper procedure in Layer 1 spec file")
		valid = false
	}
	if report.direct_sut_calls > 0 {
		append_diagnostic(diags, filename, 1, 1, "direct SUT or Layer 3 driver call in Layer 1 spec file")
		valid = false
	}
	if report.raw_testing_calls > 0 {
		append_diagnostic(diags, filename, 1, 1, "raw `testing.expect*` call in Layer 1 spec file")
		valid = false
	}
	if report.zero_arg_mega_steps > 0 {
		append_diagnostic(diags, filename, 1, 1, "zero-argument `upon` or `then` mega-step in Layer 1 spec file")
		valid = false
	}
	if report.driver_field_leaks > 0 {
		append_diagnostic(diags, filename, 1, 1, "direct driver field access in Layer 1 spec file")
		valid = false
	}
	if report.do_statements > 0 {
		append_diagnostic(diags, filename, 1, 1, "disallowed `do` statement in Layer 1 spec file")
		valid = false
	}
	if report.unknown_dsl_calls > 0 {
		append_diagnostic(diags, filename, 1, 1, "Layer 1 spec calls an undefined Layer 2 DSL step")
		valid = false
	}
	return valid
}

validate_atdd_scenario_source :: proc(
	src:      string,
	filename: string,
	diags:    ^[dynamic]Codegen_Diagnostic,
) -> bool {
	report, ok := analyze_atdd_sources({src}, "", nil, context.temp_allocator)
	if !ok {
		append_diagnostic(diags, filename, 1, 1, "failed to parse ATDD spec file")
		return false
	}
	return validate_spec_report(report, filename, diags)
}

@(private)
is_allowed_spec_ident_call :: proc(name: string) -> bool {
	if is_dsl_step_name(name) {
		return true
	}
	switch name {
	case "scenario", "current_scenario", "len", "int", "i64", "u64", "u32", "u8", "f64", "bool", "string":
		return true
	}
	return false
}

@(private)
count_non_loc_proc_params :: proc(src: string, proc_lit: ^ast.Proc_Lit) -> int {
	if proc_lit == nil || proc_lit.type == nil || proc_lit.type.params == nil {
		return 0
	}
	total := 0
	for field in proc_lit.type.params.list {
		def_str := node_source(src, field.default_value)
		if def_str == "#caller_location" {
			continue
		}
		total += max(len(field.names), 1)
	}
	return total
}

@(private)
AST_Walk_Context :: struct {
	report:            ^AST_Contract_Report,
	called:            ^map[string]bool,
	in_spec_proc:      bool,
	scenario_var_name: string,
}

@(private)
record_called_dsl_step :: proc(called: ^map[string]bool, canonical: string) {
	if called == nil {
		return
	}
	if canonical in called^ {
		return
	}
	key := strings.clone(canonical, called.allocator)
	called^[key] = true
}

@(private)
is_scenario_ctx_arg :: proc(expr: ^ast.Expr, scenario_var_name: string) -> bool {
	if expr == nil {
		return false
	}
	target := expr
	if fv, is_fv := expr.derived.(^ast.Field_Value); is_fv {
		target = fv.value
	}
	#partial switch e in target.derived {
	case ^ast.Ident:
		return e.name == "s" || (len(scenario_var_name) > 0 && e.name == scenario_var_name)
	case ^ast.Call_Expr:
		if fn_id, ok := e.expr.derived.(^ast.Ident); ok {
			return fn_id.name == "current_scenario"
		}
	}
	return false
}

@(private)
is_caller_loc_call_arg :: proc(expr: ^ast.Expr) -> bool {
	if expr == nil {
		return false
	}
	if fv, is_fv := expr.derived.(^ast.Field_Value); is_fv {
		if id, ok := fv.field.derived.(^ast.Ident); ok && id.name == "loc" {
			return true
		}
	}
	return false
}

@(private)
count_domain_call_args :: proc(args: []^ast.Expr, scenario_var_name: string, strip_ctx: bool) -> int {
	count := 0
	for arg, idx in args {
		if is_caller_loc_call_arg(arg) {
			continue
		}
		if strip_ctx && idx == 0 && is_scenario_ctx_arg(arg, scenario_var_name) {
			continue
		}
		count += 1
	}
	return count
}

@(private)
ast_visit_node :: proc(visitor: ^ast.Visitor, node: ^ast.Node) -> ^ast.Visitor {
	if node == nil {
		return nil
	}
	ctx := (^AST_Walk_Context)(visitor.data)
	#partial switch n in node.derived {
	case ^ast.Block_Stmt:
		if n.uses_do {
			ctx.report.do_statements += 1
		}
	case ^ast.Value_Decl:
		if ctx.in_spec_proc && n.is_mutable && len(n.names) > 0 && len(n.values) > 0 {
			if call, is_call := n.values[0].derived.(^ast.Call_Expr); is_call {
				if fn_id, is_id := call.expr.derived.(^ast.Ident); is_id && fn_id.name == "scenario" {
					if lhs_id, ok := n.names[0].derived.(^ast.Ident); ok && lhs_id.name != "_" {
						ctx.scenario_var_name = lhs_id.name
					}
				}
			}
		}
	case ^ast.Assign_Stmt:
		if ctx.in_spec_proc && len(n.lhs) > 0 && len(n.rhs) > 0 {
			if call, is_call := n.rhs[0].derived.(^ast.Call_Expr); is_call {
				if fn_id, is_id := call.expr.derived.(^ast.Ident); is_id && fn_id.name == "scenario" {
					if lhs_id, ok := n.lhs[0].derived.(^ast.Ident); ok && lhs_id.name != "_" {
						ctx.scenario_var_name = lhs_id.name
					}
				}
			}
		}
	case ^ast.Call_Expr:
		if ctx.in_spec_proc {
			#partial switch fn in n.expr.derived {
			case ^ast.Ident:
				if is_dsl_step_name(fn.name) {
					is_ambient := strings.has_suffix(fn.name, "_ambient")
					canonical := strings.trim_suffix(fn.name, "_ambient")
					canonical = strings.trim_suffix(canonical, "_ctx")
					record_called_dsl_step(ctx.called, canonical)
					ctx.report.dsl_steps_called += 1
					if strings.has_prefix(canonical, "upon_") || strings.has_prefix(canonical, "then_") {
						if count_domain_call_args(n.args, ctx.scenario_var_name, !is_ambient) == 0 {
							ctx.report.zero_arg_mega_steps += 1
						}
					}
				} else if !is_allowed_spec_ident_call(fn.name) {
					ctx.report.direct_sut_calls += 1
				}
			case ^ast.Selector_Expr:
				if recv_ident, ok := fn.expr.derived.(^ast.Ident); ok {
					field_name := fn.field.name
					switch recv_ident.name {
					case "given", "upon", "then":
						buf: [256]byte
						canonical := fmt.bprintf(buf[:], "%s_%s", recv_ident.name, field_name)
						record_called_dsl_step(ctx.called, canonical)
						ctx.report.dsl_steps_called += 1
						if (recv_ident.name == "upon" || recv_ident.name == "then") && count_domain_call_args(n.args, ctx.scenario_var_name, false) == 0 {
							ctx.report.zero_arg_mega_steps += 1
						}
					case "testing":
						ctx.report.raw_testing_calls += 1
					case:
						if fn.op.kind == .Arrow_Right && is_dsl_step_name(field_name) {
							record_called_dsl_step(ctx.called, field_name)
							ctx.report.dsl_steps_called += 1
							if (strings.has_prefix(field_name, "upon_") || strings.has_prefix(field_name, "then_")) && count_domain_call_args(n.args, ctx.scenario_var_name, false) == 0 {
								ctx.report.zero_arg_mega_steps += 1
							}
						} else if len(field_name) > 0 && field_name[0] >= 'a' && field_name[0] <= 'z' {
							ctx.report.direct_sut_calls += 1
						}
					}
				} else if len(fn.field.name) > 0 && fn.field.name[0] >= 'a' && fn.field.name[0] <= 'z' {
					ctx.report.direct_sut_calls += 1
				}
			}
		}
	case ^ast.Selector_Expr:
		if ctx.in_spec_proc && n.op.kind != .Arrow_Right {
			if is_scenario_ctx_arg(n.expr, ctx.scenario_var_name) {
				ctx.report.driver_field_leaks += 1
			}
		}
	}
	return visitor
}

@(private)
count_do_statements_in_file :: proc(file: ^ast.File, report: ^AST_Contract_Report) {
	walk_ctx := AST_Walk_Context{
		report       = report,
		in_spec_proc = false,
	}
	v := ast.Visitor{
		visit = ast_visit_node,
		data  = &walk_ctx,
	}
	ast.walk(&v, file)
}

@(private)
analyze_spec_ast :: proc(
	file:   ^ast.File,
	report: ^AST_Contract_Report,
	called: ^map[string]bool,
) {
	for decl_stmt in file.decls {
		#partial switch d in decl_stmt.derived {
		case ^ast.Import_Decl:
			if unquote_ast_string(d.fullpath) != "core:testing" {
				report.forbidden_imports += 1
			}
		case ^ast.Value_Decl:
			walk_ctx := AST_Walk_Context{
				report            = report,
				called            = called,
				in_spec_proc      = true,
				scenario_var_name = "s",
			}
			v := ast.Visitor{
				visit = ast_visit_node,
				data  = &walk_ctx,
			}
			if len(d.values) == 0 {
				report.non_test_procs_in_spec += 1
				ast.walk(&v, decl_stmt)
				continue
			}
			proc_lit, is_proc := d.values[0].derived.(^ast.Proc_Lit)
			if !is_proc || proc_lit.body == nil {
				report.non_test_procs_in_spec += 1
				ast.walk(&v, decl_stmt)
				continue
			}
			_, _, has_test := get_decl_attribute(d, "test")
			if !has_test {
				report.non_test_procs_in_spec += 1
			} else {
				report.spec_procs_checked += 1
			}
			ast.walk(&v, proc_lit.body)
		}
	}
}

@(private)
analyze_atdd_parsed_files :: proc(
	dsl_files:   []^ast.File,
	spec_files:  []^ast.File,
	extra_files: []^ast.File,
	allocator := context.temp_allocator,
	mod: ^ATDD_Module = nil,
) -> AST_Contract_Report {
	report: AST_Contract_Report
	called_steps  := make(map[string]bool, 64, allocator)
	defined_steps := make([dynamic]string, 0, 64, allocator)
	defined_set   := make(map[string]bool, 64, allocator)
	defer {
		for k in called_steps {
			delete(k, allocator)
		}
		delete(called_steps)
		delete(defined_steps)
		delete(defined_set)
	}

	for dsl_file in dsl_files {
		if dsl_file == nil {
			continue
		}
		report.parsed_files += 1
		count_do_statements_in_file(dsl_file, &report)
		if mod == nil {
			for decl_stmt in dsl_file.decls {
				if vd, is_vd := decl_stmt.derived.(^ast.Value_Decl); is_vd && !vd.is_mutable && len(vd.names) > 0 && len(vd.values) > 0 {
					ident, is_id := vd.names[0].derived.(^ast.Ident)
					if !is_id {
						continue
					}
					#partial switch val_node in vd.values[0].derived {
					case ^ast.Proc_Lit:
						if is_dsl_step_name(ident.name) {
							canonical := strings.trim_suffix(ident.name, "_ctx")
							append(&defined_steps, canonical)
							defined_set[canonical] = true
							report.dsl_steps_defined += 1
							if (strings.has_prefix(canonical, "upon_") || strings.has_prefix(canonical, "then_")) &&
								count_non_loc_proc_params(dsl_file.src, val_node) < 2 {
								report.zero_arg_mega_steps += 1
							}
						}
					}
				}
			}
		}
	}

	if mod != nil {
		for st in mod.steps {
			append(&defined_steps, st.proc_name)
			defined_set[st.proc_name] = true
			report.dsl_steps_defined += 1
			if len(dsl_files) > 0 {
				non_loc_params := 0
				for p in st.params {
					if !p.is_caller_loc {
						non_loc_params += 1
					}
				}
				if (strings.has_prefix(st.proc_name, "upon_") || strings.has_prefix(st.proc_name, "then_")) &&
					non_loc_params == 0 {
					report.zero_arg_mega_steps += 1
				}
			}
		}
	}

	for spec_file in spec_files {
		if spec_file == nil {
			continue
		}
		report.parsed_files += 1
		analyze_spec_ast(spec_file, &report, &called_steps)
	}

	for extra_file in extra_files {
		if extra_file == nil {
			continue
		}
		report.parsed_files += 1
		count_do_statements_in_file(extra_file, &report)
	}

	for step_name in defined_steps {
		if !called_steps[step_name] {
			report.unused_dsl_procedures += 1
		}
	}

	if len(defined_steps) > 0 {
		for called_name in called_steps {
			if !defined_set[called_name] {
				report.unknown_dsl_calls += 1
			}
		}
	}

	return report
}

analyze_atdd_sources :: proc(
	spec_sources:  []string,
	dsl_source:    string,
	extra_sources: []string = nil,
	allocator := context.temp_allocator,
) -> (report: AST_Contract_Report, ok: bool) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena)
	defer mem.dynamic_arena_destroy(&arena)
	ast_alloc := mem.dynamic_arena_allocator(&arena)

	dsl_files   := make([dynamic]^ast.File, 0, 1, allocator)
	spec_files  := make([dynamic]^ast.File, 0, len(spec_sources), allocator)
	extra_files := make([dynamic]^ast.File, 0, len(extra_sources), allocator)
	defer {
		delete(dsl_files)
		delete(spec_files)
		delete(extra_files)
	}

	if len(dsl_source) > 0 {
		dsl_file, dsl_ok := parse_odin_source_into_ast(dsl_source, "dsl_heimdall.odin", ast_alloc)
		if !dsl_ok {
			return {}, false
		}
		append(&dsl_files, dsl_file)
	}

	for src in spec_sources {
		spec_file, s_ok := parse_odin_source_into_ast(src, "spec.odin", ast_alloc)
		if !s_ok {
			return {}, false
		}
		append(&spec_files, spec_file)
	}

	for src in extra_sources {
		extra_file, e_ok := parse_odin_source_into_ast(src, "extra.odin", ast_alloc)
		if !e_ok {
			return {}, false
		}
		append(&extra_files, extra_file)
	}

	return analyze_atdd_parsed_files(dsl_files[:], spec_files[:], extra_files[:], allocator), true
}

analyze_atdd_directories :: proc(
	tests_dir: string,
	sut_dir:   string,
	allocator := context.temp_allocator,
) -> (report: AST_Contract_Report, codegen_matches: bool, ok: bool) {
	t_entries, tr_err := os.read_all_directory_by_path(tests_dir, allocator)
	if tr_err != nil {
		return {}, false, false
	}
	defer os.file_info_slice_delete(t_entries, allocator)

	s_entries, sr_err := os.read_all_directory_by_path(sut_dir, allocator)
	if sr_err != nil {
		return {}, false, false
	}
	defer os.file_info_slice_delete(s_entries, allocator)

	slice.sort_by(t_entries, proc(a, b: os.File_Info) -> bool { return a.name < b.name })
	slice.sort_by(s_entries, proc(a, b: os.File_Info) -> bool { return a.name < b.name })

	mod: ATDD_Module
	init_atdd_module(&mod)
	defer destroy_atdd_module(&mod)
	ast_alloc := mem.dynamic_arena_allocator(&mod.ast_arena)

	dsl_files    := make([dynamic]^ast.File, 0, 2, allocator)
	spec_files   := make([dynamic]^ast.File, 0, 8, allocator)
	extra_files  := make([dynamic]^ast.File, 0, 8, allocator)
	file_buffers := make([dynamic][]byte, 0, 16, allocator)
	defer {
		for buf in file_buffers {
			delete(buf, allocator)
		}
		delete(file_buffers)
		delete(dsl_files)
		delete(spec_files)
		delete(extra_files)
	}
	schema_spec: Telemetry_Schema_Spec
	init_telemetry_schema_spec(&schema_spec)
	defer destroy_telemetry_schema_spec(&schema_spec)

	gen_src        := ""
	schema_gen_src := ""

	for entry in t_entries {
		if entry.type != .Regular || !strings.has_suffix(entry.name, ".odin") {
			continue
		}
		raw, r_err := os.read_entire_file(entry.fullpath, allocator)
		if r_err != nil {
			return {}, false, false
		}
		append(&file_buffers, raw)
		src := string(raw)
		parsed, p_ok := parse_odin_source_into_ast(src, entry.fullpath, ast_alloc)
		if !p_ok {
			return {}, false, false
		}
		if entry.name == "dsl_atdd_gen.odin" {
			gen_src = src
			append(&extra_files, parsed)
		} else if strings.has_prefix(entry.name, "dsl_") {
			append(&dsl_files, parsed)
			populate_atdd_module_from_dsl_ast(parsed, &mod)
		} else if strings.has_prefix(entry.name, "spec_") {
			append(&spec_files, parsed)
		} else {
			append(&extra_files, parsed)
		}
	}

	for entry in s_entries {
		if entry.type != .Regular || !strings.has_suffix(entry.name, ".odin") {
			continue
		}
		raw, r_err := os.read_entire_file(entry.fullpath, allocator)
		if r_err != nil {
			return {}, false, false
		}
		append(&file_buffers, raw)
		src := string(raw)
		parsed, p_ok := parse_odin_source_into_ast(src, entry.fullpath, ast_alloc)
		if !p_ok {
			return {}, false, false
		}
		if entry.name == "schema_gen.odin" {
			schema_gen_src = src
		} else {
			populate_telemetry_schema_from_ast(parsed, &schema_spec, allocator)
		}
		append(&extra_files, parsed)
	}

	report = analyze_atdd_parsed_files(dsl_files[:], spec_files[:], extra_files[:], allocator, &mod)
	expected_gen := emit_atdd_module_code(&mod, allocator)
	defer delete(expected_gen, allocator)
	expected_schema_gen := emit_telemetry_schema_code(&schema_spec, allocator)
	defer delete(expected_schema_gen, allocator)
	codegen_matches = len(gen_src) > 0 && expected_gen == gen_src && len(schema_gen_src) > 0 && expected_schema_gen == schema_gen_src
	return report, codegen_matches, true
}

// =============================================================================
// ATDD Code Emission (`dsl_atdd_gen.odin`)
// =============================================================================

@(private)
is_ident_char :: proc(b: byte) -> bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

@(private)
contains_qualified_ident_prefix :: proc(s, prefix: string) -> bool {
	if len(prefix) == 0 || len(s) < len(prefix) {
		return false
	}
	rem := s
	offset := 0
	for len(rem) >= len(prefix) {
		idx := strings.index(rem, prefix)
		if idx < 0 {
			return false
		}
		abs_idx := offset + idx
		if abs_idx == 0 || !is_ident_char(s[abs_idx - 1]) {
			return true
		}
		step := idx + 1
		rem = rem[step:]
		offset += step
	}
	return false
}

@(private)
is_atdd_import_used :: proc(mod: ^ATDD_Module, alias: string) -> bool {
	if alias == "testing" {
		return true
	}
	buf: [128]byte
	prefix := fmt.bprintf(buf[:], "%s.", alias)
	for st in mod.steps {
		if contains_qualified_ident_prefix(st.return_type, prefix) {
			return true
		}
		for p in st.params {
			if contains_qualified_ident_prefix(p.type_expr, prefix) || contains_qualified_ident_prefix(p.default_expr, prefix) {
				return true
			}
		}
	}
	return false
}

@(private)
format_atdd_param :: proc(sb: ^strings.Builder, p: ATDD_Param) {
	if len(p.type_expr) == 0 && len(p.default_expr) > 0 {
		fmt.sbprintf(sb, "%s := %s", p.name, p.default_expr)
	} else if len(p.default_expr) > 0 {
		fmt.sbprintf(sb, "%s: %s = %s", p.name, p.type_expr, p.default_expr)
	} else {
		fmt.sbprintf(sb, "%s: %s", p.name, p.type_expr)
	}
}

@(private)
step_has_poly_params :: proc(st: ATDD_Step_Spec) -> bool {
	for p in st.params {
		if p.is_poly_type {
			return true
		}
	}
	return false
}

@(private)
emit_atdd_module_code :: proc(mod: ^ATDD_Module, allocator := context.allocator) -> string {
	sb := strings.builder_make(allocator)
	sig_b := strings.builder_make(allocator)
	call_b := strings.builder_make(allocator)
	defer strings.builder_destroy(&sig_b)
	defer strings.builder_destroy(&call_b)
	pkg := mod.package_name if len(mod.package_name) > 0 else "heimdall_tests"

	fmt.sbprintln(&sb, "// Code generated by heimdall_gen (`core:odin/parser`) ATDD DSL generator. DO NOT EDIT.")
	fmt.sbprintf(&sb, "package %s\n\n", pkg)

	for imp in mod.imports {
		if is_atdd_import_used(mod, imp.alias) {
			fmt.sbprintf(&sb, "%s\n", imp.raw_decl)
		}
	}
	fmt.sbprintln(&sb)

	// 1. Ambient `@(thread_local)` Scenario Context + Scoped RAII `@(deferred_out)` Constructor
	fmt.sbprintln(&sb, "// =============================================================================")
	fmt.sbprintln(&sb, "// Ambient Scenario Context & `@(deferred_out)` Scoped RAII Harness")
	fmt.sbprintln(&sb, "// =============================================================================")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "MAX_SCENARIO_DEPTH :: 4")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "@(thread_local)")
	fmt.sbprintln(&sb, "_scenario_stack: [MAX_SCENARIO_DEPTH]Suite_Ctx")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "@(thread_local)")
	fmt.sbprintln(&sb, "_scenario_depth: int")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "@(thread_local)")
	fmt.sbprintln(&sb, "_active_scenario_ctx: ^Suite_Ctx")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "current_scenario :: #force_inline proc(loc := #caller_location) -> ^Suite_Ctx {")
	fmt.sbprintln(&sb, "\tassert(_active_scenario_ctx != nil, \"No active ATDD scenario; call `_ = scenario(t)` first\", loc)")
	fmt.sbprintln(&sb, "\treturn _active_scenario_ctx")
	fmt.sbprintln(&sb, "}")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "Scenario_Handle :: struct {")
	fmt.sbprintln(&sb, "\tusing ctx:    ^Suite_Ctx,")
	fmt.sbprintln(&sb, "\tusing vtable: ^Suite_Ctx_VTable,")
	fmt.sbprintln(&sb, "\tprev_ctx:     ^Suite_Ctx,")
	fmt.sbprintln(&sb, "}")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "@(deferred_out = scenario_scope_end)")
	fmt.sbprintln(&sb, "scenario :: proc(t: ^testing.T, loc := #caller_location) -> Scenario_Handle {")
	fmt.sbprintln(&sb, "\tassert(_active_scenario_ctx == nil, \"Active ATDD scenario already running in this scope; wrap sequential scenarios in { } blocks\", loc)")
	fmt.sbprintln(&sb, "\tassert(_scenario_depth < MAX_SCENARIO_DEPTH, \"MAX_SCENARIO_DEPTH exceeded; wrap sequential scenarios in { } blocks\", loc)")
	fmt.sbprintln(&sb, "\ts := &_scenario_stack[_scenario_depth]")
	fmt.sbprintln(&sb, "\t_scenario_depth += 1")
	fmt.sbprintln(&sb, "\ts^ = {}")
	fmt.sbprintln(&sb, "\tdriver_init(s, t)")
	fmt.sbprintln(&sb, "\tprev := _active_scenario_ctx")
	fmt.sbprintln(&sb, "\t_active_scenario_ctx = s")
	fmt.sbprintln(&sb, "\treturn Scenario_Handle{ctx = s, vtable = &_suite_ctx_vtable, prev_ctx = prev}")
	fmt.sbprintln(&sb, "}")
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "scenario_scope_end :: proc(h: Scenario_Handle) {")
	fmt.sbprintln(&sb, "\t_active_scenario_ctx = h.prev_ctx")
	fmt.sbprintln(&sb, "\tif h.ctx != nil {")
	fmt.sbprintln(&sb, "\t\tdriver_destroy(h.ctx)")
	fmt.sbprintln(&sb, "\t\th.ctx^ = {}")
	fmt.sbprintln(&sb, "\t\t_scenario_depth -= 1")
	fmt.sbprintln(&sb, "\t}")
	fmt.sbprintln(&sb, "}")
	fmt.sbprintln(&sb)

	// 2. VTable struct for `s->given_*`, `s->upon_*`, `s->then_*` selector calls
	fmt.sbprintln(&sb, "Suite_Ctx_VTable :: struct {")
	for st in mod.steps {
		if step_has_poly_params(st) {
			continue
		}
		strings.builder_reset(&sig_b)
		for p in st.params {
			fmt.sbprint(&sig_b, ", ")
			format_atdd_param(&sig_b, p)
		}
		if len(st.return_type) > 0 {
			fmt.sbprintf(&sb, "\t%s: proc(s: ^Suite_Ctx%s) -> %s,\n", st.proc_name, strings.to_string(sig_b), st.return_type)
		} else {
			fmt.sbprintf(&sb, "\t%s: proc(s: ^Suite_Ctx%s),\n", st.proc_name, strings.to_string(sig_b))
		}
	}
	fmt.sbprintln(&sb, "}\n")

	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "@(rodata)")
	fmt.sbprintln(&sb, "_suite_ctx_vtable := Suite_Ctx_VTable{")
	for st in mod.steps {
		if step_has_poly_params(st) {
			continue
		}
		fmt.sbprintf(&sb, "\t%s = %s,\n", st.proc_name, st.impl_name)
	}
	fmt.sbprintln(&sb, "}\n")

	// 3. Emit `Given_DSL`, `Upon_DSL`, and `Then_DSL` ambient namespace structs (`given`, `upon`, `then`)
	prefixes := [3]struct{ pfx, type_name, var_name: string }{
		{"given_", "Given_DSL", "given"},
		{"upon_",  "Upon_DSL",  "upon"},
		{"then_",  "Then_DSL",  "then"},
	}
	for grp in prefixes {
		fmt.sbprintf(&sb, "%s :: struct {{\n", grp.type_name)
		for st in mod.steps {
			if step_has_poly_params(st) || !strings.has_prefix(st.proc_name, grp.pfx) {
				continue
			}
			short := strings.trim_prefix(st.proc_name, grp.pfx)
			strings.builder_reset(&sig_b)
			for p, idx in st.params {
				if idx > 0 {
					fmt.sbprint(&sig_b, ", ")
				}
				format_atdd_param(&sig_b, p)
			}
			if len(st.return_type) > 0 {
				fmt.sbprintf(&sb, "\t%s: proc(%s) -> %s,\n", short, strings.to_string(sig_b), st.return_type)
			} else {
				fmt.sbprintf(&sb, "\t%s: proc(%s),\n", short, strings.to_string(sig_b))
			}
		}
		fmt.sbprintln(&sb, "}\n")

		fmt.sbprintln(&sb, "@(rodata)")
		fmt.sbprintf(&sb, "%s := %s{{\n", grp.var_name, grp.type_name)
		for st in mod.steps {
			if step_has_poly_params(st) || !strings.has_prefix(st.proc_name, grp.pfx) {
				continue
			}
			short := strings.trim_prefix(st.proc_name, grp.pfx)
			fmt.sbprintf(&sb, "\t%s = %s_ambient,\n", short, st.proc_name)
		}
		fmt.sbprintln(&sb, "}\n")
	}

	// 4. Emit `<step>_ambient` procedures & optional `proc { ... }` overload groups
	fmt.sbprintln(&sb, "// =============================================================================")
	fmt.sbprintln(&sb, "// Ambient Step Wrappers & Procedure Overload Groups (for `*_ctx` steps)")
	fmt.sbprintln(&sb, "// =============================================================================")
	fmt.sbprintln(&sb)

	for st in mod.steps {
		amb_buf: [256]byte
		amb_name := fmt.bprintf(amb_buf[:], "%s_ambient", st.proc_name)
		ctx_name := st.impl_name

		strings.builder_reset(&sig_b)
		strings.builder_reset(&call_b)

		seen_variadic := false
		for p, idx in st.params {
			if idx > 0 {
				fmt.sbprint(&sig_b, ", ")
			}
			format_atdd_param(&sig_b, p)

			fmt.sbprint(&call_b, ", ")
			call_name := strings.trim_prefix(p.name, "$")
			if p.is_variadic {
				seen_variadic = true
				fmt.sbprintf(&call_b, "..%s", call_name)
			} else if seen_variadic {
				fmt.sbprintf(&call_b, "%s = %s", call_name, call_name)
			} else {
				fmt.sbprintf(&call_b, "%s", call_name)
			}
		}

		ctx_buf: [128]byte
		ctx_expr := "current_scenario()"
		if st.has_loc && len(st.loc_param) > 0 {
			ctx_expr = fmt.bprintf(ctx_buf[:], "current_scenario(%s)", st.loc_param)
		}

		if len(st.return_type) > 0 {
			fmt.sbprintf(&sb, "%s :: #force_inline proc(%s) -> %s {{\n", amb_name, strings.to_string(sig_b), st.return_type)
			fmt.sbprintf(&sb, "\treturn %s(%s%s)\n", ctx_name, ctx_expr, strings.to_string(call_b))
			fmt.sbprintln(&sb, "}\n")
		} else {
			fmt.sbprintf(&sb, "%s :: #force_inline proc(%s) {{\n", amb_name, strings.to_string(sig_b))
			fmt.sbprintf(&sb, "\t%s(%s%s)\n", ctx_name, ctx_expr, strings.to_string(call_b))
			fmt.sbprintln(&sb, "}\n")
		}

		if st.impl_name != st.proc_name {
			fmt.sbprintf(&sb, "%s :: proc {{\n", st.proc_name)
			fmt.sbprintf(&sb, "\t%s,\n", ctx_name)
			fmt.sbprintf(&sb, "\t%s,\n", amb_name)
			fmt.sbprintln(&sb, "}\n")
		}
	}

	return strings.to_string(sb)
}

generate_atdd_from_sources :: proc(
	dsl_sources:    []string,
	dsl_filenames:  []string = nil,
	spec_sources:   []string = nil,
	spec_filenames: []string = nil,
	diagnostics:    ^[dynamic]Codegen_Diagnostic = nil,
	allocator := context.allocator,
) -> (code: string, ok: bool) {
	mod: ATDD_Module
	init_atdd_module(&mod)
	defer destroy_atdd_module(&mod)
	ast_alloc := mem.dynamic_arena_allocator(&mod.ast_arena)

	all_ok := true
	for src, idx in dsl_sources {
		fname := dsl_filenames[idx] if idx < len(dsl_filenames) else "dsl.odin"
		if _, p_ok := parse_atdd_dsl_source(src, fname, &mod, diagnostics); !p_ok {
			all_ok = false
		}
	}

	for src, idx in spec_sources {
		fname := spec_filenames[idx] if idx < len(spec_filenames) else "spec.odin"
		spec_file, s_ok := parse_odin_source_into_ast(src, fname, ast_alloc)
		if !s_ok {
			append_diagnostic(diagnostics, fname, 1, 1, "failed to parse ATDD spec file")
			all_ok = false
			continue
		}
		spec_mod := &mod if len(mod.steps) > 0 else nil
		report := analyze_atdd_parsed_files(nil, {spec_file}, nil, context.temp_allocator, spec_mod)
		if !validate_spec_report(report, fname, diagnostics) {
			all_ok = false
		}
	}

	if !all_ok {
		return "", false
	}
	return emit_atdd_module_code(&mod, allocator), true
}

generate_atdd_directory :: proc(
	dir_path:     string,
	out_filename: string = "dsl_atdd_gen.odin",
	diagnostics:  ^[dynamic]Codegen_Diagnostic = nil,
	allocator := context.allocator,
) -> (out_path: string, ok: bool) {
	entries, rerr := os.read_all_directory_by_path(dir_path, context.allocator)
	if rerr != nil {
		append_diagnostic(diagnostics, dir_path, 0, 0, "failed to read ATDD test directory")
		return "", false
	}
	defer os.file_info_slice_delete(entries, context.allocator)

	dsl_files  := make([dynamic]string)
	spec_files := make([dynamic]string)
	defer delete(dsl_files)
	defer delete(spec_files)

	for entry in entries {
		if entry.type != .Regular || !strings.has_suffix(entry.name, ".odin") || strings.has_suffix(entry.name, "_gen.odin") || entry.name == out_filename {
			continue
		}
		if strings.has_prefix(entry.name, "dsl_") {
			append(&dsl_files, entry.name)
		} else if strings.has_prefix(entry.name, "spec_") {
			append(&spec_files, entry.name)
		}
	}
	slice.sort(dsl_files[:])
	slice.sort(spec_files[:])

	mod: ATDD_Module
	init_atdd_module(&mod)
	defer destroy_atdd_module(&mod)
	ast_alloc := mem.dynamic_arena_allocator(&mod.ast_arena)

	file_buffers := make([dynamic][]byte)
	defer {
		for buf in file_buffers {
			delete(buf)
		}
		delete(file_buffers)
	}

	all_ok := true
	for name in dsl_files {
		full, _ := filepath.join({dir_path, name}, context.temp_allocator)
		data, read_err := os.read_entire_file(full, context.allocator)
		if read_err != nil {
			append_diagnostic(diagnostics, full, 0, 0, "failed to read ATDD DSL file")
			return "", false
		}
		append(&file_buffers, data)
		if _, p_ok := parse_atdd_dsl_source(string(data), full, &mod, diagnostics); !p_ok {
			all_ok = false
		}
	}

	for name in spec_files {
		full, _ := filepath.join({dir_path, name}, context.temp_allocator)
		data, read_err := os.read_entire_file(full, context.allocator)
		if read_err != nil {
			append_diagnostic(diagnostics, full, 0, 0, "failed to read ATDD spec file")
			return "", false
		}
		append(&file_buffers, data)
		spec_file, s_ok := parse_odin_source_into_ast(string(data), full, ast_alloc)
		if !s_ok {
			append_diagnostic(diagnostics, full, 1, 1, "failed to parse ATDD spec file")
			all_ok = false
			continue
		}
		spec_mod := &mod if len(mod.steps) > 0 else nil
		report := analyze_atdd_parsed_files(nil, {spec_file}, nil, context.temp_allocator, spec_mod)
		if !validate_spec_report(report, full, diagnostics) {
			all_ok = false
		}
	}

	if !all_ok {
		return "", false
	}

	code := emit_atdd_module_code(&mod, context.allocator)
	defer delete(code)

	out_full, _ := filepath.join({dir_path, out_filename}, allocator)
	write_err := os.write_entire_file_from_string(out_full, code)
	if write_err != nil {
		delete(out_full, allocator)
		append_diagnostic(diagnostics, dir_path, 0, 0, "failed to write generated ATDD DSL file")
		return "", false
	}
	return out_full, true
}
