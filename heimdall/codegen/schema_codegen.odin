package heimdall_codegen

import "core:fmt"
import "core:mem"
import "core:odin/ast"
import "core:os"
import "core:path/filepath"
import "core:slice"
import "core:strings"

// =============================================================================
// Heimdall `@(telemetry_schema)` AST Serializer Generator & Call-Site Linter
// (`heimdall/codegen/schema_codegen.odin`)
//
// 1. Parses `@(telemetry_schema)` struct declarations (e.g. `Record :: struct`
//    in `heimdall/heimdall.odin`) via `core:odin/parser` and generates
//    `heimdall/schema_gen.odin` containing:
//    - `RECORD_COLUMN_COUNT`
//    - `make_record_arrow_schema` (`oa.Schema` constructor)
//    - `stage_record_into_soa` (`#soa` staging helper)
//    - `build_arrow_batch` (`#soa` -> `OdinArrow` zero-copy column serializer)
//    - `Record_Batch_Reader` (`resolve` + `decode_row` columnar reader)
// 2. Walks instrumented Odin ASTs (`analyze_telemetry_call_sites`) to:
//    - Extract deduplicated static `scope` / `name` string literals for
//      compile-time `@(rodata)` string interning / dictionary encoding
//    - Detect uncloned dynamic string arguments (`fmt.tprintf`, etc.) passed to
//      `scoped_span`, `span_begin`, `counter_add`, `gauge_set`, `histogram_record`,
//      or `event_emit`
//    - Detect misplaced `@(deferred_out)` `scoped_span` calls that appear as the
//      final statement of a block (or sole statement of an `if`/`for` body) where
//      the span would immediately end without wrapping any work
// =============================================================================

Telemetry_Field_Spec :: struct {
	name:           string,
	odin_type:      string,
	arrow_type_lit: string,
	primitive_type: string,
	is_string:      bool,
	is_enum:        bool,
}

Telemetry_Schema_Spec :: struct {
	package_name: string,
	struct_name:  string,
	fields:       [dynamic]Telemetry_Field_Spec,
	ast_arena:    mem.Dynamic_Arena,
}

Telemetry_Call_Site_Report :: struct {
	call_sites_inspected:      int,
	static_string_literals:    int,
	unique_static_scopes:      int,
	unique_static_names:       int,
	unique_static_strings:     int,
	dynamic_string_hazards:    int,
	misplaced_scoped_spans:    int,
	actor_handler_call_sites:  int,
	actor_backend_recommended: bool,
}

init_telemetry_schema_spec :: proc(s: ^Telemetry_Schema_Spec) {
	s.fields = make([dynamic]Telemetry_Field_Spec)
	mem.dynamic_arena_init(&s.ast_arena)
}

destroy_telemetry_schema_spec :: proc(s: ^Telemetry_Schema_Spec) {
	delete(s.fields)
	mem.dynamic_arena_destroy(&s.ast_arena)
}

@(private)
map_odin_type_to_arrow :: proc(
	odin_type: string,
	enum_backing: map[string]string,
) -> (arrow_type_lit, primitive_type: string, is_string, is_enum, ok: bool) {
	switch odin_type {
	case "i64":
		return "oa.Int64_Type{}", "i64", false, false, true
	case "u64":
		return "oa.UInt64_Type{}", "u64", false, false, true
	case "u32":
		return "oa.UInt32_Type{}", "u32", false, false, true
	case "u16":
		return "oa.UInt16_Type{}", "u16", false, false, true
	case "u8":
		return "oa.UInt8_Type{}", "u8", false, false, true
	case "f64":
		return "oa.Float64_Type{}", "f64", false, false, true
	case "string":
		return "oa.String_Type{}", "string", true, false, true
	}
	if backing, is_known_enum := enum_backing[odin_type]; is_known_enum {
		arr_lit, prim, _, _, backing_ok := map_odin_type_to_arrow(backing, nil)
		if backing_ok {
			return arr_lit, prim, false, true, true
		}
	}
	return "", "", false, false, false
}

@(private)
populate_telemetry_schema_from_ast :: proc(
	file: ^ast.File,
	spec: ^Telemetry_Schema_Spec,
	allocator := context.temp_allocator,
) -> bool {
	src := file.src
	if len(spec.package_name) == 0 && file.pkg_name != "" {
		spec.package_name = file.pkg_name
	}

	enum_backing := make(map[string]string, 8, allocator)
	defer delete(enum_backing)

	// Pass 1: Collect enum backing types in the file (e.g. `Kind :: enum u8`, `Status :: enum u8`)
	for decl_stmt in file.decls {
		vd, is_vd := decl_stmt.derived.(^ast.Value_Decl)
		if !is_vd || vd.is_mutable || len(vd.names) == 0 || len(vd.values) == 0 {
			continue
		}
		ident, id_ok := vd.names[0].derived.(^ast.Ident)
		enum_type, is_enum := vd.values[0].derived.(^ast.Enum_Type)
		if id_ok && is_enum && enum_type.base_type != nil {
			enum_backing[ident.name] = node_source(src, enum_type.base_type)
		}
	}

	// Pass 2: Locate `@(telemetry_schema)` struct (or `Record :: struct`)
	for decl_stmt in file.decls {
		vd, is_vd := decl_stmt.derived.(^ast.Value_Decl)
		if !is_vd || vd.is_mutable || len(vd.names) == 0 || len(vd.values) == 0 {
			continue
		}
		ident, id_ok := vd.names[0].derived.(^ast.Ident)
		if !id_ok {
			continue
		}
		_, _, has_schema_attr := get_decl_attribute(vd, "telemetry_schema")
		if !has_schema_attr && ident.name != "Record" {
			continue
		}
		struct_type, is_struct := vd.values[0].derived.(^ast.Struct_Type)
		if !is_struct || struct_type.fields == nil {
			continue
		}

		spec.struct_name = ident.name
		clear(&spec.fields)

		for field in struct_type.fields.list {
			type_str := node_source(src, field.type)
			arr_lit, prim, is_str, is_en, map_ok := map_odin_type_to_arrow(type_str, enum_backing)
			if !map_ok {
				return false
			}
			for name_expr in field.names {
				f_name := node_source(src, name_expr)
				append(&spec.fields, Telemetry_Field_Spec{
					name           = f_name,
					odin_type      = type_str,
					arrow_type_lit = arr_lit,
					primitive_type = prim,
					is_string      = is_str,
					is_enum        = is_en,
				})
			}
		}
		return len(spec.fields) > 0
	}
	return false
}

emit_telemetry_schema_code :: proc(
	spec: ^Telemetry_Schema_Spec,
	allocator := context.allocator,
) -> string {
	sb := strings.builder_make(allocator)
	pkg := spec.package_name if len(spec.package_name) > 0 else "heimdall"
	rec_name := spec.struct_name if len(spec.struct_name) > 0 else "Record"

	fmt.sbprintln(&sb, "// Code generated by heimdall_gen (`core:odin/parser`) `@(telemetry_schema)` generator. DO NOT EDIT.")
	fmt.sbprintf(&sb, "package %s\n\n", pkg)
	fmt.sbprintln(&sb, "import oa \"../deps/OdinArrow/src\"")
	fmt.sbprintln(&sb, "import \"core:mem\"")
	fmt.sbprintln(&sb)

	fmt.sbprintf(&sb, "RECORD_COLUMN_COUNT :: %d\n\n", len(spec.fields))

	// 1. `make_record_arrow_schema`
	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "make_record_arrow_schema :: proc() -> (oa.Schema, mem.Allocator_Error) {")
	fmt.sbprintln(&sb, "\treturn oa.schema_make([]oa.Field{")
	for f in spec.fields {
		fmt.sbprintf(&sb, "\t\toa.field_make(\"%s\", %s),\n", f.name, f.arrow_type_lit)
	}
	fmt.sbprintln(&sb, "\t})")
	fmt.sbprintln(&sb, "}\n")

	// 2. `stage_record_into_soa`
	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "stage_record_into_soa :: #force_inline proc(")
	fmt.sbprintln(&sb, "\tpage: ^Staging_Page,")
	fmt.sbprintln(&sb, "\tidx: int,")
	for f in spec.fields {
		fmt.sbprintf(&sb, "\t%s: %s,\n", f.name, f.odin_type)
	}
	fmt.sbprintln(&sb, ") {")
	for f in spec.fields {
		fmt.sbprintf(&sb, "\tpage.rows.%s[idx] = %s\n", f.name, f.name)
	}
	fmt.sbprintln(&sb, "}\n")

	// 3. `build_arrow_batch`
	fmt.sbprintln(&sb, "@(private)")
	fmt.sbprintln(&sb, "build_arrow_batch :: proc(schema: ^oa.Schema, page: ^Staging_Page) -> (batch: oa.Record_Batch, ok: bool) {")
	fmt.sbprintln(&sb, "\tn := page.count")
	fmt.sbprintln(&sb, "\tcols: [RECORD_COLUMN_COUNT]oa.Array")
	fmt.sbprintln(&sb, "\tbuilt := 0")
	fmt.sbprintln(&sb, "\tdefer if !ok {")
	fmt.sbprintln(&sb, "\t\tfor i in 0 ..< built {")
	fmt.sbprintln(&sb, "\t\t\toa.array_free(&cols[i])")
	fmt.sbprintln(&sb, "\t\t}")
	fmt.sbprintln(&sb, "\t}")
	fmt.sbprintln(&sb)
	for f, idx in spec.fields {
		if f.is_string {
			fmt.sbprintf(&sb, "\tcols[%d] = build_string_column_from_soa(page.rows.%s, n) or_return; built = %d\n", idx, f.name, idx + 1)
		} else if f.is_enum {
			fmt.sbprintf(
				&sb,
				"\tcols[%d] = build_primitive_column_from_soa(%s, %s, cast([^]%s)page.rows.%s, n) or_return; built = %d\n",
				idx,
				f.primitive_type,
				f.arrow_type_lit,
				f.primitive_type,
				f.name,
				idx + 1,
			)
		} else {
			fmt.sbprintf(
				&sb,
				"\tcols[%d] = build_primitive_column_from_soa(%s, %s, page.rows.%s, n) or_return; built = %d\n",
				idx,
				f.primitive_type,
				f.arrow_type_lit,
				f.name,
				idx + 1,
			)
		}
	}
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "\treturn oa.record_batch_make(schema, cols[:])")
	fmt.sbprintln(&sb, "}\n")

	// 4. `Record_Batch_Reader` + `record_batch_reader_resolve` + `record_batch_reader_decode_row`
	fmt.sbprintln(&sb, "Record_Batch_Reader :: struct {")
	for f in spec.fields {
		fmt.sbprintf(&sb, "\t%s_col: ^oa.Array,\n", f.name)
	}
	for f in spec.fields {
		if f.is_string {
			fmt.sbprintf(&sb, "\t%s_offsets: [^]i32,\n", f.name)
			fmt.sbprintf(&sb, "\t%s_bytes: [^]u8,\n", f.name)
			fmt.sbprintf(&sb, "\t%s_bytes_size: int,\n", f.name)
		} else {
			fmt.sbprintf(&sb, "\t%s_data: [^]%s,\n", f.name, f.odin_type)
		}
	}
	fmt.sbprintln(&sb, "}\n")

	fmt.sbprintln(&sb, "record_batch_reader_resolve :: proc(b: ^oa.Record_Batch) -> (reader: Record_Batch_Reader, ok: bool) {")
	fmt.sbprintln(&sb, "\tif b == nil {")
	fmt.sbprintln(&sb, "\t\treturn {}, false")
	fmt.sbprintln(&sb, "\t}")
	fmt.sbprintln(&sb, "\treader = Record_Batch_Reader{")
	for f in spec.fields {
		fmt.sbprintf(&sb, "\t\t%s_col = oa.record_batch_column(b, \"%s\"),\n", f.name, f.name)
	}
	fmt.sbprintln(&sb, "\t}")
	fmt.sbprint(&sb, "\tok = ")
	for f, idx in spec.fields {
		if idx > 0 {
			fmt.sbprint(&sb, " &&\n\t     ")
		}
		if f.is_string {
			fmt.sbprintf(&sb, "validate_record_batch_column(reader.%s_col, b.length, 3, size_of(i32))", f.name)
		} else {
			fmt.sbprintf(&sb, "validate_record_batch_column(reader.%s_col, b.length, 2, size_of(%s))", f.name, f.odin_type)
		}
	}
	fmt.sbprintln(&sb)
	fmt.sbprintln(&sb, "\tif !ok {")
	fmt.sbprintln(&sb, "\t\treturn {}, false")
	fmt.sbprintln(&sb, "\t}")
	for f in spec.fields {
		if f.is_string {
			fmt.sbprintf(&sb, "\tif reader.%s_col.buffers[1].data != nil {{\n\t\treader.%s_offsets = (cast([^]i32)reader.%s_col.buffers[1].data)[reader.%s_col.offset:]\n\t}}\n", f.name, f.name, f.name, f.name)
			fmt.sbprintf(&sb, "\treader.%s_bytes = reader.%s_col.buffers[2].data\n", f.name, f.name)
			fmt.sbprintf(&sb, "\treader.%s_bytes_size = reader.%s_col.buffers[2].size\n", f.name, f.name)
		} else {
			fmt.sbprintf(&sb, "\tif reader.%s_col.buffers[1].data != nil {{\n\t\treader.%s_data = (cast([^]%s)reader.%s_col.buffers[1].data)[reader.%s_col.offset:]\n\t}}\n", f.name, f.name, f.odin_type, f.name, f.name)
		}
	}
	fmt.sbprintln(&sb, "\treturn")
	fmt.sbprintln(&sb, "}\n")

	fmt.sbprintf(&sb, "record_batch_reader_decode_row :: #force_inline proc(reader: ^Record_Batch_Reader, row_idx: int) -> %s {{\n", rec_name)
	fmt.sbprintf(&sb, "\treturn %s{{\n", rec_name)
	for f in spec.fields {
		if f.is_string {
			fmt.sbprintf(&sb, "\t\t%s = decode_string_from_buffers(reader.%s_offsets, reader.%s_bytes, reader.%s_bytes_size, row_idx),\n", f.name, f.name, f.name, f.name)
		} else {
			fmt.sbprintf(&sb, "\t\t%s = reader.%s_data[row_idx],\n", f.name, f.name)
		}
	}
	fmt.sbprintln(&sb, "\t}")
	fmt.sbprintln(&sb, "}")

	return strings.to_string(sb)
}

generate_telemetry_schema_from_source :: proc(
	src:      string,
	filename: string = "custom_schema.odin",
	allocator := context.allocator,
) -> (code: string, column_count: int, ok: bool) {
	spec: Telemetry_Schema_Spec
	init_telemetry_schema_spec(&spec)
	defer destroy_telemetry_schema_spec(&spec)
	ast_alloc := mem.dynamic_arena_allocator(&spec.ast_arena)

	parsed, p_ok := parse_odin_source_into_ast(src, filename, ast_alloc)
	if !p_ok {
		return "", 0, false
	}
	if !populate_telemetry_schema_from_ast(parsed, &spec, context.temp_allocator) {
		return "", 0, false
	}
	return emit_telemetry_schema_code(&spec, allocator), len(spec.fields), true
}

generate_telemetry_schema_directory :: proc(
	sut_dir:      string,
	out_filename: string = "schema_gen.odin",
	diagnostics:  ^[dynamic]Codegen_Diagnostic = nil,
	allocator := context.allocator,
) -> (out_path: string, ok: bool) {
	entries, rerr := os.read_all_directory_by_path(sut_dir, context.allocator)
	if rerr != nil {
		append_diagnostic(diagnostics, sut_dir, 0, 0, "failed to read SUT directory")
		return "", false
	}
	defer os.file_info_slice_delete(entries, context.allocator)
	slice.sort_by(entries, proc(a, b: os.File_Info) -> bool { return a.name < b.name })

	spec: Telemetry_Schema_Spec
	init_telemetry_schema_spec(&spec)
	defer destroy_telemetry_schema_spec(&spec)
	ast_alloc := mem.dynamic_arena_allocator(&spec.ast_arena)

	file_buffers := make([dynamic][]byte)
	defer {
		for buf in file_buffers {
			delete(buf)
		}
		delete(file_buffers)
	}

	found_schema := false
	for entry in entries {
		if entry.type != .Regular || !strings.has_suffix(entry.name, ".odin") || entry.name == out_filename {
			continue
		}
		data, read_err := os.read_entire_file(entry.fullpath, context.allocator)
		if read_err != nil {
			append_diagnostic(diagnostics, entry.fullpath, 0, 0, "failed to read SUT file")
			return "", false
		}
		append(&file_buffers, data)
		parsed, p_ok := parse_odin_source_into_ast(string(data), entry.fullpath, ast_alloc)
		if !p_ok {
			append_diagnostic(diagnostics, entry.fullpath, 1, 1, "failed to parse SUT file")
			return "", false
		}
		if populate_telemetry_schema_from_ast(parsed, &spec, context.temp_allocator) {
			found_schema = true
			break
		}
	}

	if !found_schema {
		append_diagnostic(diagnostics, sut_dir, 1, 1, "no @(telemetry_schema) struct found in SUT directory")
		return "", false
	}

	code := emit_telemetry_schema_code(&spec, context.allocator)
	defer delete(code)

	out_full, _ := filepath.join({sut_dir, out_filename}, allocator)
	if os.write_entire_file_from_string(out_full, code) != nil {
		delete(out_full, allocator)
		append_diagnostic(diagnostics, sut_dir, 0, 0, "failed to write generated schema file")
		return "", false
	}
	return out_full, true
}

// =============================================================================
// Static Call-Site AST Linter & String Interning Catalog Extractor
// =============================================================================

@(private)
is_heimdall_telemetry_call :: proc(expr: ^ast.Expr) -> (proc_name: string, is_scoped_span: bool, ok: bool) {
	if expr == nil {
		return "", false, false
	}
	name := ""
	#partial switch e in expr.derived {
	case ^ast.Ident:
		name = e.name
	case ^ast.Selector_Expr:
		if recv, is_id := e.expr.derived.(^ast.Ident); is_id && recv.name == "heimdall" {
			name = e.field.name
		}
	}
	switch name {
	case "scoped_span":
		return name, true, true
	case "span_begin", "counter_add", "gauge_set", "histogram_record", "event_emit":
		return name, false, true
	}
	return "", false, false
}

@(private)
extract_static_string_arg :: proc(arg: ^ast.Expr, const_strings: map[string]string = nil) -> (lit_val: string, is_static: bool) {
	if arg == nil {
		return "", false
	}
	expr := arg
	if fv, is_fv := expr.derived.(^ast.Field_Value); is_fv {
		expr = fv.value
	}
	for _ in 0 ..< 8 {
		if p, is_p := expr.derived.(^ast.Paren_Expr); is_p {
			expr = p.expr
		} else {
			break
		}
	}
	if lit, is_lit := expr.derived.(^ast.Basic_Lit); is_lit && lit.tok.kind == .String {
		return unquote_ast_string(lit.tok.text), true
	}
	if ident, is_ident := expr.derived.(^ast.Ident); is_ident && const_strings != nil {
		if val, found := const_strings[ident.name]; found {
			return val, true
		}
	}
	return "", false
}

@(private)
collect_const_strings_visit :: proc(visitor: ^ast.Visitor, node: ^ast.Node) -> ^ast.Visitor {
	if node == nil {
		return nil
	}
	const_map := (^map[string]string)(visitor.data)
	if vd, is_vd := node.derived.(^ast.Value_Decl); is_vd && !vd.is_mutable {
		for name_expr, idx in vd.names {
			if idx >= len(vd.values) {
				break
			}
			if ident, is_ident := name_expr.derived.(^ast.Ident); is_ident {
				if lit_val, is_static := extract_static_string_arg(vd.values[idx], const_map^); is_static {
					const_map^[ident.name] = lit_val
				}
			}
		}
	}
	return visitor
}

@(private)
stmt_is_scoped_span_call :: proc(stmt: ^ast.Stmt) -> bool {
	if stmt == nil {
		return false
	}
	#partial switch s in stmt.derived {
	case ^ast.Expr_Stmt:
		if call, is_call := s.expr.derived.(^ast.Call_Expr); is_call {
			_, is_scoped, ok := is_heimdall_telemetry_call(call.expr)
			return ok && is_scoped
		}
	case ^ast.Value_Decl:
		if s.is_mutable && len(s.values) == 1 {
			if call, is_call := s.values[0].derived.(^ast.Call_Expr); is_call {
				_, is_scoped, ok := is_heimdall_telemetry_call(call.expr)
				return ok && is_scoped
			}
		}
	case ^ast.Assign_Stmt:
		if len(s.rhs) == 1 {
			if call, is_call := s.rhs[0].derived.(^ast.Call_Expr); is_call {
				_, is_scoped, ok := is_heimdall_telemetry_call(call.expr)
				return ok && is_scoped
			}
		}
	}
	return false
}

@(private)
expr_has_call_visit :: proc(visitor: ^ast.Visitor, node: ^ast.Node) -> ^ast.Visitor {
	if node == nil {
		return nil
	}
	found := (^bool)(visitor.data)
	if found^ {
		return nil
	}
	if _, is_call := node.derived.(^ast.Call_Expr); is_call {
		found^ = true
		return nil
	}
	return visitor
}

@(private)
stmt_is_immediate_exit :: proc(stmt: ^ast.Stmt) -> bool {
	if stmt == nil {
		return false
	}
	#partial switch s in stmt.derived {
	case ^ast.Branch_Stmt:
		return true
	case ^ast.Return_Stmt:
		has_call := false
		v := ast.Visitor{
			visit = expr_has_call_visit,
			data  = &has_call,
		}
		for res in s.results {
			ast.walk(&v, res)
			if has_call {
				return false
			}
		}
		return true
	}
	return false
}

@(private)
count_misplaced_scoped_spans_in_stmt_list :: proc(stmts: []^ast.Stmt) -> int {
	count := 0
	if len(stmts) > 0 && stmt_is_scoped_span_call(stmts[len(stmts) - 1]) {
		count += 1
	}
	if len(stmts) >= 2 && stmt_is_scoped_span_call(stmts[len(stmts) - 2]) && stmt_is_immediate_exit(stmts[len(stmts) - 1]) {
		count += 1
	}
	return count
}

@(private)
Call_Site_Walk_Ctx :: struct {
	report:           ^Telemetry_Call_Site_Report,
	scope_set:        ^map[string]bool,
	name_set:         ^map[string]bool,
	string_set:       ^map[string]bool,
	const_strings:    ^map[string]string,
	in_actor_handler: bool,
}

@(private)
type_expr_is_sleipnir_actor :: proc(expr: ^ast.Expr) -> bool {
	if expr == nil {
		return false
	}
	#partial switch t in expr.derived {
	case ^ast.Pointer_Type:
		return type_expr_is_sleipnir_actor(t.elem)
	case ^ast.Call_Expr:
		return type_expr_is_sleipnir_actor(t.expr)
	case ^ast.Selector_Expr:
		if pkg, is_id := t.expr.derived.(^ast.Ident); is_id && pkg.name == "actor" {
			return t.field.name == "Context" || t.field.name == "Actor" || t.field.name == "PID"
		}
	}
	return false
}

@(private)
proc_lit_is_sleipnir_actor_handler :: proc(pl: ^ast.Proc_Lit) -> bool {
	if pl == nil || pl.type == nil || pl.type.params == nil {
		return false
	}
	for field in pl.type.params.list {
		if type_expr_is_sleipnir_actor(field.type) {
			return true
		}
	}
	return false
}

@(private)
record_unique_string :: proc(set: ^map[string]bool, val: string) {
	if !(val in set^) {
		key := strings.clone(val, set.allocator)
		set^[key] = true
	}
}

@(private)
record_telemetry_string_arg :: proc(ctx: ^Call_Site_Walk_Ctx, expr: ^ast.Expr, is_scope: bool) {
	const_map: map[string]string = nil
	if ctx.const_strings != nil {
		const_map = ctx.const_strings^
	}
	if lit_val, is_static := extract_static_string_arg(expr, const_map); is_static {
		ctx.report.static_string_literals += 1
		record_unique_string(ctx.string_set, lit_val)
		if is_scope {
			record_unique_string(ctx.scope_set, lit_val)
		} else {
			record_unique_string(ctx.name_set, lit_val)
		}
	} else {
		ctx.report.dynamic_string_hazards += 1
	}
}

@(private)
call_site_visit_node :: proc(visitor: ^ast.Visitor, node: ^ast.Node) -> ^ast.Visitor {
	if node == nil {
		return nil
	}
	ctx := (^Call_Site_Walk_Ctx)(visitor.data)
	#partial switch n in node.derived {
	case ^ast.Proc_Lit:
		if proc_lit_is_sleipnir_actor_handler(n) {
			prev_actor := ctx.in_actor_handler
			ctx.in_actor_handler = true
			if n.body != nil {
				ast.walk(visitor, n.body)
			}
			ctx.in_actor_handler = prev_actor
			return nil
		}
	case ^ast.Defer_Stmt:
		// `defer heimdall.scoped_span(...)` delays `span_begin` until scope exit and measures zero duration!
		if stmt_is_scoped_span_call(n.stmt) {
			ctx.report.misplaced_scoped_spans += 1
		}
	case ^ast.Block_Stmt:
		// If `scoped_span(...)` is the LAST statement of a block (or immediately precedes
		// a trivial `return`, `break`, or `continue`), `@(deferred_out = span_end)` exits
		// immediately without wrapping any work!
		ctx.report.misplaced_scoped_spans += count_misplaced_scoped_spans_in_stmt_list(n.stmts)
	case ^ast.Case_Clause:
		ctx.report.misplaced_scoped_spans += count_misplaced_scoped_spans_in_stmt_list(n.body)
	case ^ast.Call_Expr:
		if _, _, is_tel := is_heimdall_telemetry_call(n.expr); is_tel {
			ctx.report.call_sites_inspected += 1
			if ctx.in_actor_handler {
				ctx.report.actor_handler_call_sites += 1
				ctx.report.actor_backend_recommended = true
			}
			for arg, arg_idx in n.args {
				if fv, is_fv := arg.derived.(^ast.Field_Value); is_fv {
					if ident, ok := fv.field.derived.(^ast.Ident); ok {
						if ident.name == "scope" {
							record_telemetry_string_arg(ctx, fv.value, true)
						} else if ident.name == "name" {
							record_telemetry_string_arg(ctx, fv.value, false)
						}
					}
				} else if arg_idx == 1 || arg_idx == 2 {
					record_telemetry_string_arg(ctx, arg, arg_idx == 1)
				}
			}
		}
	}
	return visitor
}

analyze_telemetry_call_sites :: proc(
	sources:   []string,
	allocator := context.temp_allocator,
) -> (report: Telemetry_Call_Site_Report, ok: bool) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena)
	defer mem.dynamic_arena_destroy(&arena)
	ast_alloc := mem.dynamic_arena_allocator(&arena)

	scope_set  := make(map[string]bool, 16, allocator)
	name_set   := make(map[string]bool, 32, allocator)
	string_set := make(map[string]bool, 32, allocator)
	defer {
		for k in scope_set {
			delete(k, allocator)
		}
		for k in name_set {
			delete(k, allocator)
		}
		for k in string_set {
			delete(k, allocator)
		}
		delete(scope_set)
		delete(name_set)
		delete(string_set)
	}

	const_strings := make(map[string]string, 32, ast_alloc)
	walk_ctx := Call_Site_Walk_Ctx{
		report        = &report,
		scope_set     = &scope_set,
		name_set      = &name_set,
		string_set    = &string_set,
		const_strings = &const_strings,
	}
	const_v := ast.Visitor{
		visit = collect_const_strings_visit,
		data  = &const_strings,
	}
	v := ast.Visitor{
		visit = call_site_visit_node,
		data  = &walk_ctx,
	}

	parsed_files := make([dynamic]^ast.File, 0, len(sources), ast_alloc)
	for src in sources {
		file, p_ok := parse_odin_source_into_ast(src, "telemetry_call_sites.odin", ast_alloc)
		if !p_ok {
			return {}, false
		}
		append(&parsed_files, file)
		ast.walk(&const_v, file)
	}
	// Second pass allows out-of-order constant references across declarations/files (`B :: A; A :: "lit"`).
	for file in parsed_files {
		ast.walk(&const_v, file)
	}
	for file in parsed_files {
		ast.walk(&v, file)
	}

	report.unique_static_scopes  = len(scope_set)
	report.unique_static_names   = len(name_set)
	report.unique_static_strings = len(string_set)
	return report, true
}
