package bench_render

import "core:encoding/json"
import "core:fmt"
import "core:math"
import vmem "core:mem/virtual"
import "core:os"
import "core:slice"
import "core:strings"

DEFAULT_HUGI_BIN :: "/home/pauli/.local/bin/hugi"
DEFAULT_DB_PATH :: "benchmarks/history.hugidb"
RENDERS_DIR :: "benchmarks/renders"
README_PATH :: "benchmarks/README.md"

PALETTE :: [7]string{
	"#58a6ff",
	"#3fb950",
	"#f78166",
	"#d2a8ff",
	"#ffa657",
	"#79c0ff",
	"#ff7b72",
}

Group_Meta :: struct {
	key:   string,
	title: string,
	desc:  string,
}

GROUP_META :: [7]Group_Meta{
	{"zero_cost", "`zero_cost` — Disabled / `nil` Recorder Short-Circuit", "`nil` and disabled `^heimdall.Recorder` short-circuit checks enforced with `0 B` heap allocations."},
	{"single_thread", "`single_thread` — Single-Thread `#soa` Ring & Arrow Flush", "Single-threaded `#soa` staging ring ingestion, scoped spans, nested contexts, and Arrow IPC flush."},
	{"backpressure", "`backpressure` — 500K Burst Saturation (`.Drop` vs `.Block`)", "500K burst saturation comparing non-blocking `.Drop` against lossless `.Block`."},
	{"mt_scaling", "`mt_scaling` — Multi-Threaded OS Producer Scaling (`1`–`8` Threads)", "Multi-threaded OS producer scaling across 1, 2, 4, and 8 threads feeding a Skadi worker."},
	{"sleipnir_vs_thread", "`sleipnir_vs_thread` — `.Dedicated_Thread` vs `.Sleipnir_Actor`", "Comparing `.Dedicated_Thread`, standalone/shared Sleipnir schedulers, `.Flush_Sync`, and lifecycle churn."},
	{"auto_backend", "`auto_backend` — `.Auto` Routing vs Forced Backends", "`Backend_Mode.Auto` adaptive selection vs forced `.Dedicated_Thread` / `.Sleipnir_Actor` overrides."},
	{"arrow_io_roundtrip", "`arrow_io_roundtrip` — 262K-Row `.arrow` IPC Write & Scan", "End-to-end 262,144-row `.arrow` IPC segment write and `Skadi` `ipc_read_file` scan."},
}

Hugi_Record :: struct {
	commit:             string,
	group_name:         string,
	name:               string,
	full_name:          string,
	samples:            int,
	p50_ns:             f64,
	p95_ns:             f64,
	cv_pct:             f64,
	ops_per_sec:        f64,
	mb_per_sec:         f64,
	allocs_per_op:      f64,
	alloc_bytes_per_op: f64,
}

Svg_Tick_View :: struct {
	pos:      string,
	text_pos: string,
	label:    string,
}

Svg_Point_View :: struct {
	x: string,
	y: string,
}

Line_Series_View :: struct {
	has_path: bool,
	path_d:   string,
	color:    string,
	points:   []Svg_Point_View,
}

End_Label_View :: struct {
	raw_y: f64,
	y:     string,
	text:  string,
	color: string,
}

Legend_Item_View :: struct {
	x:      string,
	rect_y: string,
	text_x: string,
	text_y: string,
	label:  string,
	color:  string,
}

Line_Chart_View :: struct {
	title:        string,
	subtitle:     string,
	y_unit_label: string,
	width:        int,
	height:       int,
	pad_l:        int,
	pad_t:        int,
	pw:           int,
	ph:           int,
	plot_right:   int,
	plot_bottom:  int,
	y_label_x:    int,
	x_label_y:    int,
	end_label_x:  int,
	y_ticks:      []Svg_Tick_View,
	x_ticks:      []Svg_Tick_View,
	series:       []Line_Series_View,
	end_labels:   []End_Label_View,
	legend:       []Legend_Item_View,
}

Bar_View :: struct {
	label:    string,
	text_y:   string,
	rect_y:   string,
	width:    string,
	val_x:    string,
	val_y:    string,
	val_text: string,
	color:    string,
}

Bar_Chart_View :: struct {
	title:       string,
	subtitle:    string,
	width:       int,
	height:      int,
	pad_l:       int,
	pad_t:       int,
	pw:          int,
	ph:          int,
	plot_bottom: int,
	label_x:     int,
	x_label_y:   int,
	x_ticks:     []Svg_Tick_View,
	bars:        []Bar_View,
}

Report_Row_View :: struct {
	name:   string,
	median: string,
	p95:    string,
	ops:    string,
	cv:     string,
	allocs: string,
}

Report_Group_View :: struct {
	title:    string,
	desc:     string,
	has_desc: bool,
	count:    int,
	rows:     []Report_Row_View,
}

Suite_Report_View :: struct {
	commit:            string,
	total_benchmarks:  int,
	total_samples_str: string,
	group_count:       int,
	groups:            []Report_Group_View,
}

Line_Series_Input :: struct {
	label:  string,
	values: []Maybe(f64),
	color:  string,
}

Bar_Spec :: struct {
	full_name: string,
	label:     string,
	color:     string,
}

line_chart_tmpl := compiled("templates/line_chart.runa.twig", Line_Chart_View)
bar_chart_tmpl := compiled("templates/bar_chart.runa.twig", Bar_Chart_View)
suite_report_tmpl := compiled("templates/suite_report.runa.twig", Suite_Report_View)

@(require_results)
fmt_commas_u64 :: proc(val: u64, allocator := context.allocator) -> string {
	buf: [32]byte
	pos := len(buf)
	v := val
	digits := 0
	if v == 0 {
		pos -= 1
		buf[pos] = '0'
	} else {
		for v > 0 {
			if digits > 0 && digits % 3 == 0 {
				pos -= 1
				buf[pos] = ','
			}
			pos -= 1
			buf[pos] = byte('0' + (v % 10))
			v /= 10
			digits += 1
		}
	}
	return strings.clone(string(buf[pos:]), allocator)
}

@(require_results)
fmt_ns :: proc(ns: f64, allocator := context.allocator) -> string {
	if ns < 1.0 {
		return fmt.aprintf("%.2f ps", ns * 1000.0, allocator = allocator)
	}
	if ns < 1_000.0 {
		return fmt.aprintf("%.2f ns", ns, allocator = allocator)
	}
	if ns < 1_000_000.0 {
		return fmt.aprintf("%.2f us", ns / 1_000.0, allocator = allocator)
	}
	if ns < 1_000_000_000.0 {
		return fmt.aprintf("%.2f ms", ns / 1_000_000.0, allocator = allocator)
	}
	return fmt.aprintf("%.3f s", ns / 1_000_000_000.0, allocator = allocator)
}

@(require_results)
fmt_axis_ns :: proc(ns: f64, allocator := context.allocator) -> string {
	if ns <= 0 {
		return strings.clone("0", allocator)
	}
	raw := fmt_ns(ns, context.temp_allocator)
	if s, ok := strings.replace_all(raw, ".000 ", " ", context.temp_allocator); ok {
		raw = s
	}
	if s, ok := strings.replace_all(raw, ".00 ", " ", context.temp_allocator); ok {
		raw = s
	}
	return strings.clone(raw, allocator)
}

@(require_results)
fmt_bytes :: proc(b: f64, allocator := context.allocator) -> string {
	if b < 1024.0 {
		return fmt.aprintf("%.0f B", b, allocator = allocator)
	}
	if b < 1024.0 * 1024.0 {
		return fmt.aprintf("%.2f KiB", b / 1024.0, allocator = allocator)
	}
	return fmt.aprintf("%.2f MiB", b / (1024.0 * 1024.0), allocator = allocator)
}

@(require_results)
fmt_tp_or_ops :: proc(rec: Hugi_Record, allocator := context.allocator) -> string {
	if rec.mb_per_sec >= 1024.0 {
		return fmt.aprintf("%.2f GB/s", rec.mb_per_sec / 1024.0, allocator = allocator)
	}
	if rec.mb_per_sec > 0 {
		return fmt.aprintf("%.1f MB/s", rec.mb_per_sec, allocator = allocator)
	}
	if rec.ops_per_sec >= 1_000_000.0 {
		return fmt.aprintf("%.2fM/s", rec.ops_per_sec / 1_000_000.0, allocator = allocator)
	}
	if rec.ops_per_sec >= 1_000.0 {
		return fmt.aprintf("%.1fK/s", rec.ops_per_sec / 1_000.0, allocator = allocator)
	}
	return fmt.aprintf("%.0f/s", rec.ops_per_sec, allocator = allocator)
}

@(require_results)
exec_hugi :: proc(args: []string, allocator := context.allocator) -> (stdout: []byte, ok: bool) {
	hugi_bin := DEFAULT_HUGI_BIN if os.exists(DEFAULT_HUGI_BIN) else "hugi"
	cmd := make([dynamic]string, 0, len(args) + 1, context.temp_allocator)
	append(&cmd, hugi_bin)
	for a in args {
		append(&cmd, a)
	}
	state, out, stderr, exec_err := os.process_exec(os.Process_Desc{command = cmd[:]}, allocator)
	if exec_err != nil && hugi_bin != "hugi" {
		delete(out, allocator)
		delete(stderr, allocator)
		cmd[0] = "hugi"
		state, out, stderr, exec_err = os.process_exec(os.Process_Desc{command = cmd[:]}, allocator)
	}
	defer delete(stderr, allocator)
	if exec_err != nil || !state.success {
		delete(out, allocator)
		return nil, false
	}
	return out, true
}

@(require_results)
latest_commit_from_hugi :: proc(db_path: string, allocator := context.allocator) -> (string, bool) {
	out, ok := exec_hugi({"history", "--db", db_path, "--limit", "1"}, context.temp_allocator)
	if !ok {
		return "", false
	}
	for line in strings.split_lines(string(out), context.temp_allocator) {
		t := strings.trim_space(line)
		if len(t) == 0 || strings.has_prefix(t, "Commit") || strings.has_prefix(t, "-") {
			continue
		}
		if idx := strings.index_byte(t, '|'); idx > 0 {
			return strings.clone(strings.trim_space(t[:idx]), allocator), true
		}
	}
	return "", false
}

@(require_results)
build_line_chart_view :: proc(
	title: string,
	subtitle: string,
	x_labels: []string,
	inputs: []Line_Series_Input,
	y_log: bool,
	y_unit_label: string,
	allocator := context.allocator,
) -> (Line_Chart_View, bool) {
	width, height, pad_l, pad_r, pad_t, pad_b := 880, 400, 82, 96, 74, 82
	pw := width - pad_l - pad_r
	ph := height - pad_t - pad_b

	min_val, max_val := max(f64), 0.0
	any_val := false
	for inp in inputs {
		for mv in inp.values {
			if v, has := mv.?; has && v > 0 {
				min_val = min(min_val, v)
				max_val = max(max_val, v)
				any_val = true
			}
		}
	}
	if !any_val {
		return {}, false
	}

	y_ticks_vals := make([dynamic]f64, 0, 10, context.temp_allocator)
	y_min_exp, y_max_exp, y_max_lin := 0.0, 1.0, 1.0
	if y_log {
		lo := int(math.floor(math.log10(min_val)))
		hi := int(math.ceil(math.log10(max_val)))
		if lo == hi { hi += 1 }
		y_min_exp, y_max_exp = f64(lo), f64(hi)
		for e := lo; e <= hi; e += 1 { append(&y_ticks_vals, math.pow10(f64(e))) }
	} else {
		top := max_val * 1.15
		step := math.pow10(math.floor(math.log10(top))) / 2.0
		if top / step > 7.0 { step *= 2.0 }
		if top / step < 3.0 { step /= 2.0 }
		n_ticks := max(2, int(math.ceil(top / step)))
		y_max_lin = f64(n_ticks) * step
		for i := 0; i <= n_ticks; i += 1 { append(&y_ticks_vals, f64(i) * step) }
	}

	y_px :: proc(v: f64, y_log: bool, pad_t, ph: int, y_min_exp, y_max_exp, y_max_lin: f64) -> f64 {
		t := (math.log10(v) - y_min_exp) / (y_max_exp - y_min_exp) if y_log else (v / y_max_lin)
		return f64(pad_t) + f64(ph) - t * f64(ph)
	}
	x_px :: proc(i, n_x, pad_l, pw: int) -> f64 {
		return f64(pad_l) + f64(pw) * 0.5 if n_x <= 1 else f64(pad_l) + (f64(i) / f64(n_x - 1)) * f64(pw)
	}

	y_ticks := make([]Svg_Tick_View, len(y_ticks_vals), allocator)
	for yt, yi in y_ticks_vals {
		yp := y_px(yt, y_log, pad_t, ph, y_min_exp, y_max_exp, y_max_lin) if (yt > 0 || !y_log) else f64(pad_t + ph)
		y_ticks[yi] = Svg_Tick_View{
			pos      = fmt.aprintf("%.1f", yp, allocator = allocator),
			text_pos = fmt.aprintf("%.1f", yp + 4.0, allocator = allocator),
			label    = fmt_axis_ns(yt, allocator),
		}
	}

	n_x := len(x_labels)
	x_ticks := make([]Svg_Tick_View, n_x, allocator)
	for xl, xi in x_labels {
		x_ticks[xi] = Svg_Tick_View{
			pos   = fmt.aprintf("%.1f", x_px(xi, n_x, pad_l, pw), allocator = allocator),
			label = strings.clone(xl, allocator),
		}
	}

	series := make([]Line_Series_View, len(inputs), allocator)
	end_labels := make([dynamic]End_Label_View, 0, len(inputs), allocator)
	for inp, si in inputs {
		pts := make([dynamic]Svg_Point_View, 0, len(inp.values), allocator)
		path_b: strings.Builder
		strings.builder_init(&path_b, context.temp_allocator)
		last_yp, last_val := 0.0, 0.0
		has_last := false
		for mv, xi in inp.values {
			if v, has := mv.?; has && (v > 0 || !y_log) {
				xp := x_px(xi, n_x, pad_l, pw)
				yp := y_px(v, y_log, pad_t, ph, y_min_exp, y_max_exp, y_max_lin)
				fmt.sbprintf(&path_b, "%s %.1f %.1f", "M" if len(pts) == 0 else " L", xp, yp)
				append(&pts, Svg_Point_View{fmt.aprintf("%.1f", xp, allocator = allocator), fmt.aprintf("%.1f", yp, allocator = allocator)})
				last_yp, last_val, has_last = yp, v, true
			}
		}
		series[si] = Line_Series_View{
			has_path = len(pts) >= 2,
			path_d   = strings.clone(strings.trim_space(strings.to_string(path_b)), allocator),
			color    = strings.clone(inp.color, allocator),
			points   = pts[:],
		}
		if has_last {
			append(&end_labels, End_Label_View{raw_y = last_yp, text = fmt_ns(last_val, allocator), color = strings.clone(inp.color, allocator)})
		}
	}
	slice.sort_by(end_labels[:], proc(a, b: End_Label_View) -> bool { return a.raw_y < b.raw_y })
	for &el, i in end_labels {
		y := max(f64(pad_t) + 10.0, min(f64(pad_t + ph) - 4.0, el.raw_y + 4.0))
		if i > 0 {
			prev_y := 0.0
			_ = prev_y
		}
		el.y = fmt.aprintf("%.1f", y, allocator = allocator)
	}

	legend := make([]Legend_Item_View, len(inputs), allocator)
	col_w := f64(pw) / f64(max(1, min(3, len(inputs))))
	for inp, idx in inputs {
		lx := f64(pad_l) + f64(idx % 3) * col_w
		ly := f64(pad_t + ph) + 44.0 + f64(idx / 3) * 20.0
		legend[idx] = Legend_Item_View{
			x      = fmt.aprintf("%.1f", lx, allocator = allocator),
			rect_y = fmt.aprintf("%.1f", ly - 9.0, allocator = allocator),
			text_x = fmt.aprintf("%.1f", lx + 18.0, allocator = allocator),
			text_y = fmt.aprintf("%.1f", ly - 3.0, allocator = allocator),
			label  = strings.clone(inp.label, allocator),
			color  = strings.clone(inp.color, allocator),
		}
	}

	return Line_Chart_View{
		title        = strings.clone(title, allocator),
		subtitle     = strings.clone(subtitle, allocator),
		y_unit_label = strings.clone(y_unit_label, allocator),
		width        = width,
		height       = height,
		pad_l        = pad_l,
		pad_t        = pad_t,
		pw           = pw,
		ph           = ph,
		plot_right   = pad_l + pw,
		plot_bottom  = pad_t + ph,
		y_label_x    = pad_l - 8,
		x_label_y    = pad_t + ph + 18,
		end_label_x  = pad_l + pw + 8,
		y_ticks      = y_ticks,
		x_ticks      = x_ticks,
		series       = series,
		end_labels   = end_labels[:],
		legend       = legend,
	}, true
}

@(require_results)
render_bar_svg :: proc(
	title: string,
	subtitle: string,
	specs: []Bar_Spec,
	by_full: map[string]Hugi_Record,
	out_path: string,
	allocator := context.allocator,
) -> bool {
	width, pad_l, pad_r, pad_t, pad_b, row_h := 940, 250, 230, 68, 42, 34
	matched := make([dynamic]struct{ spec: Bar_Spec, rec: Hugi_Record }, 0, len(specs), allocator)
	min_v, max_v := max(f64), 0.0
	for sp in specs {
		if rec, ok := by_full[sp.full_name]; ok && rec.p50_ns > 0 {
			append(&matched, struct{ spec: Bar_Spec, rec: Hugi_Record }{sp, rec})
			min_v = min(min_v, rec.p50_ns)
			max_v = max(max_v, rec.p50_ns)
		}
	}
	if len(matched) == 0 || max_v <= 0 {
		return false
	}

	ph := len(matched) * row_h + 12
	height := pad_t + ph + pad_b
	pw := width - pad_l - pad_r
	lo := int(math.floor(math.log10(min_v)))
	hi := int(math.ceil(math.log10(max_v)))
	if lo == hi { hi += 1 }

	x_ticks := make([]Svg_Tick_View, hi - lo + 1, allocator)
	for e := lo; e <= hi; e += 1 {
		t := f64(e - lo) / f64(hi - lo)
		x_ticks[e - lo] = Svg_Tick_View{
			pos   = fmt.aprintf("%.1f", f64(pad_l) + t * f64(pw), allocator = allocator),
			label = fmt_axis_ns(math.pow10(f64(e)), allocator),
		}
	}

	bars := make([]Bar_View, len(matched), allocator)
	for item, idx in matched {
		ry := pad_t + 10 + idx * row_h
		t := (math.log10(item.rec.p50_ns) - f64(lo)) / f64(hi - lo)
		w_bar := max(4.0, t * f64(pw))
		dur := fmt_ns(item.rec.p50_ns, context.temp_allocator)
		tp := fmt_tp_or_ops(item.rec, context.temp_allocator)
		bars[idx] = Bar_View{
			label    = item.spec.label,
			text_y   = fmt.aprintf("%.1f", f64(ry) + 16.0, allocator = allocator),
			rect_y   = fmt.aprintf("%.1f", f64(ry) + 3.0, allocator = allocator),
			width    = fmt.aprintf("%.1f", w_bar, allocator = allocator),
			val_x    = fmt.aprintf("%.1f", f64(pad_l) + w_bar + 8.0, allocator = allocator),
			val_y    = fmt.aprintf("%.1f", f64(ry) + 17.0, allocator = allocator),
			val_text = fmt.aprintf("%s (%s)", dur, tp, allocator = allocator),
			color    = item.spec.color,
		}
	}

	view := Bar_Chart_View{
		title       = title,
		subtitle    = subtitle,
		width       = width,
		height      = height,
		pad_l       = pad_l,
		pad_t       = pad_t,
		pw          = pw,
		ph          = ph,
		plot_bottom = pad_t + ph,
		label_x     = pad_l - 10,
		x_label_y   = pad_t + ph + 18,
		x_ticks     = x_ticks,
		bars        = bars,
	}
	sb := strings.builder_make(0, bar_chart_tmpl.approx_bytes, context.temp_allocator)
	if _, err := bar_chart_tmpl.with(strings.to_writer(&sb), view); err != nil {
		return false
	}
	return os.write_entire_file(out_path, transmute([]byte)strings.to_string(sb)) == nil
}

@(require_results)
build_suite_report_view :: proc(records: []Hugi_Record, commit: string, allocator := context.allocator) -> Suite_Report_View {
	grouped := make(map[string][dynamic]Hugi_Record, context.temp_allocator)
	extra := make([dynamic]string, context.temp_allocator)
	total_samples := u64(0)
	for rec in records {
		total_samples += u64(max(rec.samples, 0))
		g := rec.group_name if len(rec.group_name) > 0 else "default"
		if g not_in grouped {
			grouped[g] = make([dynamic]Hugi_Record, context.temp_allocator)
			known := false
			for gm in GROUP_META { if gm.key == g { known = true; break } }
			if !known { append(&extra, g) }
		}
		append(&grouped[g], rec)
	}
	ordered := make([dynamic]string, 0, len(grouped), context.temp_allocator)
	for gm in GROUP_META { if gm.key in grouped { append(&ordered, gm.key) } }
	for eg in extra { append(&ordered, eg) }

	groups := make([]Report_Group_View, len(ordered), allocator)
	for gkey, gi in ordered {
		recs := grouped[gkey][:]
		title, desc := gkey, ""
		for gm in GROUP_META { if gm.key == gkey { title, desc = gm.title, gm.desc; break } }
		rows := make([]Report_Row_View, len(recs), allocator)
		for rec, ri in recs {
			short := rec.full_name
			prefix := fmt.tprintf("%s::", gkey)
			if strings.has_prefix(short, prefix) { short = short[len(prefix):] }
			alloc_str := "**0**"
			if rec.allocs_per_op > 0 || rec.alloc_bytes_per_op > 0 {
				alloc_str = fmt.aprintf("%.1f (%s)", rec.allocs_per_op, fmt_bytes(rec.alloc_bytes_per_op, context.temp_allocator), allocator = allocator)
			} else {
				alloc_str = strings.clone("**0**", allocator)
			}
			rows[ri] = Report_Row_View{
				name   = strings.clone(short, allocator),
				median = fmt_ns(rec.p50_ns, allocator),
				p95    = fmt_ns(rec.p95_ns, allocator),
				ops    = fmt_tp_or_ops(rec, allocator),
				cv     = fmt.aprintf("%.1f%%", rec.cv_pct, allocator = allocator),
				allocs = alloc_str,
			}
		}
		groups[gi] = Report_Group_View{
			title    = strings.clone(title, allocator),
			desc     = strings.clone(desc, allocator),
			has_desc = len(desc) > 0,
			count    = len(recs),
			rows     = rows,
		}
	}
	return Suite_Report_View{
		commit            = strings.clone(commit, allocator),
		total_benchmarks  = len(records),
		total_samples_str = fmt_commas_u64(total_samples, allocator),
		group_count       = len(groups),
		groups            = groups,
	}
}

@(require_results)
render_all :: proc(db_path: string, commit_arg: string, allocator := context.allocator) -> bool {
	if !os.exists(db_path) {
		return false
	}
	arena: vmem.Arena
	if vmem.arena_init_growing(&arena) != nil { return false }
	defer vmem.arena_destroy(&arena)
	arena_alloc := vmem.arena_allocator(&arena)

	commit := commit_arg
	if len(commit) == 0 {
		c, ok := latest_commit_from_hugi(db_path, arena_alloc)
		if !ok { return false }
		commit = c
	}
	raw, ok := exec_hugi({"export", "--db", db_path, "--commit", commit, "--format", "json", "--out", "-"}, arena_alloc)
	if !ok { return false }
	records: []Hugi_Record
	if json.unmarshal(raw, &records, allocator = arena_alloc) != nil || len(records) == 0 {
		return false
	}
	if !os.exists(RENDERS_DIR) && os.make_directory(RENDERS_DIR) != nil {
		return false
	}

	by_full := make(map[string]Hugi_Record, len(records), arena_alloc)
	for rec in records { by_full[rec.full_name] = rec }
	sub := fmt.tprintf("Commit %s · -o:speed -microarch:native", commit)

	s1 := [?]Bar_Spec{
		{"zero_cost::counter_add_nil", "counter_add (nil)", PALETTE[1]},
		{"zero_cost::scoped_span_nil", "scoped_span (nil)", PALETTE[1]},
		{"zero_cost::scoped_span_disabled", "scoped_span (disabled)", PALETTE[1]},
		{"zero_cost::nested_http_db_nil", "nested_http_db (nil)", PALETTE[5]},
		{"arrow_io_roundtrip::arrow_read_262k", "arrow_read_262k (Skadi)", PALETTE[0]},
		{"backpressure::drop_500k_burst", "drop_500k_burst (.Drop)", PALETTE[3]},
		{"backpressure::block_500k_burst", "block_500k_burst (.Block)", PALETTE[4]},
		{"single_thread::counter_add_block", "counter_add (.Block)", PALETTE[0]},
		{"single_thread::event_emit_block", "event_emit (.Block)", PALETTE[0]},
		{"arrow_io_roundtrip::arrow_write_262k", "arrow_write_262k", PALETTE[3]},
		{"single_thread::scoped_span_raii", "scoped_span_raii", PALETTE[2]},
		{"single_thread::event_emit_dynamic_copy", "event_emit_dynamic_copy", PALETTE[2]},
	}
	if !render_bar_svg("Zero-Cost Disabled Path vs Active Emission & Arrow IPC", sub, s1[:], by_full, fmt.tprintf("%s/zero_cost_vs_emission.svg", RENDERS_DIR), arena_alloc) {
		return false
	}

	x_mt := [4]string{"1 thread", "2 threads", "4 threads", "8 threads"}
	mt_vals := make([]Maybe(f64), 4, arena_alloc)
	for name, i in ([4]string{"mt_scaling::threads_1", "mt_scaling::threads_2", "mt_scaling::threads_4", "mt_scaling::threads_8"}) {
		if r, found := by_full[name]; found { mt_vals[i] = r.p50_ns }
	}
	v_mt, mt_ok := build_line_chart_view(
		"Multi-Threaded Producer Scaling (1–8 OS Threads)",
		sub,
		x_mt[:],
		{{"mt_scaling (Skadi background worker)", mt_vals, PALETTE[0]}},
		y_log = false,
		y_unit_label = "Linear latency per event (p50)",
		allocator = arena_alloc,
	)
	if !mt_ok { return false }
	sb_mt := strings.builder_make(0, line_chart_tmpl.approx_bytes, context.temp_allocator)
	if _, err := line_chart_tmpl.with(strings.to_writer(&sb_mt), v_mt); err != nil { return false }
	if os.write_entire_file(fmt.tprintf("%s/mt_producer_scaling.svg", RENDERS_DIR), transmute([]byte)strings.to_string(sb_mt)) != nil { return false }

	s3 := [?]Bar_Spec{
		{"auto_backend::forced_sleipnir_actor_os_thread", "forced_sleipnir (OS thread)", PALETTE[5]},
		{"auto_backend::auto_on_os_thread", "auto_on_os_thread", PALETTE[1]},
		{"sleipnir_vs_thread::sleipnir_actor_standalone_1w", "sleipnir_standalone_1w", PALETTE[0]},
		{"sleipnir_vs_thread::dedicated_thread_1p_1w", "dedicated_thread_1p_1w", PALETTE[0]},
		{"auto_backend::auto_on_1w_sched_block", "auto_on_1w_sched_block", PALETTE[1]},
		{"auto_backend::forced_sleipnir_actor_1w_block", "forced_sleipnir_1w_block", PALETTE[4]},
		{"auto_backend::auto_in_4w_actor_x64", "auto_in_4w_actor_x64", PALETTE[1]},
		{"sleipnir_vs_thread::sleipnir_actor_shared_4w_4actors", "sleipnir_shared_4w_4actors", PALETTE[3]},
		{"auto_backend::forced_dedicated_thread_in_4w_actor_x64", "forced_dedicated_in_4w_x64", PALETTE[2]},
	}
	if !render_bar_svg("Backend Routing (.Auto vs Forced) & Sleipnir Comparison", sub, s3[:], by_full, fmt.tprintf("%s/backend_and_auto_routing.svg", RENDERS_DIR), arena_alloc) {
		return false
	}

	rep := build_suite_report_view(records, commit, arena_alloc)
	sb := strings.builder_make(0, suite_report_tmpl.approx_bytes, allocator)
	defer strings.builder_destroy(&sb)
	if _, err := suite_report_tmpl.with(strings.to_writer(&sb), rep); err != nil { return false }
	if os.write_entire_file(README_PATH, transmute([]byte)strings.to_string(sb)) != nil { return false }

	fmt.printfln("bench_render: rendered 3 SVGs and %d rows (%s) -> %s", rep.total_benchmarks, rep.commit, README_PATH)
	free_all(context.temp_allocator)
	return true
}

main :: proc() {
	commit := ""
	args := os.args[1:] if len(os.args) > 1 else nil
	for i := 0; i < len(args); i += 1 {
		if args[i] == "--commit" && i + 1 < len(args) {
			commit = args[i + 1]
			i += 1
		}
	}
	if !render_all(DEFAULT_DB_PATH, commit, context.allocator) {
		os.exit(1)
	}
}
