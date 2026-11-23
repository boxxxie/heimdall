package bench_render

import "core:strings"
import "core:testing"

@(test)
test_formatting_and_svg_template :: proc(t: ^testing.T) {
	p := fmt_ns(0.718, context.temp_allocator)
	u := fmt_ns(128.32, context.temp_allocator)
	defer free_all(context.temp_allocator)
	testing.expect_value(t, p, "718.00 ps")
	testing.expect_value(t, u, "128.32 ns")

	x_labels := [2]string{"1 thread", "2 threads"}
	vals := [2]Maybe(f64){57.85, 94.61}
	v, ok := build_line_chart_view("Heimdall MT", "Sub", x_labels[:], {{"mt", vals[:], "#58a6ff"}}, false, "ns", context.temp_allocator)
	testing.expect(t, ok)
	sb: strings.Builder
	strings.builder_init(&sb, context.temp_allocator)
	_, err := line_chart_tmpl.with(strings.to_writer(&sb), v)
	testing.expect_value(t, err, nil)
	testing.expect(t, strings.contains(strings.to_string(sb), "Heimdall MT"))
}
