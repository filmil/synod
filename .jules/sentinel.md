## 2024-10-25 - XSS in HTML string concatenation
**Vulnerability:** XSS vulnerability found when generating HTML string and embedding to `template.HTML` in go source code. Variables in string interpolation are unescaped.
**Learning:** `template.HTML` disables auto escaping context aware of `html/template`, passing dangerous raw content like `info.HTTPURL` directly.
**Prevention:** Always use `html.EscapeString` and scheme checks before passing variables to `fmt.Sprintf` for `template.HTML` usage.
