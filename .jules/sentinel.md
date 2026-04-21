## 2024-10-25 - XSS in HTML string concatenation
**Vulnerability:** XSS vulnerability found when generating HTML string and embedding to `template.HTML` in go source code. Variables in string interpolation are unescaped.
**Learning:** `template.HTML` disables auto escaping context aware of `html/template`, passing dangerous raw content like `info.HTTPURL` directly.
**Prevention:** Always use `html.EscapeString` and scheme checks before passing variables to `fmt.Sprintf` for `template.HTML` usage.
## 2025-04-17 - Fix Reflected XSS in Prefix Read result
**Vulnerability:** A reflected XSS vulnerability existed where unescaped HTML built on the server was passed to the client via a query string parameter (`prefix_result`), and then subsequently injected unescaped into the browser's DOM on redirect.
**Learning:** This occurred because the code attempted to pass formatted HTML through a URL parameter as a short-hand for passing results between requests (from a form `POST` handler, to the `GET` view).
**Prevention:** Always serialize structured data (e.g. JSON) or simple IDs into URL parameters instead of pre-rendered HTML. Decode the data server-side and render safely using standard HTML templating libraries (like `html/template` in Go) which handle context-aware escaping.

## 2025-04-20 - Fix Reflected XSS in gRPC Error and Participant Details
**Vulnerability:** Reflected XSS vulnerabilities existed where unescaped strings retrieved from remote nodes (like `err.Error()`, `info.HTTPURL`, `grpcAddr`, `info.ShortName`) were injected directly into HTML string layouts via `fmt.Sprintf` and then wrapped with `template.HTML`, bypassing HTML escaping protections.
**Learning:** This occurred because `html/template` only automatically escapes standard string insertions (`{{.}}`), and manual `template.HTML` casting explicitly disables escaping for anything passed to it. Directly formatted HTML strings using `fmt.Sprintf` and `template.HTML` must manually escape untrusted inputs.
**Prevention:** Always wrap untrusted external string data with `html.EscapeString()` *before* combining it into an HTML template string payload utilizing `fmt.Sprintf` intended for a `template.HTML` cast.
