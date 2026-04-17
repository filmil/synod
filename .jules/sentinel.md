## 2025-04-17 - Fix Reflected XSS in Prefix Read result
**Vulnerability:** A reflected XSS vulnerability existed where unescaped HTML built on the server was passed to the client via a query string parameter (`prefix_result`), and then subsequently injected unescaped into the browser's DOM on redirect.
**Learning:** This occurred because the code attempted to pass formatted HTML through a URL parameter as a short-hand for passing results between requests (from a form `POST` handler, to the `GET` view).
**Prevention:** Always serialize structured data (e.g. JSON) or simple IDs into URL parameters instead of pre-rendered HTML. Decode the data server-side and render safely using standard HTML templating libraries (like `html/template` in Go) which handle context-aware escaping.
