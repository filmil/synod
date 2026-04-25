## 2025-02-21 - Security Headers
**Vulnerability:** Lack of security headers (`Content-Security-Policy`, `X-Content-Type-Options`, `X-Frame-Options`, `Strict-Transport-Security`)
**Learning:** `http.Serve` does not implement standard security headers natively.
**Prevention:** Always wrap `http.ServeMux` or route handlers with an HTTP middleware that adds essential security headers.
