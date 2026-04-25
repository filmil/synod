## 2025-02-21 - Security Headers
**Vulnerability:** Lack of security headers (`Content-Security-Policy`, `X-Content-Type-Options`, `X-Frame-Options`, `Strict-Transport-Security`)
**Learning:** `http.Serve` does not implement standard security headers natively.
**Prevention:** Always wrap `http.ServeMux` or route handlers with an HTTP middleware that adds essential security headers.
## 2025-04-25 - Enhance Security Headers
**Vulnerability:** Missing strict Referrer-Policy header.
**Learning:** While the app has basic security headers (CSP, X-Content-Type-Options, etc.), adding `Referrer-Policy: strict-origin-when-cross-origin` helps prevent leaking potentially sensitive URL information to external domains when cross-origin links are clicked.
**Prevention:** Include a comprehensive set of security headers for all web endpoints as a standard defense-in-depth practice.
