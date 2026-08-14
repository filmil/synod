import os
from datetime import datetime

file_path = ".jules/sentinel.md"
os.makedirs(os.path.dirname(file_path), exist_ok=True)

entry = f"""## {datetime.now().strftime('%Y-%m-%d')} - [Fix XSS in gRPC Status Handler]
Vulnerability: Cross-Site Scripting (XSS) via unescaped gRPC channel error messages rendered using template.HTML.
Learning: Manually constructing HTML strings in Go code and casting them to template.HTML is inherently risky and bypasses built-in contextual escaping.
Prevention: To prevent XSS, use structured data types (e.g., slices of structs) to pass data to HTML templates, allowing the html/template engine to handle contextual auto-escaping safely during rendering.

"""

if not os.path.exists(file_path):
    with open(file_path, "w") as f:
        f.write("# Security Learnings\n\n" + entry)
else:
    with open(file_path, "r") as f:
        content = f.read()
    with open(file_path, "w") as f:
        f.write(content.replace("# Security Learnings\n\n", "# Security Learnings\n\n" + entry))
