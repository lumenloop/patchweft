# Security and trust

Patchweft 0.1 is intended for trusted collaborators. A registered job executes
with the worker Unix account's privileges. API permissions do not make arbitrary
source code or profiles safe to run. Use a dedicated account and keep credentials
and unrelated sensitive files outside its reach.

Use HTTPS outside loopback and issue narrowly scoped, owner-private credential
files. The local administrative CLI directly accesses state; filesystem access
is an administrative capability. See [connections](docs/connections.md) and
[operations](docs/operations.md) for the full boundary.

Report a suspected vulnerability through
[GitHub's private vulnerability reporting](https://github.com/rawritude/patchweft/security/advisories/new).
Provide a minimal synthetic reproduction and affected version. Do not include
credentials or private project data. Avoid posting an exploitable issue publicly
before maintainers can assess it.

The current 0.1 development line receives fixes. There is no support SLA or
backport commitment for older revisions.
