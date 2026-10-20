# Security

## Reporting a vulnerability

Please report security problems privately through GitHub: open the
repository's **Security** tab and choose **Report a vulnerability**. Do not
open a public issue.

Include what you found, how to reproduce it and which version or commit you
ran. You will get a reply within a week, and a fix or a plan within 30 days
of confirming the problem.

## Supported versions

Fixes go into the latest release only.

## Scope

In scope: the `webhookd` service, the Go SDK (`webhooks`) and `webhookctl`.
Things that are documented as the operator's responsibility are out of
scope, such as running with `ALLOW_PRIVATE_NETWORKS=true`, exposing
`/metrics` publicly, or serving the API without TLS. See "Running in
production" in [docs/operations.md](docs/operations.md).
