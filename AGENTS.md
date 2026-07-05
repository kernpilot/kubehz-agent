# kubehz-agent — guidance for AI agents & contributors

How to work on `kubehz-agent`. This binary runs **in a customer's cluster**, so
it must be readable and auditable — treat every change with that in mind.

## Security is paramount

Security applies to every change — features, fixes, refactors, tests.

- **Never guess versions.** Before adding/bumping any dependency (Go module,
  Docker base image, GitHub Action) confirm the latest stable version against its
  registry (proxy.golang.org, the registry's manifest API, the repo's releases).
  Hallucinated versions waste time and can pull malicious packages.
- **Never pipe remote content into a shell.** Download, read, verify, then run.
- **No secrets in code** — no tokens, keys, passwords. The agent-token `A` is
  never logged, printed, or embedded; `Config.String()` redacts it.
- **Outbound-only stays outbound-only.** Do not add an inbound listener, port, or
  HTTP server. The only egress is the authenticated heartbeat POST.
- **No telemetry.** There is deliberately no analytics/phone-home code path. Do
  not add one — the privacy guarantee is enforced by construction.
- **Plain HTTP is a red flag.** The API URL must be https (loopback excepted).
- **Least privilege.** Read-only RBAC on nodes/pods/events + a name-scoped get on
  the agent's own Secret. Any new capability must justify its RBAC delta.

## Correctness bar (why this agent is in Go)

Node facts come from **typed** client-go objects, never string-parsed JSON. The
bash heartbeat's bugs (every node `control-plane`; client-tag version; phantom
`runc` node) are regression-tested in `internal/collector/*_test.go`. Keep them
green and add a case for any new extraction.

## Build & test

```bash
go build ./...
go test -race ./...
go vet ./...
gofmt -l .            # must be empty
docker build -t kubehz-agent:dev --build-arg VERSION=dev .
```

Use conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `build:`, …). Keep
CI green — no new `go vet`/`golangci-lint`/`govulncheck` findings.

## Ground truth

The design lives in the kubehz-cluster repo:
`docs/internal/managed-platform-spec.md` (§2 live view, §1.7 agent identity, §1.3
invariants). The wire contract is kubehz-api `server/utils/{validation,heartbeat}.ts`.
When either moves, update `internal/state` + `internal/collector` and the tests
together.
