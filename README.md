# kubehz-agent

Informer-based **live-view agent** for the [kubehz](https://kubehz.cloud) managed
tier. It runs as a long-running Deployment **in the user's own cluster**, watches
nodes / pods / warning-events through typed [client-go](https://github.com/kubernetes/client-go)
informers, and pushes a debounced snapshot to the kubehz platform API — **outbound
only**, authenticated with the same in-cluster agent-token the bash heartbeat uses.

> Status: **P2 scaffold** (managed-platform-spec §2). A working skeleton — live
> view is wired end-to-end (informers → debounce → authenticated POST); the
> acting capabilities (scaling, self-heal, upgrades) are later phases.

It complements, and does not replace, the lightweight bash **heartbeat CronJob**
(registered tier). The Go rewrite exists because node facts must be **correct**:
the bash agent repeatedly mis-parsed node JSON with `sed` (every node reported
`control-plane`; `kubectl version` returned the *client* tag; a greedy scrape
invented a `runc` node). This agent reads **typed** objects, so those bugs cannot
recur — and the mapping is unit-tested as a regression guard.

## Design principles (hard invariants)

- **Outbound-only.** No inbound listener, no port, no HTTP server anywhere. The
  agent dials the apiserver (watch) and kubehz-api (heartbeat POST); nothing dials
  in. (managed-platform-spec §1.3.1)
- **No telemetry, ever.** The only network egress is the authenticated heartbeat.
  There is no analytics/phone-home code path — the privacy guarantee is enforced
  *by construction* and auditable in this source. (§1.2.5)
- **Reuses the P0 identity.** The agent invents no auth. It reads agent-token `A`
  (`khz_agt_<hex64>`) from the in-cluster Secret `kubehz-agent` and sends
  `Authorization: Bearer A` to the existing heartbeat endpoint — the server's
  authenticated-heartbeat ratchet applies unchanged. (§1.7)
- **Least privilege.** Read-only on nodes/pods/events; a single name-scoped `get`
  on its own token Secret. No writes, no logs/exec, no other secret reads.
- **Fail toward report-only.** A partial read degrades that section to empty
  rather than aborting; a rejected push is retried with backoff; the agent never
  blocks the cluster. (§1.3.4)

## Architecture

```
                in-cluster (user's cluster)                     kubehz platform
 ┌──────────────────────────────────────────────────┐        ┌──────────────────┐
 │  SharedInformerFactory (nodes, pods, events)      │        │                  │
 │        │ add/update/delete                        │        │                  │
 │        ▼                                           │        │                  │
 │  changeHandler ──signal──▶ Coalescer              │        │                  │
 │                            (debounce 10s,          │        │                  │
 │                             min-gap 15s,           │        │                  │
 │                             periodic 60s)          │        │                  │
 │                                │ flush()           │        │                  │
 │                                ▼                    │        │                  │
 │  collector.BuildPayload(listers) ─▶ schema-2      │        │                  │
 │                                │ payload           │        │                  │
 │                                ▼                    │ Bearer A                  │
 │  Sender (latest-slot + backoff) ───── POST ───────┼───────▶│  /api/clusters/  │
 │                                                    │  (443) │   {id}/heartbeat │
 └──────────────────────────────────────────────────┘        └──────────────────┘
```

### Package layout

| Package | Responsibility |
|---|---|
| `cmd/kubehz-agent` | entrypoint: load config → build client → resolve token → run agent |
| `internal/config` | env + mounted-token loading, https/format validation, redaction |
| `internal/kube` | in-cluster clientset, discovery server-version, Secret-read fallback |
| `internal/collector` | **typed** node/pod/event → payload mapping (the correctness core) |
| `internal/state` | schema-2 payload types + `ApplyCaps` (server-bound enforcement) |
| `internal/publisher` | `Publisher` (POST+Bearer), `Backoff`, `Coalescer` (debounce), `Sender` (retry) |
| `internal/agent` | wiring: informers → coalescer → sender |
| `internal/buildinfo` | build-stamped version |
| `deploy/` | namespace, SA, RBAC, ConfigMap, Deployment, kustomization |

The debounce policy is a **pure function** (`publisher.decide`) — no timers in the
tested code path — so the whole cadence (debounce / min-gap / periodic) is
exhaustively unit-tested.

## The payload (schema 2) and the endpoint

The agent POSTs to the **existing** heartbeat endpoint
`POST /api/clusters/{id}/heartbeat` with `Authorization: Bearer A`, carrying an
**additive** `schema: 2` body. Because the live `HeartbeatSchema` is a *non-strict*
zod object, a schema-2 payload is **accepted today** — unknown fields are stripped,
the schema-1 base fields persist — so it degrades gracefully until the API is
extended.

```jsonc
{
  "schema": 2,
  "clusterId": "kubehz.in.net",
  "timestamp": "2026-07-05T12:00:00Z",
  "agent":      { "version": "0.1.0", "mode": "operator" },
  "kubernetes": { "version": "v1.35.5" },          // SERVER gitVersion (discovery)
  "nodes": [
    {
      "name": "cp-1", "status": "Ready", "ready": true,
      "roles": "control-plane",                     // typed label keys, done right
      "instanceType": "cx32",
      "kubeletVersion": "v1.35.5",                  // per-node SERVER version
      "capacity": { "cpu": "4", "memory": "8148Mi" }
    }
  ],
  "workloads": {
    "pods": { "total": 42, "running": 40, "pending": 1, "failed": 1,
              "succeeded": 0, "unknown": 0 }        // counts only — no names
  },
  "events": [                                        // recent Warnings (capped 20)
    { "reason": "BackOff", "kind": "Pod", "count": 3, "lastSeen": "…" }
  ]
  // forward-compat, populated in later phases: "pools":[…], "desired":{…}
}
```

Every field is capped to the server's `HeartbeatSchema` bounds by `state.ApplyCaps`
(nodes ≤ 500, name ≤ 253, status ≤ 63, …) so a well-behaved agent is never 400'd.

**Privacy gating.** `byNamespace` pod counts and event `namespace`/`note` are
included **only** when `KUBEHZ_REPORT_NAMESPACES=true`. The default is phase-only
counts and reason/kind-only events — workload *visibility* without workload
*contents* (§1.2.5, §2).

### Required kubehz-api extension (next, not in this repo)

To *store and surface* the rich fields, kubehz-api needs (see the report):
1. Extend `HeartbeatSchema` (`server/utils/validation.ts`) with the capped
   schema-2 fields.
2. In `processHeartbeat`, persist them to the snapshot; write the observed
   `kubernetes.version` to a real observed column; **throttle** `heartbeat_events`
   inserts to ≤ 1 / 60 s so debounced live pushes don't bloat the uptime buckets.
3. Tighten `connected` from `agent.mode` (operator ⇒ ~3 min, not ~15).

An **alternative** endpoint (`POST /api/clusters/{id}/state`, latest-only, no
event row) is discussed in the handoff report — pick one before the API work.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `CLUSTER_ID` | — (required) | cluster domain, e.g. `kubehz.in.net` |
| `KUBEHZ_API_URL` | — (required) | api base; **https** (http only for loopback) |
| `KUBEHZ_AGENT_TOKEN_FILE` | `/var/run/secrets/kubehz/agent-token` | mounted bearer A |
| `KUBEHZ_AGENT_TOKEN` | — | token via env (discouraged; tests/local only) |
| `KUBEHZ_FULL_INTERVAL` | `60s` | periodic full-snapshot push |
| `KUBEHZ_DEBOUNCE` | `10s` | coalesce a change burst |
| `KUBEHZ_MIN_GAP` | `15s` | floor between two pushes |
| `KUBEHZ_REPORT_NAMESPACES` | `false` | opt into namespace/message reporting |
| `KUBEHZ_LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

Token resolution order: env → mounted file → k8s-API Secret read (fallback). The
Deployment mounts the Secret as a **read-only file** (default), so no Secret-`get`
RBAC is exercised.

## Build, test, run

```bash
go build ./...
go test -race ./...
go vet ./... && gofmt -l .

# container (pinned distroless/static, non-root, read-only rootfs)
docker build -t kubehz-agent:dev --build-arg VERSION=dev .

# deploy (after the kubehz-agent Secret is bootstrapped by the lok8s plumbing)
kubectl apply -k deploy/
```

Toolchain: **Go 1.26** · **client-go v0.35.6** (matches the pilot's Kubernetes
v1.35.x). Versions are verified against their registries before pinning — never
guessed (see `AGENTS.md`).

## Security & supply chain

- **Image:** distroless `static-debian12:nonroot`, pinned by digest; static
  `CGO_ENABLED=0` binary; runs non-root (65532), read-only rootfs, all caps
  dropped, seccomp `RuntimeDefault`.
- **CI:** `gofmt`/`vet`/`golangci-lint`/`go test -race`/`govulncheck`; multi-arch
  build pushed to public **GHCR** (customer-pullable) + internal **Harbor** mirror,
  **cosign**-signed (keyless OIDC) with an attested **SBOM** and SLSA provenance.
- **The token `A` is never logged or printed.** `Config.String()` redacts it; the
  claim-code `C` is never mounted or read by this agent.

## Follow-ups (later phases)

- **Dashboard** live surfaces (nodes capacity/instance-type, workloads card,
  "reported n s ago"). Harden `readyNodeCount` to use the explicit `ready` bool
  (the current regex substring-matches `NotReady` as ready) and key off `schema`.
- **HA:** leader-elected Lease (`kubehz-system`) for multi-replica.
- **P3+ informers:** MachineDeployment pools (`pools[]`), apps deployments summary,
  the `/desired` pull loop + `desired` ack, CSR cert-expiry — each with its own
  RBAC delta (spec §2–§6).
- **Field-level deltas:** the current push is a change-triggered *full* snapshot
  (latest-wins, matching the server JSONB). Delta encoding is a future
  optimization if payload size ever matters.
- **Server-side Warning filter:** the events informer currently watches all
  events and filters `Warning` in code. A `type=Warning` field selector on a
  dedicated filtered factory would shrink the cache and the change-signal noise
  on busy clusters.

## License

[MIT](LICENSE).
