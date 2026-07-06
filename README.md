# kubehz-agent

Informer-based **live-view + desired-state agent** for the
[kubehz](https://kubehz.cloud) managed tier. It runs as a long-running
Deployment **in the user's own cluster**, watches nodes / pods /
warning-events through typed [client-go](https://github.com/kubernetes/client-go)
informers, pushes a debounced snapshot to the kubehz platform API, and pulls
the platform's desired-state document to execute **server-authorized worker
scaling locally** — all **outbound only**, authenticated with the same
in-cluster agent-token the bash heartbeat uses.

> Status: **P2 live view + P3 worker scaling**. The live view is wired
> end-to-end (informers → debounce → authenticated POST); the desired-state
> loop executes **replica scaling only** (server-gated). Upgrades and
> self-heal are later phases — an authorized-but-unbuilt upgrade is reported
> as an unsupported action, never improvised.

It complements, and does not replace, the lightweight bash **heartbeat CronJob**
(registered tier). The Go rewrite exists because node facts must be **correct**:
the bash agent repeatedly mis-parsed node JSON with `sed` (every node reported
`control-plane`; `kubectl version` returned the *client* tag; a greedy scrape
invented a `runc` node). This agent reads **typed** objects, so those bugs cannot
recur — and the mapping is unit-tested as a regression guard.

## Design principles (hard invariants)

- **Outbound-only.** No inbound listener, no port, no HTTP server anywhere. The
  agent dials the apiserver (watch), kubehz-api's heartbeat POST, and
  kubehz-api's desired-state GET — every connection is **opened by the agent**;
  nothing dials in and the platform can never push a command.
  (managed-platform-spec §1.3.1)
- **No telemetry, ever.** The only network egress is the authenticated
  heartbeat + desired-state poll. There is no analytics/phone-home code path —
  the privacy guarantee is enforced *by construction* and auditable in this
  source. (§1.2.5)
- **Reuses the P0 identity.** The agent invents no auth. It reads agent-token `A`
  (`khz_agt_<hex64>`) from the in-cluster Secret `kubehz-agent` and sends
  `Authorization: Bearer A` to the existing heartbeat endpoint — the server's
  authenticated-heartbeat ratchet applies unchanged. (§1.7)
- **Least privilege.** Read-only on nodes/pods/events; a single name-scoped `get`
  on its own token Secret. No logs/exec, no other secret reads. The ONE write —
  MachineDeployment replica patch — is an opt-in RBAC overlay
  (`deploy/managed/`), absent from the base entirely.
- **Fail toward report-only.** A partial read degrades that section to empty
  rather than aborting; a rejected push is retried with backoff; acting is
  server-gated and every guard refuses rather than improvises; the agent never
  blocks the cluster. (§1.3.4)

## Architecture

```
                in-cluster (user's cluster)                     kubehz platform
 ┌──────────────────────────────────────────────────┐        ┌──────────────────┐
 │  SharedInformers (nodes, pods, events⚠only)       │        │                  │
 │        │ add/update/delete                        │        │                  │
 │        ▼                                           │        │                  │
 │  changeHandler ──signal──▶ Coalescer              │        │                  │
 │        ▲                   (debounce 10s,          │        │                  │
 │        │ notify             min-gap 15s,           │        │                  │
 │  actions.Store              periodic 60s)          │        │                  │
 │        ▲                       │ flush()           │        │                  │
 │        │ report                ▼                    │        │                  │
 │        │        collector.BuildPayload + actions[] │        │                  │
 │        │                       │ schema-2 payload  │ Bearer A                  │
 │        │                       ▼                    │        │                  │
 │        │  Sender (latest-slot + backoff) ── POST ──┼───────▶│  /api/clusters/  │
 │        │                                           │  (443) │   {id}/heartbeat │
 │  Executor ◀── doc ── Poller ◀──── GET (ETag/304) ──┼───────▶│  /api/clusters/  │
 │     │ merge-patch spec.replicas                    │ Bearer A│   {id}/desired   │
 │     ▼                                              │        └──────────────────┘
 │  MachineDeployment (cluster.k8s.io/v1alpha1)       │
 │     └─▶ machine-controller provisions with the    │
 │         cluster's OWN hcloud credentials           │
 └──────────────────────────────────────────────────┘
```

### Package layout

| Package | Responsibility |
|---|---|
| `cmd/kubehz-agent` | entrypoint: load config → build clients → resolve token → run agent |
| `internal/config` | env + mounted-token loading, https/format validation, redaction |
| `internal/kube` | in-cluster clientset + dynamic client (one GVR), discovery server-version, Secret-read fallback |
| `internal/collector` | **typed** node/pod/event → payload mapping (the correctness core) |
| `internal/state` | schema-2 payload types incl. `actions[]` + `ApplyCaps` (server-bound enforcement) |
| `internal/publisher` | `Publisher` (POST+Bearer), `Backoff`, `Coalescer` (debounce), `Sender` (retry) |
| `internal/desired` | the P3 pull loop: contract types, ETag-aware client, Poller |
| `internal/executor` | the P3 acting side: MachineDeployment replica patches + every safety rail |
| `internal/actions` | in-memory action-report store (latest-revision-wins, diff-aware notify) |
| `internal/agent` | wiring: informers → coalescer → sender; poller → executor → actions → beats |
| `internal/buildinfo` | build-stamped version |
| `deploy/` | base (read-only) + `managed/` overlay (acting RBAC) — resources are named `kubehz-live-agent*` to coexist with the bash CronJob agent's `kubehz-agent*` RBAC (see `deploy/README.md`) |

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
  ],
  "actions": [                                       // P3 desired-state progress (capped 20)
    { "type": "scale", "target": "pool-a", "status": "done",
      "detail": "replicas 2 to 3; machine-controller reconciles the machines with the cluster's own credentials",
      "revision": 7 }
  ]
  // forward-compat, populated in later phases: "pools":[…], "desired":{…}
}
```

The server persists `actions` **latest-wins** and treats an absent `actions`
key as *clear* — so the agent keeps reporting the current revision's actions
on every beat while they are relevant, and stops (clearing them) when acting
is switched off or a new revision produces nothing to do.

Every field is capped to the server's `HeartbeatSchema` bounds by `state.ApplyCaps`
(nodes ≤ 500, name ≤ 253, status ≤ 63, …) so a well-behaved agent is never 400'd.

**Privacy gating.** `byNamespace` pod counts and event `namespace`/`note` are
included **only** when `KUBEHZ_REPORT_NAMESPACES=true`. The default is phase-only
counts and reason/kind-only events — workload *visibility* without workload
*contents* (§1.2.5, §2).

The kubehz-api side of this contract is **shipped**: schema-2 ingestion
(nodes/workloads/events/observed version, `connected` tightening from
`agent.mode`) and the `actions[]` persistence + `GET /clusters/{id}/desired`
landed with the P3 desired-state foundation. The `pools[]`/`desired{}` blocks
remain accepted-but-stripped; the agent leaves them empty until the API
ingests them.

## Desired state & acting (P3) — the honest model

The agent executes exactly one capability: **worker-pool replica scaling**.
How it works, truthfully:

1. **The platform never pushes.** The agent polls
   `GET /api/clusters/{id}/desired` (bearer `A`, strong ETag
   `"<revision>-<flags>"`, `If-None-Match` → cheap `304`) every
   `KUBEHZ_DESIRED_POLL_SECONDS` (default 60, + up to 10% jitter). The
   document carries `workerPools[{name, machineType, desiredReplicas}]` and a
   **server-computed** `execution{scaling, upgrades}` block (tenant tier ×
   cluster access × platform kill switch).
2. **Acting is server-gated with no local enable.** There is *no* agent-side
   configuration that can turn execution on; when the server says
   `scaling: false` (or any 4xx), the agent acts on nothing and clears its
   reports. A tier downgrade reaches the agent on its next poll (the flags
   are part of the ETag). 401/403 honor the full retry backoff, exactly like
   the heartbeat sender.
3. **The executor edits one field of one resource.** For each desired pool it
   merge-patches `spec.replicas` on the MachineDeployment **of the same
   name** in `KUBEHZ_MD_NAMESPACE` (default `kube-system`) — GVR
   `machinedeployments.cluster.k8s.io/v1alpha1`, KubeOne's machine-controller
   group (**not** `cluster.x-k8s.io`). The pool↔MD **name equality is the
   mapping contract** (the lok8s/KubeOne provisioning path creates both from
   the same spec); an unmatched pool is skipped and reported `failed`.
4. **The cluster's own machinery does the provisioning.** The patch wakes the
   in-cluster machine-controller, which creates/deletes Hetzner servers with
   the credentials **it already holds**. The agent contains no hcloud token,
   no SSH key, no provisioner — revoking the platform's access is
   `kubectl delete ns kubehz-system` and nothing else changes.
5. **Guard-rails refuse, never improvise** (all unit-tested):
   `desiredReplicas` outside `0..KUBEHZ_MAX_REPLICAS` (default 50) is refused
   — never clamped-and-applied; control-plane-looking MDs (node-role labels or
   `control-plane`/`master`/segment-`cp` names) are refused;
   cluster-autoscaler-owned MDs (node-group annotations, either API-group
   spelling) are refused; pools execute sequentially with
   halt-on-first-transient-failure; already-at-desired is a no-op `done`;
   **no creates, no deletes, no machineType/version edits** — a pending
   upgrade with `execution.upgrades: true` is reported as an unsupported
   `failed` action.
6. **Progress is reported, in memory only.** Every outcome
   (`pending → in-progress → done/failed`, with the acted `revision`) rides
   the heartbeat's `actions[]`. Restart = re-poll + reconverge; because the
   executor is idempotent, that costs zero cluster writes.

The acting RBAC (`patch` on machinedeployments, kube-system-scoped Role) is
an **opt-in overlay** — `kubectl apply -k deploy/managed/` — absent from the
registered-tier base. See [deploy/README.md](deploy/README.md).

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
| `KUBEHZ_DESIRED_POLL_SECONDS` | `60` | desired-state pull cadence (integer seconds; + ≤10% jitter) |
| `KUBEHZ_MD_NAMESPACE` | `kube-system` | where the executor looks for MachineDeployments |
| `KUBEHZ_MAX_REPLICAS` | `50` | per-pool ceiling; out-of-bounds desired is **refused**, not clamped |
| `KUBEHZ_LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

None of these enable acting — execution is authorized exclusively by the
server's `/desired` `execution{}` flags plus the opt-in `deploy/managed/`
RBAC. The envs above are guard-rails only.

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

# deploy (after the kubehz-agent Secret is bootstrapped by the lok8s plumbing);
# coexists with the bash CronJob agent — see deploy/README.md for the naming
# contract and the image digest-pinning rules.
kubectl apply -k deploy/          # registered tier: observe + report only
kubectl apply -k deploy/managed/  # managed tier: + worker-scaling RBAC
```

Toolchain: **Go 1.26** · **client-go v0.35.6** (matches the pilot's Kubernetes
v1.35.x). Versions are verified against their registries before pinning — never
guessed (see `AGENTS.md`).

## Security & supply chain

- **Image:** distroless `static-debian12:nonroot`, pinned by digest; static
  `CGO_ENABLED=0` binary; runs non-root (65532), read-only rootfs, all caps
  dropped, seccomp `RuntimeDefault`.
- **CI:** `gofmt`/`vet`/`golangci-lint`/`go test -race`/`govulncheck`; multi-arch
  build pushed to **GHCR** (the canonical customer image — the package must be
  set public for anonymous pulls) + internal **Harbor** mirror, **cosign**-signed
  (keyless OIDC) with an attested **SBOM** and SLSA provenance.
- **The token `A` is never logged or printed.** `Config.String()` redacts it; the
  claim-code `C` is never mounted or read by this agent.

## Follow-ups (later phases)

- **Dashboard** live surfaces (nodes capacity/instance-type, workloads card,
  "reported n s ago"). Harden `readyNodeCount` to use the explicit `ready` bool
  (the current regex substring-matches `NotReady` as ready) and key off `schema`.
- **HA:** leader-elected Lease (`kubehz-system`) for multi-replica.
- **Observed pools:** a MachineDeployment informer feeding `pools[]` (observed
  replicas/ready per pool) once the API ingests it — closes the
  desired→observed convergence loop in the dashboard; apps deployments
  summary; CSR cert-expiry (spec §2).
- **P6 upgrades / P5 self-heal:** worker-version rolls (`versions.kubelet`)
  and machine remediation — each with its own executor guard-rails + RBAC
  delta (spec §3–§4). Until then upgrades report as unsupported.
- **Field-level deltas:** the current push is a change-triggered *full* snapshot
  (latest-wins, matching the server JSONB). Delta encoding is a future
  optimization if payload size ever matters.

## License

[MIT](LICENSE).
