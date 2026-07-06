# kubehz-agent

Informer-based **live-view + desired-state agent** for the
[kubehz](https://kubehz.cloud) managed tier. It runs as a long-running
Deployment **in the user's own cluster**, watches nodes / pods /
warning-events through typed [client-go](https://github.com/kubernetes/client-go)
informers, pushes a debounced snapshot to the kubehz platform API, and pulls
the platform's desired-state document to execute **server-authorized worker
scaling locally** — all **outbound only**, authenticated with the same
in-cluster agent-token the bash heartbeat uses.

> Status: **P2 live view + P3 worker scaling + P5 self-healing + P6 worker
> upgrades**, plus `machineIssues[]` failure surfacing and the lok8s
> `ClusterInventory` loop (inventory on the beat, addon `availableUpdates`
> written back to the CR status — visible via plain kubectl). Every acting
> capability is **server-gated** (the `/desired` `execution{}` flags computed
> from tier × access × platform kill switch) with hard, unit-tested
> guardrails; anything authorized-but-unbuilt is reported as an unsupported
> action, never improvised. Control-plane upgrades stay user-driven.

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
- **Least privilege.** Read-only on nodes/pods/events and the lok8s
  `ClusterInventory`; a single name-scoped `get` on its own token Secret. No
  logs/exec, no other secret reads. Every **acting** write —
  MachineDeployment patch (replicas / kubelet version) and Machine **delete**
  (self-healing, the sharpest permission the agent holds — loudly documented
  in `deploy/managed/rbac-managed.yaml`, removable independently) — is an
  opt-in RBAC overlay (`deploy/managed/`), absent from the base entirely.
  The base's one write, `patch` on `clusterinventories/status`, is a
  visibility mirror on the cluster's own reporting object (see the inventory
  section), not an acting permission.
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
| `internal/state` | schema-2 payload types incl. `actions[]`/`machineIssues[]`/`inventory` + `ApplyCaps` (server-bound enforcement) |
| `internal/publisher` | `Publisher` (POST+Bearer, response `availableUpdates` parse), `Backoff`, `Coalescer` (debounce), `Sender` (retry) |
| `internal/inventory` | ClusterInventory (lok8s.dev) manager: periodic spec read → `inventory` block; idempotent `availableUpdates` status write-back |
| `internal/desired` | the pull loop: contract types (incl. the P5 `healing` policy), ETag-aware client, Poller |
| `internal/machines` | grounded machine-controller API surface: GVRs, exact field paths, Machine→pool resolution |
| `internal/machineissues` | ungated, fail-soft `machineIssues[]` collector (terminal errors, retry-loop events, join timeouts) |
| `internal/executor` | the acting side: P3 replica patches, P5 heal deletes, P6 kubelet rolls + every safety rail |
| `internal/actions` | in-memory action-report store (latest-revision-wins, diff-aware notify, stale-heal prune) |
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
  "actions": [                                       // desired-state progress (capped 20)
    { "type": "scale", "target": "pool-a", "status": "done",
      "detail": "replicas 2 to 3; machine-controller reconciles the machines with the cluster's own credentials",
      "revision": 7 },
    { "type": "upgrade", "target": "pool-a", "status": "in-progress",
      "detail": "v1.34.9 → v1.35.6 (1/3)", "revision": 7 },     // P6: target = POOL name
    { "type": "heal", "target": "pool-a-5b6c7-xk2lp", "status": "done",
      "detail": "deleted Machine (node w-3 NotReady for 6m0s (unhealthyAfter 5m0s)); machine-controller/MachineSet recreates it with the cluster's own credentials",
      "revision": 7 }                                            // P5: target = MACHINE name
  ],
  "machineIssues": [                                 // machine-controller failures (capped 20)
    { "pool": "pool-b", "machine": "pool-b-abc12-x9k2p",
      "reason": "ReconcilingError",
      "message": "failed to create server: unsupported location for server type",
      "since": "2026-07-06T11:48:00Z" }
  ],
  "inventory": {                                     // ClusterInventory.spec, verbatim
    "lok8sVersion": "1.4.2", "kind": "kubeone", "provider": "hetzner",
    "kubernetesVersion": "v1.35.5",
    "specHash": "sha256-hex of cluster.lok8s.yaml",
    "renderedAt": "2026-07-06T10:00:00Z",
    "addons": [
      { "name": "cilium", "chartVersion": "1.16.1", "appVersion": "1.16.1",
        "category": "networking", "source": "addon" }
    ]
  }
  // forward-compat, populated in later phases: "pools":[…], "desired":{…}
}
```

**Action targets are opaque identifiers.** For `heal` the target is the
**Machine** name (stable pre-join — a machine whose node never appeared has
no node name at all); the node name, when known, rides in `detail`. For
per-pool `upgrade` progress the target is the **pool** name. Dashboards
should render targets verbatim, never parse them.

**`machineIssues[]`** is pure observation, deliberately **ungated** by the
execution flags: a pool that never converges is otherwise invisible (its
machines never become nodes). Three grounded sources, read off the Machine
objects machine-controller maintains: **terminal** `status.errorReason`/
`errorMessage` ("manual intervention required": InvalidConfiguration,
CreateError, …), **retry-loop** Warning events on Machines (reason
`ReconcilingError` — where "webhook accepted it, hcloud rejects every
create: *unsupported location for server type*" lives; it is never a status
field), and an agent-synthesized `NodeJoinTimeout` for machines with no node
10 minutes after creation. Fail-soft: without the managed overlay's machines
read RBAC (or without the CRD) the block is simply absent. `since` is the
agent-observed first-seen time, stable across passes.

**`inventory`** mirrors the **spec** of the lok8s `ClusterInventory` CR
(`clusterinventories.lok8s.dev/v1alpha1`, the cluster-scoped singleton named
`cluster` that the user's own `lo` CLI server-side-applies at the end of
`provision`/bootstrap), field names verbatim. The agent GETs it periodically
at the full-beat cadence (it only changes on lo deploys — an informer on a
possibly-absent CRD would error-loop on every never-lok8s-deployed cluster)
and fails soft: no CRD/CR/RBAC → no `inventory` key at all, zero noise.
**Data inventory / privacy class:** the block is metadata the *user's own
tooling already wrote into their own cluster* — lok8s/driver/k8s versions, a
spec hash, and addon names + pinned chart versions/categories. The CRD schema
itself prunes anything beyond those fields (no chart values, env overrides,
credentials, or rendered manifests can exist in the CR), so it is the same
privacy class as the version fields and is **not** gated by
`KUBEHZ_REPORT_NAMESPACES`.

**The write-back — addon updates via plain kubectl.** The heartbeat
*response* carries `availableUpdates: [{name, current, latest}]`, computed by
the platform from the reported inventory against the published lok8s addons
index. The key is **tristate** (kubehz-api f628c97): **absent** = no verdict
(the beat carried no inventory, or the index was unreachable — the server
omits the key precisely so an outage never wipes the agent's last known
status); **present `[]`** = index consulted, nothing is newer — the agent
**clears** `status.availableUpdates` (without this, a user who upgrades
their addons would keep stale "update available" notices forever);
**non-empty** = written as-is. Every verdict lands — with `lastReported`
(RFC3339) — on the CR via the **status subresource** (JSON merge patch,
field manager `kubehz-agent`), so:

```bash
kubectl get clusterinventory cluster -o yaml   # status.availableUpdates — no dashboard needed
```

Boundaries: the spec is never written (lo-owned); the sibling
`status.observedAddons` is never touched (merge semantics); the write is
idempotent (compared against the CR's current status — refreshed every poll
and after every write, so restarts and repeat responses, including an
already-clear `[]`, cost zero patches) — but idempotence is keyed on a status
**existing**: on a fresh CR with no recorded status the first verdict always
writes, even `[]`, so `lastReported` gets stamped and never-checked becomes
checked-and-clear; a clear is patched as an explicit `[]`, never `null`
(merge-patch `null` would delete the field; `[]` keeps "checked, nothing
newer" distinguishable from never-checked); an RBAC-denied patch warns
**once** and beating continues; server input is clamped to the CRD's own
status bounds before patching, and a non-empty verdict whose entries are all
invalid is refused rather than treated as a clear.

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
`agent.mode`), the `actions[]`/`machineIssues[]` persistence,
`GET /clusters/{id}/desired` incl. the P5 `healing` block + `heal` action
enum (kubehz-api d57c206), and the **inventory loop** (kubehz-api f628c97):
`inventory` ingestion (validated + capped — note the addons **hard cap of
100**, `HB_MAX_INVENTORY_ADDONS`, which is why `state.MaxAddons` is 100 and
not the CRD's 256: one entry more would 400 the whole beat) plus the
tristate `availableUpdates` response described above, surfaced on
`GET /clusters/{id}` as `inventory` + `addonUpdates`. The
`pools[]`/`desired{}` blocks remain accepted-but-stripped; the agent leaves
them empty until the API ingests them. Should the response shape ever move,
`internal/publisher`'s `heartbeatResponse` is the single place to adjust.

## Desired state & acting — the honest model

The agent executes exactly three capabilities: **worker-pool replica scaling**
(P3), **worker self-healing** (P5), and **worker kubelet upgrades** (P6).
How they work, truthfully:

1. **The platform never pushes.** The agent polls
   `GET /api/clusters/{id}/desired` (bearer `A`, strong ETag, `If-None-Match`
   → cheap `304`) every `KUBEHZ_DESIRED_POLL_SECONDS` (default 60, + up to
   10% jitter). The document carries
   `workerPools[{name, machineType, desiredReplicas}]`, a **server-computed**
   `execution{scaling, upgrades, healing}` block (tenant tier × cluster
   access × platform kill switch), and the **server-owned**
   `healing{enabled, maxUnhealthy, nodeStartupTimeoutSeconds,
   unhealthyAfterSeconds, cooldownSeconds}` policy. The ETag is treated as an
   **opaque token** (cached, echoed verbatim, never parsed) — a server-side
   format change costs one extra `200`, nothing else.
2. **Acting is server-gated with no local enable.** There is *no* agent-side
   configuration that can turn any execution on; when the server says
   `false` (or any 4xx), the agent acts on nothing and clears its reports. A
   tier downgrade or kill-switch flip reaches the agent on its next poll
   (the flags are part of the ETag). 401/403 honor the full retry backoff,
   exactly like the heartbeat sender.
3. **Scaling (P3): one field of one resource.** For each desired pool it
   merge-patches `spec.replicas` on the MachineDeployment **of the same
   name** in `KUBEHZ_MD_NAMESPACE` (default `kube-system`) — GVR
   `machinedeployments.cluster.k8s.io/v1alpha1`, KubeOne's machine-controller
   group (**not** `cluster.x-k8s.io`). The pool↔MD **name equality is the
   mapping contract**; an unmatched pool is skipped and reported `failed`.
4. **Self-healing (P5): delete the Machine, let the cluster rebuild it.** A
   worker node continuously NotReady/Unknown for `unhealthyAfterSeconds`, or
   a machine whose node never appeared within `nodeStartupTimeoutSeconds`,
   gets its backing **Machine deleted** — machine-controller/MachineSet
   recreates it. Guardrails (hard, refusal-biased, all unit-tested):
   **never control-plane** (node role labels, machine labels, and the same
   CP-MD heuristic scaling uses); a **storm brake** — more unhealthy than
   `maxUnhealthy` deletes *nothing* and reports every candidate `failed`
   ("unhealthy count N exceeds maxUnhealthy M — refusing (possible outage)");
   at most `maxUnhealthy` disruptions in flight (any already-deleting machine
   counts); per-pool `cooldownSeconds` with the agent's start time as
   baseline (**restart = fresh cooldown** — a crash-looping agent can never
   become a machine-delete loop); autoscaler-owned pools and unowned machines
   are refused. Healing re-evaluates every poll tick; detection windows are
   floored at 60 s agent-side.
   **Eviction unwedge (the bounded follow-through):** when the healed node is
   truly dead (kubelet stopped), machine-controller's eviction of the pods
   stuck Terminating there can never confirm, so the deleted Machine sits in
   teardown indefinitely — a zombie server billing the user (observed live:
   ~12 min until manual intervention). If a machine **this agent itself
   heal-deleted** is still deleting after
   `KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS` (default 300) *and* its node is
   still unreachable (Ready Unknown/False), the agent force-deletes exactly
   the pods **already Terminating on that node** (grace 0, UID-preconditioned
   so a same-name replacement is never hit), **once per machine**, and
   reports it on the heal action ("eviction unwedged: force-deleted N
   pod(s)…"). A manually-deleted machine is never touched (in-memory
   tracking — a restart conservatively forgets), a recovered node is left
   alone, and without the pods-delete ClusterRole the attempt warns loudly
   and fails soft while healing keeps working.
5. **Upgrades (P6): roll workers toward the declared version.** When the
   desired `kubernetesVersion` differs from a pool MD's declared kubelet, the
   executor patches `spec.template.spec.versions.kubelet` (the exact
   machine-controller field path) and machine-controller performs the rolling
   machine replacement. **Pre-flight:** the *observed* control-plane version
   must be at/above the target minor — else `failed` "control plane not yet
   at target" (upgrade the CP first via `lo provision` / `kubeone apply`;
   an unknown observed version refuses, never guesses). **One pool rolls at
   a time**; progress reports per pool as `"vFROM → vTO (n/m)"` (n = machines
   at target with a joined node); autoscaler pools report unsupported;
   halt-on-failure. Workers only — the agent never upgrades a control plane.
6. **The cluster's own machinery does the provisioning.** Every write wakes
   the in-cluster machine-controller, which creates/deletes Hetzner servers
   with the credentials **it already holds**. The agent contains no hcloud
   token, no SSH key, no provisioner — revoking the platform's access is
   `kubectl delete ns kubehz-system` and nothing else changes.
7. **Progress is reported, in memory only.** Every outcome
   (`pending → in-progress → done/failed`, with the acted `revision`) rides
   the heartbeat's `actions[]`; stale heal reports for self-recovered nodes
   are pruned. Restart = re-poll + reconverge; because the executors are
   idempotent, that costs zero cluster writes.

The acting RBAC — `patch` on machinedeployments (replicas + kubelet version)
and read + **delete** on machines (kube-system-scoped Roles), plus a
pods-**delete**-only ClusterRole for the eviction unwedge (the stuck pods
span arbitrary namespaces, which no namespaced Role can express) — is an
**opt-in overlay**: `kubectl apply -k deploy/managed/`, absent from the
registered-tier base. The machines **delete** verb exists solely for P5 and
is loudly documented in `deploy/managed/rbac-managed.yaml`; drop that one
verb and healing fails closed (reported `Forbidden`) while everything else
keeps working. Drop the unwedge ClusterRole and only the unwedge is
disabled — healing itself is unaffected. See
[deploy/README.md](deploy/README.md).

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
| `KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS` | `300` | how long a heal-deleted machine may sit deleting (node still dead) before the one-shot eviction unwedge; min 60 |
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
kubectl apply -k deploy/managed/  # managed tier: + acting RBAC (scale/upgrade
                                  #   patch, machineIssues read, heal delete)
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
- **Field-level deltas:** the current push is a change-triggered *full* snapshot
  (latest-wins, matching the server JSONB). Delta encoding is a future
  optimization if payload size ever matters.

## License

[MIT](LICENSE).
