# kubehz-agent

Informer-based **live-view + desired-state agent** for the
[kubehz](https://kubehz.io) managed tier. It runs as a long-running
Deployment **in the user's own cluster**. It watches nodes, pods and
warning events through typed [client-go](https://github.com/kubernetes/client-go)
informers and pushes a debounced snapshot to the kubehz platform API. It
also pulls the platform's desired-state document and executes
**server-authorized worker scaling locally**. All of this is **outbound
only**, and the agent authenticates with the same in-cluster agent-token as
the bash heartbeat.

> Status: **live view, worker scaling, self-healing and worker upgrades**,
> plus `machineIssues[]` failure surfacing and the lok8s `ClusterInventory`
> loop. In that loop the inventory goes out on the beat, and the addon
> `availableUpdates` come back to the CR status (visible via plain kubectl).
> The server gates every acting capability: it computes the `/desired`
> `execution{}` flags from tier × access × platform kill switch. The agent
> adds hard, unit-tested guardrails. An action that is authorized but not
> built is reported as unsupported, never improvised. Acting is
> single-writer (a leader-elected Lease), so it is safe to run more than one
> replica. Control-plane upgrades stay user-driven.

It complements, and does not replace, the lightweight bash **heartbeat CronJob**
(registered tier). The Go rewrite exists because node facts must be **correct**.
The bash agent repeatedly mis-parsed node JSON with `sed`: every node reported
`control-plane`, `kubectl version` returned the *client* tag, and a greedy scrape
invented a `runc` node. This agent reads **typed** objects, so those bugs cannot
recur. Unit tests keep the mapping as a regression guard.

## Design principles (hard invariants)

- **Outbound-only.** No inbound listener, no port, no HTTP server anywhere. The
  agent dials the apiserver (watch), the API's heartbeat POST, and the API's
  desired-state GET. The agent **opens every connection**. Nothing dials in,
  and the platform can never push a command.
- **No telemetry, ever.** The only network egress is the authenticated
  heartbeat + desired-state poll. There is no analytics or phone-home code
  path. The privacy guarantee is enforced *by construction* and auditable in
  this source.
- **Reuses the existing identity.** The agent invents no auth. It reads
  agent-token `A` (`khz_agt_<hex64>`) from the in-cluster Secret `kubehz-agent`
  and sends `Authorization: Bearer A` to the existing heartbeat endpoint. The
  server's authenticated-heartbeat ratchet applies unchanged.
- **Least privilege.** The base reads nodes/pods/events, the lok8s
  `ClusterInventory`, the CertificateSigningRequests (`list`) and the URLs
  `/readyz` and `/version`. It has a single name-scoped `get` on its own token
  Secret. No logs/exec, no other secret reads.
  - Every **acting** write is an opt-in RBAC overlay (`deploy/managed/`),
    absent from the base entirely. The acting writes are the MachineDeployment
    patch (replicas / kubelet version), the Machine **delete**, the pod
    **delete** (cluster-wide, for the eviction unwedge) and the acting Lease.
  - The Machine delete (self-healing) and the cluster-wide pod delete are the
    sharpest permissions the agent holds. `deploy/managed/rbac-managed.yaml`
    documents them, and you can remove each one independently.
  - With the acting Lease, the agent proves that it is the only actor before it
    writes to a machine or a pool.
  - The base's one write, `patch` on `clusterinventories/status`, is a
    visibility mirror on the cluster's own reporting object (see the inventory
    section), not an acting permission.
- **Fail toward report-only.** A partial read degrades that section to empty
  and does not abort. The agent retries a rejected push with backoff. Acting is
  server-gated, and every guard refuses and does not improvise. The agent never
  blocks the cluster.

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
| `internal/desired` | the pull loop: contract types (incl. the `healing` policy), ETag-aware client, Poller |
| `internal/machines` | grounded machine-controller API surface: GVRs, exact field paths, Machine→pool resolution |
| `internal/machineissues` | ungated, fail-soft `machineIssues[]` collector (terminal errors, retry-loop events, join timeouts) |
| `internal/executor` | the acting side: replica patches, heal deletes, kubelet rolls + every safety rail |
| `internal/actions` | in-memory action-report store (latest-revision-wins, diff-aware notify, stale-heal prune) |
| `internal/leader` | the acting Lease: leader election gating the desired-state loop (one actor, or none) |
| `internal/agent` | wiring: informers → coalescer → sender; poller → executor → actions → beats |
| `internal/buildinfo` | build-stamped version |
| `deploy/` | base (read-only, plus the `clusterinventories/status` patch) + `managed/` overlay (acting RBAC). The resources are named `kubehz-live-agent*` to coexist with the bash CronJob agent's `kubehz-agent*` RBAC (see `deploy/README.md`) |

The debounce policy is a **pure function** (`publisher.decide`), with no timers
in the tested code path. Thus unit tests cover the whole cadence (debounce /
min-gap / periodic) exhaustively.

## The payload (schema 2) and the endpoint

The agent POSTs to the **existing** heartbeat endpoint
`POST /api/clusters/{id}/heartbeat` with `Authorization: Bearer A`, carrying an
**additive** `schema: 2` body. The server's heartbeat schema is *non-strict*: the
API ingests the schema-2 fields that it knows (see "The API side of this
contract" below) and strips the others.

```jsonc
{
  "schema": 2,
  "clusterId": "cluster.example.com",
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
  "components": [                                    // control-plane health (schema-1 field, see below)
    { "name": "apiserver", "status": "Healthy" }, { "name": "etcd", "status": "Healthy" },
    { "name": "scheduler", "status": "Healthy" }, { "name": "controller-manager", "status": "Healthy" }
  ],
  "certificates": { "expiresAt": "2027-07-05T12:00:00Z" },  // earliest NotAfter on issued CSRs (absent when none)
  "workloads": {
    "pods": { "total": 42, "running": 40, "pending": 1, "failed": 1,
              "succeeded": 0, "unknown": 0 }        // counts only, no names
  },
  "events": [                                        // recent Warnings (capped 20)
    { "reason": "BackOff", "kind": "Pod", "count": 3, "lastSeen": "…" }
  ],
  "actions": [                                       // desired-state progress (capped 20)
    { "type": "scale", "target": "pool-a", "status": "done",
      "detail": "replicas 2 to 3; machine-controller reconciles the machines with the cluster's own credentials",
      "revision": 7 },
    { "type": "upgrade", "target": "pool-a", "status": "in-progress",
      "detail": "v1.34.9 → v1.35.6 (1/3)", "revision": 7 },     // target = POOL name
    { "type": "heal", "target": "pool-a-5b6c7-xk2lp", "status": "done",
      "detail": "deleted Machine (node w-3 NotReady for 6m0s (unhealthyAfter 5m0s)); machine-controller/MachineSet recreates it with the cluster's own credentials",
      "revision": 7 }                                            // target = MACHINE name
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
  // forward-compat, not sent yet: "pools":[…], "desired":{…}
}
```

**`components[]` and `certificates`** are the two schema-1 facts the bash
CronJob reports. Operator mode silences the CronJob (lok8s
`KUBEHZ_HEARTBEAT_OWNER=operator`, exactly one producer beats). So the live
agent reads them itself (`internal/controlplane`), in the CronJob's way:

- apiserver and etcd from `GET /readyz?verbose`, with `/version` as the
  reachability fallback;
- scheduler and controller-manager from the Ready condition of the kube-system
  static pods (the pod informer cache, no extra RBAC);
- `certificates.expiresAt` as the earliest `NotAfter` across the certificates
  issued on CertificateSigningRequests (paged, 500 per List, at most 20 pages
  per beat; past that the agent omits the field).

The RBAC delta is two read-only rules: `certificatesigningrequests` list (public
material only) and `nonResourceURLs` `/readyz`, `/version` get. Approved CSRs
are garbage-collected an hour after issuance, so `certificates` is present
only while a recent rotation is visible. That matches the CronJob's
coverage. Every probe fails soft: a failed read omits its entry, never a
false `Unhealthy`.

**Action targets are opaque identifiers.** For `heal` the target is the
**Machine** name (stable pre-join: a machine whose node never appeared has
no node name at all). The node name, when known, rides in `detail`. For
per-pool `upgrade` progress the target is the **pool** name. Dashboards
should render targets verbatim, never parse them.

**`machineIssues[]`** is pure observation, deliberately **ungated** by the
execution flags. A pool that never converges is otherwise invisible, because its
machines never become nodes. The agent reads three grounded sources off the
Machine objects that machine-controller maintains:

- **terminal** `status.errorReason`/`errorMessage` ("manual intervention
  required": InvalidConfiguration, CreateError, …);
- **retry-loop**: every Warning event on a Machine that still exists. A typical
  reason is `ReconcilingError`. Here lives "webhook accepted it, hcloud rejects
  every create: *unsupported location for server type*". It is never a status
  field;
- an agent-synthesized `NodeJoinTimeout` for machines with no node 10 minutes
  after creation.

The collector fails soft: without the managed overlay's machines read RBAC (or
without the CRD) the block is absent. `since` is the agent-observed first-seen
time, stable across passes.

**`inventory`** mirrors the **spec** of the lok8s `ClusterInventory` CR, field
names verbatim. That CR is `clusterinventories.lok8s.dev/v1alpha1`, the
cluster-scoped singleton named `cluster`. The user's own `lo` CLI
server-side-applies it at the end of `provision`/bootstrap. The agent GETs it
periodically at the full-beat cadence, because it only changes on lo deploys. (An
informer on a possibly-absent CRD would error-loop on every never-lok8s-deployed
cluster.) It fails soft: no CRD/CR/RBAC → no `inventory` key at all, zero noise.

**Data inventory / privacy class:** the block is metadata the *user's own
tooling already wrote into their own cluster*: lok8s/driver/k8s versions, a
spec hash, and addon names + pinned chart versions/categories. The CRD schema
itself prunes anything beyond those fields. No chart values, env overrides,
credentials, or rendered manifests can exist in the CR. So it is the same
privacy class as the version fields and is **not** gated by
`KUBEHZ_REPORT_NAMESPACES`.

**The write-back: addon updates via plain kubectl.** The heartbeat
*response* carries `availableUpdates: [{name, current, latest}]`. The platform
computes it from the reported inventory against the published lok8s addons
index. The key is **tristate**:

- **absent** = no verdict: the beat carried no inventory, or the index was
  unreachable. The server omits the key precisely so an outage never wipes the
  agent's last known status.
- **present `[]`** = index consulted, nothing is newer. The agent **clears**
  `status.availableUpdates`. Without this, a user who upgrades their addons
  would keep stale "update available" notices forever.
- **non-empty** = written as-is.

Every verdict lands, with `lastReported` (RFC3339), on the CR via the **status
subresource** (JSON merge patch, field manager `kubehz-agent`), so:

```bash
kubectl get clusterinventory cluster -o yaml   # status.availableUpdates, no dashboard needed
```

Boundaries:

- The agent never writes the spec (lo-owned).
- It never touches the sibling `status.observedAddons` (merge semantics).
- The write is idempotent. The agent compares it against the CR's current
  status, refreshed every poll and after every write. So restarts and repeat
  responses, including an already-clear `[]`, cost zero patches.
- Idempotence is keyed on a status **existing**. On a fresh CR with no recorded
  status the first verdict always writes, even `[]`. So `lastReported` gets
  stamped, and never-checked becomes checked-and-clear.
- A clear is patched as an explicit `[]`, never `null`. Merge-patch `null` would
  delete the field, and `[]` keeps "checked, nothing newer" distinguishable from
  never-checked.
- An RBAC-denied patch warns **once**, and beating continues.
- The agent clamps server input to the CRD's own status bounds before it
  patches. It refuses a non-empty verdict whose entries are all invalid, and
  does not treat it as a clear.

The server persists `actions` **latest-wins** and treats an absent `actions`
key as *clear*. So the agent keeps reporting the current revision's actions
on every beat while they are relevant. It stops (and so clears them) when acting
is switched off or a new revision produces nothing to do.

`state.ApplyCaps` caps every field to the server's heartbeat bounds (nodes ≤ 500,
name ≤ 253, status ≤ 63, …), so a well-behaved agent is never 400'd.

**Privacy gating.** `byNamespace` pod counts and event `namespace`/`note` are
included **only** when `KUBEHZ_REPORT_NAMESPACES=true`. The default is phase-only
counts and reason/kind-only events: workload *visibility* without workload
*contents*. The gate does not cover `machineIssues[]`: with the managed overlay,
that block carries the Machine name and the message text of the error or event.

The API side of this contract is **shipped**:

- schema-2 ingestion (nodes/workloads/events/observed version, `connected`
  tightening from `agent.mode`);
- the `actions[]`/`machineIssues[]` persistence;
- `GET /clusters/{id}/desired` incl. the `healing` block + `heal` action enum;
- the **inventory loop**: `inventory` ingestion (validated + capped) plus the
  tristate `availableUpdates` response described above. `GET /clusters/{id}`
  shows them as `inventory` + `addonUpdates`.

Note the addons **hard cap of 100** on ingestion. It is why `state.MaxAddons` is
100 and not the CRD's 256: one entry more would 400 the whole beat. The
`pools[]`/`desired{}` blocks remain accepted-but-stripped, and the agent leaves
them empty until the API ingests them. Should the response shape ever move,
`internal/publisher`'s `heartbeatResponse` is the single place to adjust.

## Desired state & acting: the honest model

The agent executes exactly three capabilities: **worker-pool replica scaling**,
**worker self-healing**, and **worker kubelet upgrades**.
How they work, truthfully:

1. **The platform never pushes.** The agent polls
   `GET /api/clusters/{id}/desired` (bearer `A`, strong ETag, `If-None-Match`
   → cheap `304`) every `KUBEHZ_DESIRED_POLL_SECONDS` (default 60, + up to
   10% jitter). The document carries
   `workerPools[{name, machineType, desiredReplicas}]`, a **server-computed**
   `execution{scaling, upgrades, healing}` block (tenant tier × cluster
   access × platform kill switch), and the **server-owned**
   `healing{enabled, maxUnhealthy, nodeStartupTimeoutSeconds,
   unhealthyAfterSeconds, cooldownSeconds}` policy. The ETag is an **opaque
   token**: the agent caches it, echoes it verbatim and never parses it. A
   server-side format change costs one extra `200`, nothing else.
   **Stale intent is not intent.** Neither a `304` nor a `200` proves
   freshness. A proxy, a CDN or a broken gateway can serve either one from a
   frozen copy forever, and healing asks to re-run on *every* tick. Every
   request carries `Cache-Control: no-cache`, so a cache that obeys it must
   ask the platform first. For a cache that does not, the agent
   **revalidates** on its own cadence. Every 5 polls, whatever those polls
   returned, it fetches **unconditionally** (no `If-None-Match`, so no cache
   may answer that with a `304`). Only a document served to that request
   counts as proof. Past 10 polls without proof an armed healer **stops
   acting**, and past 60 every other retry stops. The agent reports the
   refusal as a failed action ("the agent does not act on intent nobody
   re-affirmed"). Healing deletes machines, which is why it gets the tighter
   bound. The next successful revalidation clears it.
   **Unchanged intent is not re-run.** A revalidation that returns the same
   document is treated like a `304`. (The agent compares it in full, because
   the flags can change without a new revision.) A pass that already converged
   does not run again, so a manual change made since (a pool scaled by hand)
   stays. Only new intent, or a pass that asked for a re-run (a transient
   failure, an armed healer), runs.
2. **Acting is server-gated with no local enable.** There is *no* agent-side
   configuration that can turn any execution on. When the server says
   `false`, the agent acts on nothing and clears its reports. A **rejection**
   (401/403, any other 4xx except 408/429, or a document that fails
   validation) also **revokes** the cached document and its ETag. A later
   `304` cannot bring it back, and the agent acts again only on a new
   document. An **incomplete** document is rejected whole: a pool without
   `desiredReplicas`, or an enabled healing policy with a missing number.
   (Missing or `null` is not zero: zero would remove every server of the pool.)
   So is complete JSON with a field of the wrong type (`"desiredReplicas":
   "3"`) and a body above 1 MiB. Only a body that is not complete JSON (a
   transfer cut short) counts as a transient failure and keeps the cached
   document until its freshness bound. A
   tier downgrade or kill-switch flip reaches the agent on its next poll
   (the flags are part of the ETag). 401/403 honor the full retry backoff,
   exactly like the heartbeat sender.
3. **Scaling: one field of one resource.** For each desired pool it
   merge-patches `spec.replicas` on the MachineDeployment **of the same
   name** in `KUBEHZ_MD_NAMESPACE` (default `kube-system`). The GVR is
   `machinedeployments.cluster.k8s.io/v1alpha1`, KubeOne's machine-controller
   group (**not** `cluster.x-k8s.io`). The pool↔MD **name equality is the
   mapping contract**. An unmatched pool is skipped and reported `failed`.
   Three things the scaling loop deliberately does **not** do, each reported
   rather than improvised:
   - **It never creates a pool.** A MachineDeployment carries the image,
     network, SSH keys and cloud-init of your own provisioning, which the
     agent may not invent (and the RBAC overlay grants `patch`, never
     `create`). A desired pool with no MD reports `failed` ("the agent never
     creates pools; create it with your own tooling (`lo provision` /
     `kubeone apply`)"), not a forever-pending promise.
   - **It never deletes a pool.** A MachineDeployment the document does not
     declare is left alone and named once in the log. A delete would tear
     down every server in the pool, and a document that dropped a pool looks
     exactly like a partial one. Removing a pool stays a deliberate operator
     action.
   - **It never changes a machine type.** `workerPools[].machineType` is read
     and *reported*: when it differs from the pool template's declared type
     the scale action's detail says so ("machineType cpx41 not applied (pool
     runs cpx31)"). Applying it would replace every server in the pool. An
     unreadable template type reports nothing: unknown is not a mismatch.
4. **Self-healing: delete the Machine, let the cluster rebuild it.** The
   agent acts on a worker node that is continuously NotReady/Unknown for
   `unhealthyAfterSeconds`, or on a machine whose node never appeared within
   `nodeStartupTimeoutSeconds`. It **deletes the backing Machine**, and
   machine-controller/MachineSet recreates it. Guardrails (hard,
   refusal-biased, all unit-tested):
   - **Never control-plane**: node role labels, machine labels, and the same
     CP-MD heuristic scaling uses.
   - A **storm brake**: more unhealthy than `maxUnhealthy` deletes *nothing* and
     reports every candidate `failed`. The detail says "unhealthy count N
     exceeds maxUnhealthy M" and "refusing (possible outage)".
   - At most `maxUnhealthy` disruptions in flight (any already-deleting machine
     counts).
   - A per-pool `cooldownSeconds`, **never below 5 minutes** whatever the
     document says, with the agent's start time as baseline. **Restart = fresh
     cooldown**: a crash-looping agent can never become a machine-delete loop.
   - The agent refuses autoscaler-owned pools and unowned machines.
   - Healing re-evaluates every poll tick. Detection windows are floored at
     60 s agent-side.

   **Three bounds owe nothing to the server's numbers.** `maxUnhealthy`, the
   cooldown and the budget all arrive in the document. One
   wrong-but-plausible number (`maxUnhealthy: 50` on a 12-machine cluster)
   satisfies every one of them:
   - **Simultaneous failure is an outage, not hardware.** When *every* node of
     a pool goes unhealthy inside 5 minutes, the agent refuses to remediate
     anything there and reports why. The same rule applies to the whole worker
     set, which is how one-machine pools are covered. The rule needs at least
     two nodes: it does not cover a cluster with one worker. Independent
     hardware does not fail together. A partition, an apiserver outage or a control-plane
     restart does, and those leave the servers alive and running your
     workload while their nodes read `Unknown`. The agent cannot tell a dead
     node from a partitioned one, so it does not guess.
   - **Missing evidence refuses.** A not-Ready node that reports no Ready
     transition time could have failed at the same moment as all the others,
     so its pool is not healed until it does, and across the worker set it
     counts as failed inside the window.
   - **Healing acts on a pool only while at most a third of it is out of
     service** (rounded down, floored at one machine, so small pools still
     heal). Out of service is every machine slot without a Ready node: the
     unhealthy machines themselves, a machine being deleted, a replacement
     that has not joined or is not Ready yet (also one that failed and is a
     candidate now), a replica that does not exist yet. Deleting a machine
     that does not serve leaves the count as it is. Only a machine that
     serves again lowers it. So healing's own deletions never put more than
     a third of a pool out of service, across passes and restarts. A pool
     that is out beyond that is not healed at all: every candidate waits and
     is reported. This bounds the blast radius by the size of the pool, not
     by a number the platform sent.

   **Eviction unwedge (the bounded follow-through).** When the healed node is
   truly dead (kubelet stopped), machine-controller's eviction of the pods
   stuck Terminating there can never confirm. So the deleted Machine sits in
   teardown indefinitely: a zombie server that bills the user (observed live:
   ~12 min until manual intervention). The agent acts only on a machine that
   **this agent itself heal-deleted**, that is still deleting after
   `KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS` (default 300), *and* whose node is
   still unreachable (Ready Unknown/False). Then it force-deletes exactly the
   pods **already Terminating on that node** (grace 0, UID-preconditioned so a
   same-name replacement is never hit), **once per machine** and at most 100
   pods. It reports this on the heal action ("eviction unwedged after …:
   force-deleted N pod(s) stuck terminating on unreachable node …"). The
   agent never touches a manually-deleted machine (in-memory tracking: a
   restart conservatively forgets) and leaves a recovered node alone. Without
   the pods-delete ClusterRole the attempt warns loudly and fails soft, while
   healing keeps working.
5. **Upgrades: roll workers toward the declared version.** When the
   desired `kubernetesVersion` differs from a pool MD's declared kubelet, the
   executor patches `spec.template.spec.versions.kubelet` (the exact
   machine-controller field path), and machine-controller performs the rolling
   machine replacement. **Pre-flight:** the *observed* control-plane version
   must be at/above the target minor, else `failed` "control plane not yet
   at target". (Upgrade the CP first via `lo provision` / `kubeone apply`.
   An unknown observed version refuses, never guesses.) **One pool rolls at
   a time.** Progress reports per pool as `"vFROM → vTO (n/m)"` (n = machines
   at target with a joined node). Autoscaler pools report unsupported. A
   failed patch stops that pass (halt-on-failure): the later pools wait, and
   the next poll tries again. Workers only: the agent never upgrades a control
   plane.
6. **The cluster's own machinery does the provisioning.** Every write wakes
   the in-cluster machine-controller, which creates/deletes Hetzner servers
   with the credentials **it already holds**. The agent contains no hcloud
   token, no SSH key, no provisioner. To revoke the platform's access, run
   `kubectl delete ns kubehz-system`. This deletes everything in the namespace:
   both agents, their ServiceAccounts and the Secret with the cluster identity.
   So the heartbeats stop too. The ClusterRoles and the Role in `kube-system`
   stay without a subject until you delete them. Your nodes, pools and workloads
   do not change.
7. **Exactly one replica acts.** Acting is gated on a leader-elected Lease
   (`kubehz-system/kubehz-live-agent`, RBAC in `deploy/managed/`): the poller
   and the executor start only while this replica holds it. When the lease
   is lost, or the agent shuts down, acting is cancelled **first**, and the
   agent waits for it to return before anything else happens: it does not
   contend again while acting may still run, and on shutdown it keeps the
   lease renewed until acting has returned and only then releases it. The
   successor begins with a fresh cooldown baseline and this replica clears
   its action reports. The reason is concrete: the per-pool cooldown, the
   in-flight budget and the one-pool-at-a-time roll are per-*process* state,
   so two actors would double every one of those bounds.
   **No Lease, no acting**: without the coordination RBAC (or the API) the
   agent keeps reporting and never acts. Before it contends, it reads the
   Lease once. When the apiserver refuses that read, as on the base install,
   the agent says so once in its log and does not contend. It reads the
   Lease again every 5 minutes, so a grant that you apply later starts
   acting without a restart. A grant removed later has the same effect: the
   next refused read ends the election (and stops acting at the next
   renewal), and the agent goes back to that read. In every other case it
   contends, and after two
   minutes without the lease it says so in its log. It cannot tell a peer
   holding the lease from a missing write grant or an unreachable
   apiserver, so that warning names all three. Running more than one
   replica is therefore safe. While a peer holds
   the lease, a standby sends **no heartbeats**: the platform keeps one
   action snapshot per cluster and reads a beat without actions as "clear",
   so a standby's beats would erase the holder's reports. The holder reports
   the same live view. The check runs before every send, a retry too, so a
   beat queued before the standby saw the peer is dropped, not sent late. A
   standby reports again when the lease is free, when a read finds it
   unchanged for one lease duration, or when the apiserver answers that the
   Lease does not exist (deleted). Only the apiserver's own answer counts: a
   404 page from a proxy or a router is not one. Any other failed read
   changes nothing, so a standby that cannot read the Lease stays silent.
   Because the holder is then the only reporter, **a holder whose
   heartbeats fail for 5 minutes steps down**: it stops acting, releases
   the lease, and does not contend again until one of its heartbeats goes
   through, so a replica that can reach the platform takes over. On
   shutdown the process waits for the drain and the release before it exits
   (the default 30 s grace period covers both). Upgrading from an agent
   that predates this: re-apply `deploy/managed/`.
8. **Progress is reported, in memory only.** Every outcome
   (`pending → in-progress → done/failed`, with the acted `revision`) rides
   the heartbeat's `actions[]`; stale heal reports for self-recovered nodes
   are pruned. Restart = re-poll + reconverge; because the executors are
   idempotent, that costs zero cluster writes.

The acting RBAC is an **opt-in overlay**, `kubectl apply -k deploy/managed/`,
absent from the registered-tier base. It holds:

- list + `patch` on machinedeployments (replicas + kubelet version) and list +
  **delete** on machines (kube-system-scoped Roles);
- a `coordination.k8s.io` Lease Role in `kubehz-system`: the single-actor proof,
  without which the agent refuses to act at all;
- a pods-**delete**-only ClusterRole for the eviction unwedge. The stuck pods
  span arbitrary namespaces, which no namespaced Role can express.

The machines **delete** verb exists solely for self-healing and is loudly
documented in `deploy/managed/rbac-managed.yaml`. Drop that one verb and healing
fails closed (reported `Forbidden`) while everything else keeps working. Drop the
unwedge ClusterRole and only the unwedge is disabled: healing itself is
unaffected. See [deploy/README.md](deploy/README.md).

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `CLUSTER_ID` | none (required) | cluster domain, e.g. `cluster.example.com` |
| `KUBEHZ_API_URL` | none (required) | api base; **https** (http only for loopback) |
| `KUBEHZ_AGENT_TOKEN_FILE` | `/var/run/secrets/kubehz/agent-token` | mounted bearer A |
| `KUBEHZ_AGENT_TOKEN` | none | token via env (discouraged; tests/local only) |
| `KUBEHZ_NAMESPACE` | `kubehz-system` | namespace of the Secret for the API fallback |
| `KUBEHZ_SECRET_NAME` | `kubehz-agent` | name of the Secret for the API fallback |
| `KUBEHZ_FULL_INTERVAL` | `60s` | periodic full-snapshot push |
| `KUBEHZ_DEBOUNCE` | `10s` | coalesce a change burst |
| `KUBEHZ_MIN_GAP` | `15s` | floor between two pushes |
| `KUBEHZ_REPORT_NAMESPACES` | `false` | opt into namespace/message reporting |
| `KUBEHZ_DESIRED_POLL_SECONDS` | `60` | desired-state pull cadence (integer seconds; + ≤10% jitter) |
| `KUBEHZ_MD_NAMESPACE` | `kube-system` | where the executor looks for MachineDeployments |
| `KUBEHZ_MAX_REPLICAS` | `50` | per-pool ceiling; out-of-bounds desired is **refused**, not clamped |
| `KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS` | `300` | how long a heal-deleted machine may sit deleting (node still dead) before the one-shot eviction unwedge; min 60 |
| `KUBEHZ_POD_NAME` | none (downward API) | this replica's name in the acting Lease's `holderIdentity`; falls back to the hostname |
| `KUBEHZ_LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

None of these enable acting. Only the server's `/desired` `execution{}` flags
plus the opt-in `deploy/managed/` RBAC authorize execution. The envs above are
guard-rails only.

Token resolution order: env → mounted file → k8s-API Secret read (fallback). The
Deployment mounts the Secret as a **read-only file** (default), so no Secret-`get`
RBAC is exercised.

**Re-enrollment without a restart.** `lo kubehz re-enroll` rotates the
in-cluster Secret. The kubelet refreshes the projected file within its sync
period (about a minute). When the server rejects the token (401/403), the
heartbeat sender and the desired-state poller re-read it from where it came
from: the mounted file, or the Secret on the API-fallback path. If it
changed, they retry at once with the new token. An unchanged token keeps the
full backoff. A token given via `KUBEHZ_AGENT_TOKEN` (env) cannot change, so that
path needs a rollout restart.

## Build, test, run

```bash
go build ./...
go test -race ./...
go vet ./... && gofmt -l .

# Integration: the SCALING loop against a REAL apiserver (every other test
# drives a fake client). CI runs this on a throwaway kind cluster; by hand:
kind create cluster --name kubehz-agent-it \
  --image kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0
go test -tags integration -count=1 -v ./test/integration/...
kind delete cluster --name kubehz-agent-it

# container (pinned distroless/static, non-root, read-only rootfs)
docker build -t kubehz-agent:dev --build-arg VERSION=dev .

# deploy (after the kubehz-agent Secret is bootstrapped by the lok8s plumbing);
# coexists with the bash CronJob agent: see deploy/README.md for the naming
# contract and the image digest-pinning rules. One exact release: apply its
# install.yaml or install-managed.yaml (deploy/README.md). From this tree, main
# pins the newest release, and a tag pins the release before it:
kubectl apply -k deploy/          # registered tier: observe + report only
kubectl apply -k deploy/managed/  # managed tier: + acting RBAC (scale/upgrade
                                  #   patch, machineIssues read, heal delete,
                                  #   pod delete, acting Lease)
```

Toolchain: **Go 1.26** · **client-go v0.35.6** (matches Kubernetes v1.35.x).
Versions are verified against their registries before pinning, never guessed
(see `AGENTS.md`).

## Security & supply chain

- **Image:** distroless `static-debian12:nonroot`, pinned by digest; static
  `CGO_ENABLED=0` binary; runs non-root (65532), read-only rootfs, all caps
  dropped, seccomp `RuntimeDefault`.
- **The token `A` is never logged or printed.** `Config.String()` redacts it.
- **The claim code `C` is never mounted or used.** The Deployment mounts only the
  `agent-token` key. The API fallback reads the whole Secret object, and that
  object also holds `claim-code`. The agent takes only `agent-token` from it.

### Two lanes: `ci.yaml` and `release.yaml`

`ci.yaml` is the continuous lane. Every push to `main` runs
`gofmt`/`vet`/`golangci-lint`/`go test -race`/`govulncheck`, then builds the
moving `main` image and its copy in the internal mirror.

`dep-scan.yml` runs `osv-scanner` each day on `main`: a new advisory shows without
a commit. It matches offline against a downloaded archive of advisories, so it
sends nothing about the dependencies. It reads every `go.mod` and the standard
library of the image build. An accepted finding is an entry in
`.github/dep-scan-ignore.json` with a reason and an expiry date.

`release.yaml` cuts the customer artifact for a `vX.Y.Z` git tag. Tags belong to
it alone, and `ci.yaml` skips them. Two workflows that build one tag would
produce two different digests for one version, and the registry tag would then
point at whichever run finished last.

What a release publishes:

| Requirement | How it ships |
|---|---|
| **Public** image | `ghcr.io/kernpilot/kubehz-agent`, anonymously pullable. GHCR is the only registry on the critical path; the internal mirror is copied afterwards and cannot fail a release. |
| **Multi-arch** | `linux/amd64` + `linux/arm64`. A step reads the published index back and fails the release if either platform is missing. |
| **Distroless/static base** | `gcr.io/distroless/static-debian12:nonroot`, itself pinned by digest in the `Dockerfile`. |
| **cosign-signed** | Keyless, through the Actions OIDC identity. No long-lived key exists. |
| **SBOM attached** | SPDX, generated from the pushed image, attested with `cosign attest --type spdxjson`, and uploaded to the release. BuildKit's own SBOM and SLSA provenance also ride inside the image index. |
| **Pinned by digest in the manifests** | `deploy/base/deployment.yaml` carries a digest, never a tag. The release renders `install.yaml` / `install-managed.yaml` with the released digest and refuses to publish if a moving tag survived. |
| **Verifiable** | The workflow runs the `cosign verify` and `cosign verify-attestation` commands of [deploy/README.md](deploy/README.md) against the pushed image, before it publishes the GitHub Release. If one fails, there is no release for that digest. |

Actions are pinned by commit SHA in both lanes. A tag can move; a commit
cannot.

### How the agent is updated

There is no self-update and no auto-pull: the agent runs the digest its
manifest pins until the manifest changes. To move to a newer version, apply
the newer `install.yaml` / `install-managed.yaml` (or re-apply `deploy/`
after bumping the digest in `deploy/base/deployment.yaml`). On a lok8s
cluster, re-run `lo kubehz deploy` after lok8s re-vendors `deploy/`
(`internal/kubehz/manifests/live-agent/`, digest-pinned). Rolling back is
the same step with the previous digest. Nothing on the platform side can
change which agent image runs in a customer's cluster.

### Publishing a release

The repository is public and the GHCR package is already public: a customer
can pull `ghcr.io/kernpilot/kubehz-agent` today with no credentials. So cutting a
version is one owner action and nothing else:

```bash
git tag -a vX.Y.Z -m "kubehz-agent vX.Y.Z" && git push origin vX.Y.Z
```

That runs `release.yaml`: verify, build, sign, attest, verify again, then
publish the GitHub Release with `install.yaml`, `install-managed.yaml`,
`kubehz-agent.spdx.json`, `digest.txt` and `SHA256SUMS`.

If a run dies halfway, re-run the workflow by hand (**Actions → Release → Run
workflow**) with the same tag. Every step is idempotent.

After the release run, pin `main` to it. Set the image in `deploy/base/deployment.yaml`
and the digest in the two override examples (`deploy/kustomization.yaml`,
`deploy/base/kustomization.yaml`) to the release's `digest.txt`. Check that
`kubectl kustomize deploy/` and `deploy/managed/` render the image of
`install.yaml` and `install-managed.yaml`. The tag itself keeps the digest of
the release before it, because the release run makes the new digest from the
tag.

The internal mirror step needs the `HARBOR_USERNAME` / `HARBOR_PASSWORD`
repository secrets. Without them it prints a line and skips. The mirror is
private and only a convenience, so its absence must never hold up a customer
release.

## Follow-ups

- **Dashboard** live surfaces: node capacity and instance type, a workloads
  card, "reported n s ago". A reader of the payload uses the `ready` bool, not
  the `status` text (`NotReady` contains `Ready`).
- **Observed pools:** a MachineDeployment informer feeding `pools[]` (observed
  replicas/ready per pool) once the API ingests it. It closes the
  desired→observed convergence loop in the dashboard. Also an apps deployments
  summary.
- **Field-level deltas:** the current push is a change-triggered *full* snapshot
  (latest-wins, matching the server JSONB). Delta encoding is a future
  optimization if payload size ever matters.

## License

[MIT](LICENSE).
