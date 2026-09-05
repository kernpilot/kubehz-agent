# Deploying kubehz-agent (live view + desired state)

```bash
# Registered tier — observe + report only (no ACTING RBAC; the single write
# is the ClusterInventory status mirror, see below):
kubectl apply -k deploy/

# Managed tier — adds the acting permission for server-gated worker scaling:
kubectl apply -k deploy/managed/
```

## The base/managed RBAC split

The base carries **no acting permission**: read-only on nodes/pods/events and
the lok8s `ClusterInventory` (`clusterinventories.lok8s.dev`), plus one
narrowly-scoped write — `patch` on `clusterinventories/status`. That write is
a **visibility feature, not acting** (which is why it lives in the base, so
registered-tier users get it too): the agent mirrors the server-computed
addon `availableUpdates` onto the status of the cluster's own reporting
object, making updates `kubectl`-visible without a dashboard
(`kubectl get clusterinventory cluster -o yaml`). Nothing reconciles that
status — no machine, pod, or credential can be moved through it — and the
status subresource cannot touch the lo-owned spec. Dropping the rule only
loses the kubectl mirror (the agent warns once and keeps beating).

`deploy/managed/` is the base **plus** `rbac-managed.yaml`, which grants
exactly three things to the `kubehz-live-agent` ServiceAccount:

1. **MachineDeployment patch** (namespaced Role, kube-system):
   `get,list,watch,patch` on `machinedeployments.cluster.k8s.io` (KubeOne's
   machine-controller API group — **not** `cluster.x-k8s.io`). Patch covers
   the two fields the executors edit: `spec.replicas` (P3 scaling) and
   `spec.template.spec.versions.kubelet` (P6 worker rolls).
2. **Machine read + delete** (same Role): reads feed `machineIssues[]` and
   the P5/P6 loops; **delete is the P5 self-healing permission** — loudly
   documented in the file, removable independently (healing then fails
   closed as a reported Forbidden).
3. **Pods delete** (ClusterRole `kubehz-live-agent-eviction-unwedge` —
   delete verb ONLY): exists solely to **unwedge evictions of heal-deleted
   machines' dead nodes**. When healing deletes a Machine whose node is
   truly unreachable, the pods stuck Terminating there never confirm and
   machine-controller retries eviction forever — a zombie server billing the
   user. After `KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS` (default 300) the
   agent force-deletes exactly those already-Terminating pods, once per
   machine, UID-preconditioned, and only while the node is still
   unreachable. It has to be a ClusterRole (the stuck pods span arbitrary
   namespaces); drop it and the unwedge is disabled while healing keeps
   working.

Every write is executed only when the platform's `/desired` document
authorizes it — the overlay grants ability, never intent.

**Registered-tier users should apply the base, not the overlay** — acting is
server-gated off for their tier, so the extra permissions buy nothing and
least privilege says an unused write should not exist. Without the overlay a
(mis)authorized acting attempt fails closed as a reported Forbidden action.

If you relocate the pools (`KUBEHZ_MD_NAMESPACE`), move the Role's
`namespace` in `rbac-managed.yaml` accordingly — the machine-side grants are
deliberately namespaced Roles.

## Coexistence with the bash heartbeat CronJob (read this first)

Two kubehz agents can run in the same cluster:

| | bash heartbeat (registered tier) | **this agent** (live view, managed tier) |
|---|---|---|
| workload | CronJob `kubehz-heartbeat` (lok8s-managed) | Deployment `kubehz-live-agent` |
| SA / RBAC / ConfigMap prefix | `kubehz-agent*` | `kubehz-live-agent*` |
| ClusterRole rules | nodes, componentstatuses, namespaces, CSRs, `/readyz`, `/version`, kube-system pod list, Secret **create** | nodes/pods/events/clusterinventories `get,list,watch` + CSRs `get,list` + `/readyz`,`/version` `get` + clusterinventories/status `patch` + scoped Secret `get` |

The name split is deliberate and load-bearing: the two RBAC sets are
**different**, so reusing the `kubehz-agent*` names would overwrite the
CronJob's ClusterRole/Role and break its component-health heartbeat. Applying
this kustomization never touches the CronJob agent; it may **coexist with or
replace** it. The live agent reports `components[]` and `certificates` itself
(the CronJob's reads, ported — README "The payload"), so replacing the
CronJob loses nothing; lok8s does exactly that in operator mode
(`KUBEHZ_HEARTBEAT_OWNER=operator` silences the CronJob so one producer
beats).

Shared objects (used, never modified, by this stack):

- **Namespace `kubehz-system`** — created if absent. Carries **no**
  `pod-security.kubernetes.io/enforce` label on purpose: the namespace is
  shared, and enforcing `restricted` would block the CronJob's pods. The live
  agent's own pod spec is restricted-compliant regardless.
- **Secret `kubehz-agent`** (keys `agent-token`, `claim-code`) — the cluster's
  identity, bootstrapped by the lok8s heartbeat plumbing (create-if-absent,
  never rotated). The Deployment mounts only the `agent-token` key read-only.
  **Do not rename references to this Secret**; both agents authenticate with it.

## Image

The Deployment pins the image **by digest** (a multi-arch index — `linux/amd64`
and `linux/arm64`, because customer worker nodes are not all x86). The
committed default is the **public** package
`ghcr.io/kernpilot/kubehz-agent@sha256:…`, which anyone can pull without a
registry login. That is the ref a customer cluster resolves.

It used to default to the internal Harbor mirror
(`docker.kubehz.in.net/kubehz-builds/kubehz-agent`). Harbor is private and
IP-allowlisted, so the shipped default was an `ImagePullBackOff` everywhere
except our own pilot. Harbor is still mirrored by digest for dogfood pulls, but
it is now the **override**, not the default:

```yaml
# deploy/kustomization.yaml (or deploy/managed/kustomization.yaml)
images:
  - name: ghcr.io/kernpilot/kubehz-agent
    newName: docker.kubehz.in.net/kubehz-builds/kubehz-agent
    digest: sha256:<the same digest>
```

The same override retargets an air-gapped mirror or a pull-through cache. Keep
the digest and change only the registry — one build is mirrored by digest, so
the bytes are identical either way.

Never deploy a moving tag (`main`, `latest`). A digest is the only ref that
`cosign verify` and a rollback can reason about. The release artifacts hold to
the same rule: `install.yaml` and `install-managed.yaml` on each GitHub Release
are rendered from this tree with that release's digest already substituted, and
the release workflow fails if either still carries a tag.

### Verify before you run it

Every released digest is signed keyless through the GitHub Actions OIDC
identity, with an attested SPDX SBOM. There is no key to trust and no account
to hold — the certificate names the workflow, repo and ref that built the
bytes.

```bash
digest=sha256:...                       # from digest.txt on the release, or the manifest above

cosign verify \
  --certificate-identity-regexp '^https://github.com/kernpilot/kubehz-agent/\.github/workflows/release\.yaml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "ghcr.io/kernpilot/kubehz-agent@${digest}"

cosign verify-attestation --type spdxjson \
  --certificate-identity-regexp '^https://github.com/kernpilot/kubehz-agent/\.github/workflows/release\.yaml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "ghcr.io/kernpilot/kubehz-agent@${digest}"
```

The release workflow runs both commands against the published image before it
publishes the release, so a digest that a customer cannot verify never ships.
Images built from `main` by `ci.yaml` are signed under that workflow's identity
instead — widen the regexp to `/\.github/workflows/` if you verify one of those.

## Migrating from the pre-rename scaffold

Early scaffold manifests (before 2026-07-06) named these resources
`kubehz-agent*`, silently overwriting the CronJob agent's RBAC. If such a
cluster exists:

1. `kubectl apply -k deploy/` (creates the `kubehz-live-agent*` set; the old
   Deployment keeps running until step 2).
2. Delete the old Go-agent resources: `kubectl -n kubehz-system delete deploy
   kubehz-agent` (the SA/ClusterRole/CRB named `kubehz-agent` belong to the
   CronJob agent — leave them, but their **rules** were clobbered, so:)
3. Re-apply the bash agent's manifests (lok8s `libs/kubehz/manifests/agent/`)
   to restore its ClusterRole/Role rules, and remove the
   `kubehz-agent-secret` Role/RoleBinding drift the scaffold introduced there
   (the lok8s copy grants Secret `create` for bootstrap; the scaffold's did not).
