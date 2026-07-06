# Deploying kubehz-agent (live-view, managed tier)

```bash
kubectl apply -k deploy/
```

## Coexistence with the bash heartbeat CronJob (read this first)

Two kubehz agents can run in the same cluster:

| | bash heartbeat (registered tier) | **this agent** (live view, managed tier) |
|---|---|---|
| workload | CronJob `kubehz-heartbeat` (lok8s-managed) | Deployment `kubehz-live-agent` |
| SA / RBAC / ConfigMap prefix | `kubehz-agent*` | `kubehz-live-agent*` |
| ClusterRole rules | nodes, componentstatuses, namespaces, CSRs, `/readyz`, `/version`, kube-system pod list, Secret **create** | nodes/pods/events `get,list,watch` + scoped Secret `get` |

The name split is deliberate and load-bearing: the two RBAC sets are
**different**, so reusing the `kubehz-agent*` names would overwrite the
CronJob's ClusterRole/Role and break its component-health heartbeat. Applying
this kustomization never touches the CronJob agent; it may **coexist with or
replace** it (replacing = also removing the lok8s CronJob manifests — the live
agent does not report `components[]`/certificates yet, so keep the CronJob
until it does).

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

The Deployment pins the image **by digest** (multi-arch index). The committed
default is the kubehz-internal Harbor mirror
(`docker.kubehz.in.net/kubehz-builds/kubehz-agent@sha256:…`) — the ref the
pilot dogfoods. The canonical public image is
`ghcr.io/kernpilot/kubehz-agent` (same digest, cosign-signed keyless with an
attested SBOM); anonymous pulls require the GHCR package to be public.

Retarget or repin via the kustomize `images:` override (commented example in
`kustomization.yaml`) rather than editing `deployment.yaml`. Never deploy a
moving tag (`main`, `latest`): a digest is the only ref cosign verification and
rollback can reason about.

```bash
# Verify what you are about to run (keyless, GitHub Actions OIDC):
cosign verify \
  --certificate-identity-regexp 'https://github.com/kernpilot/kubehz-agent/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/kernpilot/kubehz-agent@sha256:<digest>
```

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
