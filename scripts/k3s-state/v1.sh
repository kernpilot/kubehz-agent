#!/usr/bin/env bash
# kubehz: k3s with managed state, install script v1.
#
# This script installs a k3s SERVER whose datastore is a managed kine on
# the kubehz platform. The dashboard shows the command that downloads this
# file, checks its sha256, and then runs it:
#
#   curl -sfLo kubehz-k3s.sh <url> && echo "<sha256>  kubehz-k3s.sh" | sha256sum -c && sudo bash kubehz-k3s.sh
#
# What it does, in order:
#   1. Checks the machine (root, bash, Linux, systemd; no old k3s etcd data).
#   2. Installs /usr/local/bin/kubehz-k3s-uninstall.sh (source:
#      scripts/k3s-state/uninstall.sh), which removes all of this again.
#   3. Installs the kubehz-agent binary (pinned, sha256 checked).
#   4. Asks for the install ticket at a hidden prompt and redeems it with
#      "kubehz-agent state-redeem": the client key is made on this machine
#      and never leaves it.
#   5. Installs k3s (pinned, sha256 checked) with the managed datastore, no
#      flannel, no network policy controller, no traefik, no servicelb.
#   6. Installs Cilium (pinned chart, sha256 checked) through the k3s Helm
#      controller, and the static SSO role bindings.
#   7. Recovery (--recovery): removes the old Node object and its node
#      password Secret, then restarts k3s.
#   8. Installs the in-cluster kubehz-agent and its registration, and keeps
#      the applied manifest for the uninstall.
#   9. Installs the certificate renewal timer (kubehz-agent state-renew,
#      every 5 minutes).
#
# The script is static: the same file for every cluster. It never pipes a
# download into a shell, and every download is checked against a pinned
# sha256 before use.

# Refuse other shells first, in POSIX syntax ("read -s" is not POSIX).
if [ -z "${BASH_VERSION:-}" ]; then
	echo "error: run this script with bash (sudo bash $0)" >&2
	exit 1
fi

set -Eeuo pipefail
umask 077

# ── Pins ─────────────────────────────────────────────────────────────────
# The release workflow replaces the @...@ values (scripts/k3s-state/render.sh).
readonly AGENT_VERSION="@KUBEHZ_AGENT_VERSION@"
readonly AGENT_SHA256_AMD64="@KUBEHZ_AGENT_SHA256_AMD64@"
readonly AGENT_SHA256_ARM64="@KUBEHZ_AGENT_SHA256_ARM64@"
readonly INSTALL_YAML_SHA256="@KUBEHZ_INSTALL_YAML_SHA256@"
readonly INSTALL_MAINTENANCE_YAML_SHA256="@KUBEHZ_INSTALL_MAINTENANCE_YAML_SHA256@"
readonly AGENT_RELEASES="https://github.com/kernpilot/kubehz-agent/releases/download"

# k3s releases this script can install: "<version> <sha256 amd64> <sha256 arm64>",
# from the sha256sum-<arch>.txt files of each k3s-io/k3s release. A new
# cluster gets K3S_DEFAULT; a recovery gets the version the platform stored.
readonly K3S_DEFAULT="v1.37.2+k3s1"
readonly -a K3S_PINS=(
	"v1.36.6+k3s1 66ceb8e67edfa547195135d74783bfadc2ecc04a91059e90ae5f70a3dbc98c15 49dcecdb4f64f5e92bdc52d204d407fb39729e492f09074488125ee58b6f2895"
	"v1.37.2+k3s1 265b3ee6a3198ef14033c63ef46cbb32370d83795199fb6d1b01f21575eddbda a779ccf1e5e2e33a62f5a29c2f1c0bbf99431e100579d4c10f0aa0742a6aa84b"
)
# The official k3s install script of tag v1.37.2+k3s1. With
# INSTALL_K3S_SKIP_DOWNLOAD=true it downloads nothing; it writes the systemd
# unit and starts k3s.
readonly K3S_INSTALLER_REF="v1.37.2%2Bk3s1"
readonly K3S_INSTALLER_SHA256="ed01f89fd977bf20ac1516bbebf8370bf3ddbaa55dac8aba610956a4c78cc00b"

# The Cilium Helm chart (helm.cilium.io index digest of cilium-1.20.4.tgz).
readonly CILIUM_VERSION="1.20.4"
readonly CILIUM_CHART_SHA256="505abff8385811df1a949776fc42e9f10697989a332acc86d6e8933efcb855ed"

# ── Paths ────────────────────────────────────────────────────────────────
readonly STATE_DIR="/etc/kubehz/state"
readonly AGENT_MANIFEST="/etc/kubehz/kubehz-agent.yaml"
readonly AGENT_BIN="/usr/local/bin/kubehz-agent"
readonly UNINSTALL_BIN="/usr/local/bin/kubehz-k3s-uninstall.sh"
readonly K3S_BIN="/usr/local/bin/k3s"
readonly K3S_CONFIG="/etc/rancher/k3s/config.yaml"
readonly K3S_KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
readonly K3S_SERVER_DIR="/var/lib/rancher/k3s/server"
readonly K3S_MANIFESTS="${K3S_SERVER_DIR}/manifests"
readonly K3S_CHARTS="${K3S_SERVER_DIR}/static/charts"

# ── Options ──────────────────────────────────────────────────────────────
API_URL="https://api.kubehz.cloud"
LABEL=""
RECOVERY=0
MAINTENANCE=0

WORK=""
declare -A CFG=()

log() { printf '==> %s\n' "$*" >&2; }
die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

usage() {
	cat <<'EOF'
Usage: sudo bash kubehz-k3s.sh [options]

Installs a k3s server with a managed datastore on the kubehz platform.
The script asks for the install ticket from the dashboard.

Options:
  --recovery         Install a replacement for a lost server. Use a recovery
                     ticket. The old server must stay off.
  --maintenance      Also install the k3s upgrade path (the system-upgrade-
                     controller with zero replicas and the agent's rights to
                     run upgrades that you start in the dashboard).
  --label NAME       Machine label in the dashboard (default: the host name).
  --api-url URL      kubehz api (default: https://api.kubehz.cloud).
  -h, --help         Show this text.
EOF
}

parse_args() {
	while (($# > 0)); do
		case "$1" in
		--recovery) RECOVERY=1 ;;
		--maintenance) MAINTENANCE=1 ;;
		--label)
			(($# > 1)) || die "--label needs a value"
			LABEL="$2"
			shift
			;;
		--api-url)
			(($# > 1)) || die "--api-url needs a value"
			API_URL="$2"
			shift
			;;
		-h | --help)
			usage
			exit 0
			;;
		*) die "unknown option '$1'; run with --help for the options" ;;
		esac
		shift
	done
	[[ "${API_URL}" =~ ^https://[A-Za-z0-9.-]+(:[0-9]{1,5})?$ ]] ||
		die "api URL '${API_URL}' is not https://<host>[:port]"
	if [[ -z "${LABEL}" ]]; then
		LABEL="$(uname -n | cut -d. -f1 | tr '[:upper:]' '[:lower:]')"
	fi
	[[ "${LABEL}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
		die "machine label '${LABEL}' is not a DNS label; set one with --label"
}

cleanup() {
	if [[ -n "${WORK}" && -d "${WORK}" ]]; then
		rm -rf -- "${WORK}"
	fi
}

on_error() {
	local line="$1"
	printf 'error: the install stopped at line %s. Fix the cause above, then run the script again: it continues where it stopped.\n' "${line}" >&2
}

# download URL DEST SHA256: fetch over https only and check the sha256
# before anything uses the file.
download() {
	local url="$1" dest="$2" sum="$3"
	curl --proto '=https' --tlsv1.2 -fsSL --retry 3 --retry-delay 2 -o "${dest}" "${url}" ||
		die "download of ${url} failed; check the network of this machine, then run the script again"
	if ! printf '%s  %s\n' "${sum}" "${dest}" | sha256sum -c --quiet - >/dev/null 2>&1; then
		rm -f -- "${dest}"
		die "the sha256 of ${url} is not ${sum}; the file was not used"
	fi
}

check_machine() {
	[[ "${EUID}" -eq 0 ]] || die "run this script as root (sudo bash $0)"
	[[ "$(uname -s)" == "Linux" ]] || die "this script installs on Linux only"
	command -v systemctl >/dev/null || die "systemd is necessary (systemctl not found)"
	local tool
	for tool in curl sha256sum install sed cut tr; do
		command -v "${tool}" >/dev/null || die "${tool} is necessary; install it, then run the script again"
	done
	case "$(uname -m)" in
	x86_64 | amd64) ARCH="amd64" ;;
	aarch64 | arm64) ARCH="arm64" ;;
	*) die "machine architecture $(uname -m) is not supported (amd64 and arm64 only)" ;;
	esac
	readonly ARCH
	if [[ -e "${K3S_SERVER_DIR}/db/etcd" ]]; then
		die "this machine has embedded etcd data of an earlier k3s (${K3S_SERVER_DIR}/db/etcd); k3s would use it instead of the managed datastore. Use a clean machine, or uninstall k3s and remove ${K3S_SERVER_DIR} first"
	fi
	if [[ "${AGENT_VERSION}" == @* || "${K3S_INSTALLER_SHA256}" == @* ]]; then
		die "this is the unrendered script from the source tree; download the script of a kubehz-agent release"
	fi
}

# install_uninstall_script writes kubehz-k3s-uninstall.sh. The release
# workflow (scripts/k3s-state/render.sh) puts the content of
# scripts/k3s-state/uninstall.sh in place of the line below.
install_uninstall_script() {
	cat >"${WORK}/uninstall.sh" <<'KUBEHZ_UNINSTALL_EOF'
@KUBEHZ_UNINSTALL_SCRIPT@
KUBEHZ_UNINSTALL_EOF
	install -m 0700 -o root -g root "${WORK}/uninstall.sh" "${UNINSTALL_BIN}"
}

install_agent_binary() {
	local sum="${AGENT_SHA256_AMD64}"
	[[ "${ARCH}" == "arm64" ]] && sum="${AGENT_SHA256_ARM64}"
	log "Installing kubehz-agent ${AGENT_VERSION}"
	download "${AGENT_RELEASES}/${AGENT_VERSION}/kubehz-agent-linux-${ARCH}" "${WORK}/kubehz-agent" "${sum}"
	install -m 0755 -o root -g root "${WORK}/kubehz-agent" "${AGENT_BIN}"
}

redeem() {
	if [[ -L "${STATE_DIR}/current" && -f "${STATE_DIR}/install.env" ]]; then
		log "The ticket was redeemed before; continuing the install"
		return
	fi
	local ticket=""
	if [[ -t 0 ]]; then
		read -rs -p "Install ticket from the dashboard: " ticket
		printf '\n' >&2
	else
		read -r ticket || true
	fi
	[[ -n "${ticket}" ]] || die "no ticket; copy the ticket from the dashboard and run the script again"
	log "Redeeming the ticket"
	# printf is a bash builtin: the ticket is in no argument list and no
	# environment of another process.
	local args=(state-redeem --dir "${STATE_DIR}" --api-url "${API_URL}" --label "${LABEL}" --role server)
	if ! printf '%s\n' "${ticket}" | "${AGENT_BIN}" "${args[@]}"; then
		ticket=""
		die "the ticket was not redeemed (see the message above)"
	fi
	ticket=""
}

# read_install_env reads install.env with a parser. It never sources the
# file, and it checks each value again.
read_install_env() {
	local key value
	while IFS='=' read -r key value; do
		case "${key}" in
		CLUSTER_ID | MACHINE_ID | STATE_ENDPOINT | K3S_VERSION | CLUSTER_CIDR | SERVICE_CIDR | TLS_SANS | HAS_AUTHN_CONFIG | HAS_SSO_BINDINGS | HAS_AGENT_TOKEN)
			CFG["${key}"]="${value}"
			;;
		"") ;;
		*) die "install.env has the unknown key '${key}'" ;;
		esac
	done <"${STATE_DIR}/install.env"

	[[ "${CFG[CLUSTER_ID]:-}" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$ ]] || die "install.env: cluster id is not valid"
	[[ "${CFG[STATE_ENDPOINT]:-}" =~ ^https://[a-z0-9.-]+:[0-9]{1,5}/?$ ]] || die "install.env: datastore endpoint is not valid"
	[[ -n "${CFG[K3S_VERSION]:-}" ]] || CFG[K3S_VERSION]="${K3S_DEFAULT}"
	[[ "${CFG[K3S_VERSION]}" =~ ^v1\.[0-9]{1,3}\.[0-9]{1,3}\+k3s[0-9]{1,3}$ ]] || die "install.env: k3s version is not valid"
	[[ -n "${CFG[CLUSTER_CIDR]:-}" ]] || CFG[CLUSTER_CIDR]="10.42.0.0/16"
	[[ -n "${CFG[SERVICE_CIDR]:-}" ]] || CFG[SERVICE_CIDR]="10.43.0.0/16"
	local cidr
	for cidr in "${CFG[CLUSTER_CIDR]}" "${CFG[SERVICE_CIDR]}"; do
		[[ "${cidr}" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/[0-9]{1,2}$ ]] || die "install.env: CIDR '${cidr}' is not an IPv4 CIDR"
	done
	[[ "${CFG[TLS_SANS]:-}" =~ ^[A-Za-z0-9.,:-]*$ ]] || die "install.env: TLS SANs are not valid"
}

k3s_sha256() {
	local version="$1" pin v amd64 arm64
	for pin in "${K3S_PINS[@]}"; do
		read -r v amd64 arm64 <<<"${pin}"
		if [[ "${v}" == "${version}" ]]; then
			if [[ "${ARCH}" == "amd64" ]]; then printf '%s' "${amd64}"; else printf '%s' "${arm64}"; fi
			return 0
		fi
	done
	return 1
}

write_k3s_config() {
	local endpoint="${CFG[STATE_ENDPOINT]%/}"
	install -d -m 0755 /etc/rancher/k3s
	{
		echo "# Written by the kubehz install script ${AGENT_VERSION}. The datastore files are"
		echo "# read through the symlink ${STATE_DIR}/current, which the renewal timer switches."
		echo "token-file: ${STATE_DIR}/k3s-token"
		echo "datastore-endpoint: ${endpoint}"
		echo "datastore-cafile: ${STATE_DIR}/current/server-ca.crt"
		echo "datastore-certfile: ${STATE_DIR}/current/client.crt"
		echo "datastore-keyfile: ${STATE_DIR}/current/client.key"
		echo "flannel-backend: none"
		echo "disable-network-policy: true"
		echo "disable:"
		echo "  - traefik"
		echo "  - servicelb"
		echo "cluster-cidr: ${CFG[CLUSTER_CIDR]}"
		echo "service-cidr: ${CFG[SERVICE_CIDR]}"
		echo "write-kubeconfig-mode: \"0600\""
		if [[ -n "${CFG[TLS_SANS]}" ]]; then
			local san
			local -a sans
			IFS=',' read -r -a sans <<<"${CFG[TLS_SANS]}"
			echo "tls-san:"
			for san in "${sans[@]}"; do
				echo "  - \"${san}\""
			done
		fi
		if [[ "${CFG[HAS_AUTHN_CONFIG]:-0}" == "1" ]]; then
			echo "kube-apiserver-arg:"
			echo "  - authentication-config=${STATE_DIR}/authentication-config.yaml"
		fi
	} >"${K3S_CONFIG}.tmp"
	chmod 0600 "${K3S_CONFIG}.tmp"
	mv -f -- "${K3S_CONFIG}.tmp" "${K3S_CONFIG}"
}

write_cilium_chart() {
	log "Preparing Cilium ${CILIUM_VERSION}"
	install -d -m 0700 "${K3S_CHARTS}" "${K3S_MANIFESTS}"
	download "https://helm.cilium.io/cilium-${CILIUM_VERSION}.tgz" "${WORK}/cilium.tgz" "${CILIUM_CHART_SHA256}"
	install -m 0644 "${WORK}/cilium.tgz" "${K3S_CHARTS}/cilium-${CILIUM_VERSION}.tgz"
	# The k3s Helm controller installs the chart from the server's own static
	# file path. bootstrap: the chart runs before the cluster has a CNI.
	cat >"${K3S_MANIFESTS}/kubehz-cilium.yaml" <<EOF
# Written by the kubehz install script ${AGENT_VERSION}.
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: cilium
  namespace: kube-system
spec:
  bootstrap: true
  chart: https://%{KUBERNETES_API}%/static/charts/cilium-${CILIUM_VERSION}.tgz
  targetNamespace: kube-system
  valuesContent: |-
    ipam:
      mode: cluster-pool
      operator:
        clusterPoolIPv4PodCIDRList:
          - "${CFG[CLUSTER_CIDR]}"
    operator:
      replicas: 1
    kubeProxyReplacement: false
EOF
	chmod 0600 "${K3S_MANIFESTS}/kubehz-cilium.yaml"
	if [[ "${CFG[HAS_SSO_BINDINGS]:-0}" == "1" ]]; then
		install -m 0600 "${STATE_DIR}/sso-rbac.yaml" "${K3S_MANIFESTS}/kubehz-sso-rbac.yaml"
	fi
}

install_k3s() {
	local version="${CFG[K3S_VERSION]}" sum asset="k3s"
	sum="$(k3s_sha256 "${version}")" ||
		die "this script does not pin k3s ${version}; run the install script version that the dashboard shows"
	[[ "${ARCH}" == "arm64" ]] && asset="k3s-arm64"
	log "Installing k3s ${version}"
	download "https://github.com/k3s-io/k3s/releases/download/${version/+/%2B}/${asset}" "${WORK}/k3s" "${sum}"
	download "https://raw.githubusercontent.com/k3s-io/k3s/${K3S_INSTALLER_REF}/install.sh" "${WORK}/k3s-install.sh" "${K3S_INSTALLER_SHA256}"
	install -m 0755 -o root -g root "${WORK}/k3s" "${K3S_BIN}"
	write_k3s_config
	write_cilium_chart
	# The installer reads the flags from ${K3S_CONFIG}. It does not start
	# k3s: a start that waits for an unreachable datastore never ends, so
	# the script starts k3s without a wait and waits with a time limit.
	INSTALL_K3S_SKIP_DOWNLOAD=true INSTALL_K3S_SKIP_START=true INSTALL_K3S_EXEC="server" sh "${WORK}/k3s-install.sh"
	systemctl restart --no-block k3s
	wait_apiserver
}

wait_apiserver() {
	local i
	for ((i = 0; i < 60; i++)); do
		if "${K3S_BIN}" kubectl --kubeconfig "${K3S_KUBECONFIG}" get --raw /readyz >/dev/null 2>&1; then
			return 0
		fi
		sleep 5
	done
	die "the k3s apiserver did not become ready in 5 minutes; see 'journalctl -u k3s' (a datastore error names the cause), then run the script again"
}

kubectl_k3s() {
	"${K3S_BIN}" kubectl --kubeconfig "${K3S_KUBECONFIG}" "$@"
}

# recover_node removes the Node object and the node password Secret of the
# lost server (same host name), so that this machine registers as that
# node. It runs once per machine.
recover_node() {
	local node marker="${STATE_DIR}/recovery-done"
	if [[ -f "${marker}" ]]; then
		return
	fi
	node="$(uname -n | tr '[:upper:]' '[:lower:]')"
	log "Recovery: removing the old Node ${node} and its node password"
	kubectl_k3s delete node "${node}" --ignore-not-found --wait=false
	kubectl_k3s -n kube-system delete secret "${node}.node-password.k3s" --ignore-not-found
	: >"${marker}"
	systemctl restart --no-block k3s
	sleep 5
	wait_apiserver
}

install_agent() {
	local manifest="install.yaml" sum="${INSTALL_YAML_SHA256}"
	if ((MAINTENANCE)); then
		manifest="install-maintenance.yaml"
		sum="${INSTALL_MAINTENANCE_YAML_SHA256}"
	fi
	log "Installing the in-cluster kubehz-agent (${manifest})"
	wait_apiserver
	download "${AGENT_RELEASES}/${AGENT_VERSION}/${manifest}" "${WORK}/agent.yaml" "${sum}"
	sed -i \
		-e "s|KUBEHZ_API_URL_PLACEHOLDER|${API_URL}|g" \
		-e "s|CLUSTER_ID_PLACEHOLDER|${CFG[CLUSTER_ID]}|g" \
		"${WORK}/agent.yaml"
	# The uninstall deletes what this manifest applied.
	install -d -m 0700 "$(dirname "${AGENT_MANIFEST}")"
	install -m 0600 "${WORK}/agent.yaml" "${AGENT_MANIFEST}"
	kubectl_k3s create namespace kubehz-system --dry-run=client -o yaml | kubectl_k3s apply -f - >/dev/null
	if [[ "${CFG[HAS_AGENT_TOKEN]:-0}" == "1" && -f "${STATE_DIR}/agent-token" ]]; then
		kubectl_k3s -n kubehz-system create secret generic kubehz-agent \
			--from-file=agent-token="${STATE_DIR}/agent-token" --dry-run=client -o yaml |
			kubectl_k3s apply -f - >/dev/null
		rm -f -- "${STATE_DIR}/agent-token"
	elif ! kubectl_k3s -n kubehz-system get secret kubehz-agent >/dev/null 2>&1; then
		log "No agent registration came with the ticket: the agent waits for Secret kubehz-system/kubehz-agent"
	fi
	kubectl_k3s apply -f "${WORK}/agent.yaml" >/dev/null
}

install_renew_timer() {
	log "Installing the certificate renewal timer"
	cat >/etc/systemd/system/kubehz-state-renew.service <<EOF
[Unit]
Description=kubehz: renew the datastore client certificate of k3s
After=network-online.target k3s.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=${AGENT_BIN} state-renew --dir ${STATE_DIR}
EOF
	cat >/etc/systemd/system/kubehz-state-renew.timer <<'EOF'
[Unit]
Description=kubehz: renew the datastore client certificate of k3s every 5 minutes

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
RandomizedDelaySec=30s

[Install]
WantedBy=timers.target
EOF
	chmod 0644 /etc/systemd/system/kubehz-state-renew.service /etc/systemd/system/kubehz-state-renew.timer
	systemctl daemon-reload
	systemctl enable --now kubehz-state-renew.timer
}

main() {
	parse_args "$@"
	check_machine
	WORK="$(mktemp -d)"
	trap cleanup EXIT
	trap 'on_error ${LINENO}' ERR

	install_uninstall_script
	install_agent_binary
	redeem
	read_install_env
	install_k3s
	if ((RECOVERY)); then
		recover_node
	fi
	install_agent
	install_renew_timer

	log "Done: cluster ${CFG[CLUSTER_ID]}, k3s ${CFG[K3S_VERSION]}"
	log "Admin kubeconfig: ${K3S_KUBECONFIG} (root only)"
	log "Uninstall: sudo ${UNINSTALL_BIN}"
	if ((!MAINTENANCE)); then
		log "k3s upgrades from the dashboard need the maintenance path: run the script again with --maintenance"
	fi
}

main "$@"
