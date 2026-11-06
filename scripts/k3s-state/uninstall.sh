#!/usr/bin/env bash
# kubehz: remove k3s with managed state from this machine.
#
# The install script (scripts/k3s-state/v1.sh) writes this file to
# /usr/local/bin/kubehz-k3s-uninstall.sh. The dashboard shows the command:
#
#   sudo kubehz-k3s-uninstall.sh
#
# What it does, in order:
#   1. Stops the certificate renewal timer and service and removes them.
#   2. On a server: removes the in-cluster kubehz-agent and its
#      registration (Secret kubehz-system/kubehz-agent) that the install
#      script applied. It does this only when the apiserver answers and no
#      other server is in the cluster: the other servers keep their agent.
#   3. Removes /etc/kubehz/state (the datastore client keys and
#      certificates, the k3s token) and the kubehz-agent binary.
#   4. Runs k3s-uninstall.sh on a server, or k3s-agent-uninstall.sh on an
#      agent node.
#   5. Removes this file.
#
# Each step skips what is not there. After an interruption, run the script
# again: it continues, and a run after a complete uninstall changes nothing.
#
# The cluster's state on the kubehz platform stays. Delete the cluster in
# the dashboard to remove it.

# Refuse other shells first, in POSIX syntax.
if [ -z "${BASH_VERSION:-}" ]; then
	echo "error: run this script with bash (sudo bash $0)" >&2
	exit 1
fi

set -Eeuo pipefail

log() { printf '==> %s\n' "$*" >&2; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

# Tests set KUBEHZ_UNINSTALL_ROOT to a folder with a fake machine: all the
# paths below are in that folder, and the script does not need root.
ROOT="${KUBEHZ_UNINSTALL_ROOT:-}"
if [[ -n "${ROOT}" && ("${ROOT}" != /* || "${ROOT}" == "/" || ! -d "${ROOT}") ]]; then
	printf 'error: KUBEHZ_UNINSTALL_ROOT %s is not an absolute folder other than /\n' "${ROOT}" >&2
	exit 2
fi
readonly ROOT

readonly BIN_DIR="${ROOT}/usr/local/bin"
readonly KUBEHZ_DIR="${ROOT}/etc/kubehz"
readonly STATE_DIR="${KUBEHZ_DIR}/state"
readonly AGENT_MANIFEST="${KUBEHZ_DIR}/kubehz-agent.yaml"
readonly AGENT_BIN="${BIN_DIR}/kubehz-agent"
readonly SELF="${BIN_DIR}/kubehz-k3s-uninstall.sh"
readonly UNIT_DIR="${ROOT}/etc/systemd/system"
# The timer first: a timer that runs can start the service again.
readonly -a UNITS=(kubehz-state-renew.timer kubehz-state-renew.service)
readonly K3S_BIN="${BIN_DIR}/k3s"
readonly K3S_KUBECONFIG="${ROOT}/etc/rancher/k3s/k3s.yaml"
readonly SERVER_UNINSTALL="${BIN_DIR}/k3s-uninstall.sh"
readonly AGENT_UNINSTALL="${BIN_DIR}/k3s-agent-uninstall.sh"

usage() {
	cat <<'EOF'
Usage: sudo kubehz-k3s-uninstall.sh

Removes k3s with managed state from this machine: the certificate renewal
timer, /etc/kubehz/state, the kubehz-agent (on the last server), and k3s.
The cluster's state on the kubehz platform stays.
EOF
}

kubectl_k3s() {
	"${K3S_BIN}" kubectl --kubeconfig "${K3S_KUBECONFIG}" --request-timeout=30s "$@"
}

stop_renewal() {
	local unit removed=0
	for unit in "${UNITS[@]}"; do
		systemctl disable --now "${unit}" >/dev/null 2>&1 || systemctl stop "${unit}" >/dev/null 2>&1 || true
		if [[ -e "${UNIT_DIR}/${unit}" ]]; then
			log "Removing ${unit}"
			rm -f -- "${UNIT_DIR}/${unit}"
			removed=1
		fi
	done
	if ((removed)); then
		systemctl daemon-reload || warn "systemctl daemon-reload failed"
	fi
	systemctl reset-failed "${UNITS[@]}" >/dev/null 2>&1 || true
}

# remove_agent_from_cluster deletes what the install script applied: the
# saved agent manifest, the namespace (with the registration Secret), and
# the admission policy that the agent writes for k3s upgrades.
remove_agent_from_cluster() {
	if [[ ! -f "${AGENT_MANIFEST}" ]]; then
		return 0
	fi
	if [[ ! -x "${SERVER_UNINSTALL}" || ! -x "${K3S_BIN}" || ! -f "${K3S_KUBECONFIG}" ]]; then
		return 0
	fi
	if ! kubectl_k3s get --raw /readyz >/dev/null 2>&1; then
		warn "the apiserver does not answer: the in-cluster kubehz-agent stays in the cluster's state"
		return 0
	fi
	local self nodes node
	local -a others=()
	self="$(uname -n | tr '[:upper:]' '[:lower:]')"
	if ! nodes="$(kubectl_k3s get nodes -l node-role.kubernetes.io/control-plane -o name)"; then
		warn "the list of servers is not available: the in-cluster kubehz-agent stays in the cluster's state"
		return 0
	fi
	while IFS= read -r node; do
		if [[ -n "${node}" && "${node}" != "node/${self}" ]]; then
			others+=("${node#node/}")
		fi
	done <<<"${nodes}"
	if ((${#others[@]} > 0)); then
		log "Other servers are in the cluster (${others[*]}): the in-cluster kubehz-agent stays"
		return 0
	fi
	log "Removing the in-cluster kubehz-agent and its registration"
	kubectl_k3s delete -f "${AGENT_MANIFEST}" --ignore-not-found --wait=false >/dev/null ||
		warn "the delete of the agent objects failed"
	kubectl_k3s delete validatingadmissionpolicybinding,validatingadmissionpolicy kubehz-k3s-upgrade-plans \
		--ignore-not-found --wait=false >/dev/null || warn "the delete of the agent's admission policy failed"
	kubectl_k3s delete namespace kubehz-system --ignore-not-found --wait=false >/dev/null ||
		warn "the delete of namespace kubehz-system failed"
}

remove_host_files() {
	if [[ -e "${STATE_DIR}" || -L "${STATE_DIR}" ]]; then
		log "Removing ${STATE_DIR#"${ROOT}"}"
		rm -rf -- "${STATE_DIR}"
	fi
	rm -f -- "${AGENT_MANIFEST}" "${AGENT_BIN}"
	rmdir -- "${KUBEHZ_DIR}" 2>/dev/null || true
}

uninstall_k3s() {
	local uninstaller=""
	if [[ -x "${SERVER_UNINSTALL}" ]]; then
		uninstaller="${SERVER_UNINSTALL}"
	elif [[ -x "${AGENT_UNINSTALL}" ]]; then
		uninstaller="${AGENT_UNINSTALL}"
	else
		log "k3s is not installed (no k3s-uninstall.sh or k3s-agent-uninstall.sh)"
		return 0
	fi
	log "Running ${uninstaller#"${ROOT}"}"
	"${uninstaller}" || die "${uninstaller#"${ROOT}"} failed (see the message above); fix the cause, then run this script again"
}

main() {
	case "${1:-}" in
	"") ;;
	-h | --help)
		usage
		return 0
		;;
	*)
		printf 'error: unknown argument %s; this script takes no arguments (see --help)\n' "$1" >&2
		exit 2
		;;
	esac
	if [[ -z "${ROOT}" && "${EUID}" -ne 0 ]]; then
		die "run this script as root (sudo $0)"
	fi
	stop_renewal
	remove_agent_from_cluster
	remove_host_files
	uninstall_k3s
	# Last: after a failure above, the script stays for the next run.
	rm -f -- "${SELF}"
	log "Done: k3s with managed state is removed from this machine. The cluster's state on the kubehz platform stays; delete the cluster in the dashboard to remove it."
}

main "$@"
