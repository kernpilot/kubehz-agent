#!/usr/bin/env bash
# Tests of the managed-state scripts:
#   - uninstall.sh on a fake machine (KUBEHZ_UNINSTALL_ROOT), with stubs for
#     systemctl, k3s and the k3s uninstall scripts;
#   - render.sh with fake release assets.
#
#   bash scripts/k3s-state/test.sh
#
# The tests do not need root and change nothing outside a temporary folder.
set -Eeuo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf -- "${tmp}"' EXIT

failures=0
current=""
fail() {
	printf 'FAIL %s: %s\n' "${current}" "$*" >&2
	failures=$((failures + 1))
}

self_node="$(uname -n | tr '[:upper:]' '[:lower:]')"
stubs="${tmp}/stubs"
mkdir -p "${stubs}"
log="${tmp}/calls.log"

# systemctl logs its arguments.
cat >"${stubs}/systemctl" <<EOF
#!/usr/bin/env bash
printf 'systemctl %s\n' "\$*" >>"${log}"
EOF
chmod +x "${stubs}/systemctl"

# machine ROLE: a fresh fake machine of that role ("server" or "agent").
# Writes the root folder to ${root}.
machine() {
	local role="$1"
	root="$(mktemp -d "${tmp}/machine.XXXXXX")"
	: >"${log}"
	printf '0\n' >"${tmp}/ready"
	printf 'node/%s\n' "${self_node}" >"${tmp}/nodes"
	printf '0\n' >"${tmp}/uninstall-rc"

	mkdir -p "${root}/etc/kubehz/state/gen-1" "${root}/etc/systemd/system" "${root}/usr/local/bin" "${root}/etc/rancher/k3s"
	echo key >"${root}/etc/kubehz/state/gen-1/client.key"
	ln -s gen-1 "${root}/etc/kubehz/state/current"
	echo "CLUSTER_ID=c1" >"${root}/etc/kubehz/state/install.env"
	echo "kind: List" >"${root}/etc/kubehz/kubehz-agent.yaml"
	echo "[Unit]" >"${root}/etc/systemd/system/kubehz-state-renew.service"
	echo "[Unit]" >"${root}/etc/systemd/system/kubehz-state-renew.timer"
	echo "binary" >"${root}/usr/local/bin/kubehz-agent"
	install -m 0700 "${here}/uninstall.sh" "${root}/usr/local/bin/kubehz-k3s-uninstall.sh"

	# k3s logs its arguments; "get --raw /readyz" and "get nodes" answer
	# from the files ready and nodes.
	cat >"${root}/usr/local/bin/k3s" <<EOF
#!/usr/bin/env bash
printf 'k3s %s\n' "\$*" >>"${log}"
case " \$* " in
*" get --raw /readyz "*) exit "\$(cat "${tmp}/ready")" ;;
*" get nodes "*) cat "${tmp}/nodes" ;;
esac
EOF
	chmod +x "${root}/usr/local/bin/k3s"

	local uninstaller="k3s-uninstall.sh"
	if [[ "${role}" == "server" ]]; then
		echo "apiVersion: v1" >"${root}/etc/rancher/k3s/k3s.yaml"
	else
		uninstaller="k3s-agent-uninstall.sh"
	fi
	# The k3s uninstaller logs whether /etc/kubehz/state is still there,
	# removes k3s and itself, and exits with the code in uninstall-rc.
	cat >"${root}/usr/local/bin/${uninstaller}" <<EOF
#!/usr/bin/env bash
state=absent
[[ -e "${root}/etc/kubehz/state" ]] && state=present
printf '%s state=%s\n' "${uninstaller}" "\${state}" >>"${log}"
rc="\$(cat "${tmp}/uninstall-rc")"
[[ "\${rc}" == 0 ]] || exit "\${rc}"
rm -rf -- "${root}/usr/local/bin/k3s" "${root}/etc/rancher/k3s" "${root}/usr/local/bin/${uninstaller}"
EOF
	chmod +x "${root}/usr/local/bin/${uninstaller}"
}

# run SCRIPT: runs the uninstall script on ${root} with a clean environment.
# Sets ${rc} and ${out}.
run() {
	local script="${1:-${root}/usr/local/bin/kubehz-k3s-uninstall.sh}"
	rc=0
	out="$(env -i PATH="${stubs}:/usr/bin:/bin" HOME="${tmp}" KUBEHZ_UNINSTALL_ROOT="${root}" bash "${script}" 2>&1)" || rc=$?
}

# line PATTERN: the number of the first log line that matches, or 0.
line() {
	local n
	n="$(grep -n -m1 -e "$1" "${log}" | cut -d: -f1)" || true
	printf '%s' "${n:-0}"
}

assert_removed() {
	local p
	for p in etc/kubehz usr/local/bin/kubehz-agent usr/local/bin/kubehz-k3s-uninstall.sh \
		etc/systemd/system/kubehz-state-renew.service etc/systemd/system/kubehz-state-renew.timer; do
		if [[ -e "${root}/${p}" || -L "${root}/${p}" ]]; then
			fail "/${p} is still there"
		fi
	done
}

# ── uninstall.sh ─────────────────────────────────────────────────────────

current="the only server: everything goes, in order"
machine server
run
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
assert_removed
timer="$(line '^systemctl disable --now kubehz-state-renew.timer$')"
service="$(line '^systemctl .*kubehz-state-renew.service')"
del="$(line "^k3s kubectl .* delete -f ${root}/etc/kubehz/kubehz-agent.yaml ")"
ns="$(line '^k3s kubectl .* delete namespace kubehz-system ')"
vap="$(line '^k3s kubectl .* delete validatingadmissionpolicybinding,validatingadmissionpolicy kubehz-k3s-upgrade-plans ')"
k3s="$(line '^k3s-uninstall.sh state=absent$')"
((timer > 0 && service > timer)) || fail "the timer is not stopped before the service (timer line ${timer}, service line ${service})"
((del > timer)) || fail "the agent manifest is not deleted after the timer stopped (line ${del})"
((ns > 0 && vap > 0)) || fail "the namespace or the admission policy is not deleted"
((k3s > del)) || fail "k3s-uninstall.sh did not run after the cluster cleanup with /etc/kubehz/state removed (line ${k3s})"

current="a second run changes nothing"
: >"${log}"
run "${here}/uninstall.sh"
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q 'delete' "${log}" && fail "the second run deleted objects: $(cat "${log}")"
grep -q 'uninstall.sh state' "${log}" && fail "the second run ran a k3s uninstaller"
[[ -f "${here}/uninstall.sh" ]] || fail "the script removed the file it ran from, not the installed copy"

current="another server: the in-cluster agent stays"
machine server
printf 'node/%s\nnode/other-server\n' "${self_node}" >"${tmp}/nodes"
run
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q ' delete ' "${log}" && fail "objects were deleted: $(grep ' delete ' "${log}")"
grep -q 'other-server' <<<"${out}" || fail "the output does not name the other server: ${out}"
assert_removed
(($(line '^k3s-uninstall.sh state=absent$') > 0)) || fail "k3s-uninstall.sh did not run"

current="the apiserver does not answer: the uninstall continues"
machine server
printf '1\n' >"${tmp}/ready"
run
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q 'get nodes\| delete ' "${log}" && fail "kubectl ran after the readiness check failed"
assert_removed
(($(line '^k3s-uninstall.sh state=absent$') > 0)) || fail "k3s-uninstall.sh did not run"

current="an agent node: k3s-agent-uninstall.sh, no kubectl"
machine agent
run
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q '^k3s ' "${log}" && fail "kubectl ran on an agent node: $(grep '^k3s ' "${log}")"
(($(line '^k3s-agent-uninstall.sh state=absent$') > 0)) || fail "k3s-agent-uninstall.sh did not run"
assert_removed

current="the k3s uninstaller fails: the script stays for the next run"
machine server
printf '3\n' >"${tmp}/uninstall-rc"
run
[[ "${rc}" != 0 ]] || fail "exit 0 after a failed k3s-uninstall.sh"
[[ -f "${root}/usr/local/bin/kubehz-k3s-uninstall.sh" ]] || fail "the script removed itself after a failure"
grep -q 'run this script again' <<<"${out}" || fail "the error does not say what to do: ${out}"
printf '0\n' >"${tmp}/uninstall-rc"
run
[[ "${rc}" == 0 ]] || fail "the next run: exit ${rc}: ${out}"
assert_removed

current="arguments and the test root are checked"
machine server
run_args() {
	rc=0
	out="$(env -i PATH="${stubs}:/usr/bin:/bin" KUBEHZ_UNINSTALL_ROOT="$1" bash "${here}/uninstall.sh" "${@:2}" 2>&1)" || rc=$?
}
run_args "${root}" --force
[[ "${rc}" == 2 ]] || fail "an unknown argument: exit ${rc}"
run_args / --help
[[ "${rc}" == 2 ]] || fail "KUBEHZ_UNINSTALL_ROOT=/: exit ${rc}"
run_args relative
[[ "${rc}" == 2 ]] || fail "a relative KUBEHZ_UNINSTALL_ROOT: exit ${rc}"
[[ -s "${log}" ]] && fail "a refused run called a command: $(cat "${log}")"

# ── render.sh ────────────────────────────────────────────────────────────

current="render.sh embeds the uninstall script byte for byte"
assets="${tmp}/assets"
mkdir -p "${assets}"
for f in kubehz-agent-linux-amd64 kubehz-agent-linux-arm64 install.yaml install-maintenance.yaml; do
	printf '%s\n' "${f}" >"${assets}/${f}"
done
rendered="${tmp}/k3s-state-v1.sh"
if ! bash "${here}/render.sh" v9.8.7 "${assets}" >"${rendered}" 2>"${tmp}/render.err"; then
	fail "render.sh failed: $(cat "${tmp}/render.err")"
fi
grep -q '@KUBEHZ_' "${rendered}" && fail "a placeholder is left"
grep -q '^readonly AGENT_VERSION="v9.8.7"$' "${rendered}" || fail "the version is not set"
grep -q "^readonly AGENT_SHA256_AMD64=\"$(sha256sum "${assets}/kubehz-agent-linux-amd64" | cut -d' ' -f1)\"$" "${rendered}" ||
	fail "the amd64 sha256 is not set"
bash -n "${rendered}" || fail "the rendered script has a syntax error"
# Run the function of the rendered script that writes the uninstall script
# (with a fake "install"), and compare what it writes. The function ends
# before install_agent_binary; the heredoc has "}" lines of its own.
body="$(sed -n '/^install_uninstall_script() {$/,/^install_agent_binary() {$/p' "${rendered}" | sed '$d')"
(
	WORK="${tmp}/work"
	UNINSTALL_BIN="${tmp}/work/kubehz-k3s-uninstall.sh"
	mkdir -p "${WORK}"
	# shellcheck disable=SC2329 # the eval'd function calls it
	install() { cp -- "${@: -2:1}" "${UNINSTALL_BIN}"; }
	eval "${body}"
	install_uninstall_script
	cmp -s "${here}/uninstall.sh" "${UNINSTALL_BIN}"
) || fail "the script that the rendered installer writes differs from uninstall.sh"

current="the installer writes the uninstall script first, at the paths that the uninstall uses"
main_body="$(sed -n '/^main() {$/,/^}$/p' "${rendered}")"
first="$(grep -n -m1 -x $'\tinstall_uninstall_script' <<<"${main_body}" | cut -d: -f1)" || true
binary="$(grep -n -m1 -x $'\tinstall_agent_binary' <<<"${main_body}" | cut -d: -f1)" || true
((${first:-0} > 0 && ${first:-0} < ${binary:-0})) ||
	fail "main does not call install_uninstall_script before install_agent_binary"
# has FILE LINE: FILE has LINE as a whole line (literal text).
has() { grep -qxF -e "$2" "$1"; }
# shellcheck disable=SC2016 # the lines are literal script text
if ! { has "${rendered}" 'readonly UNINSTALL_BIN="/usr/local/bin/kubehz-k3s-uninstall.sh"' &&
	has "${here}/uninstall.sh" 'readonly SELF="${BIN_DIR}/kubehz-k3s-uninstall.sh"' &&
	has "${here}/uninstall.sh" 'readonly BIN_DIR="${ROOT}/usr/local/bin"'; }; then
	fail "the installer and the uninstall do not agree on the uninstall script path"
fi
# shellcheck disable=SC2016 # the lines are literal script text
if ! { has "${rendered}" 'readonly AGENT_MANIFEST="/etc/kubehz/kubehz-agent.yaml"' &&
	has "${rendered}" $'\tinstall -m 0600 "${WORK}/agent.yaml" "${AGENT_MANIFEST}"' &&
	has "${here}/uninstall.sh" 'readonly AGENT_MANIFEST="${KUBEHZ_DIR}/kubehz-agent.yaml"' &&
	has "${here}/uninstall.sh" 'readonly KUBEHZ_DIR="${ROOT}/etc/kubehz"'; }; then
	fail "the installer does not keep the agent manifest where the uninstall reads it"
fi

current="render.sh fails on a missing asset"
rm -f "${assets}/install.yaml"
if bash "${here}/render.sh" v9.8.7 "${assets}" >/dev/null 2>&1; then
	fail "render.sh passed without install.yaml"
fi

if ((failures > 0)); then
	printf '%d check(s) failed\n' "${failures}" >&2
	exit 1
fi
echo "ok: k3s-state script tests passed"
