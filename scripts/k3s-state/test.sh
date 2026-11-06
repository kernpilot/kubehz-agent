#!/usr/bin/env bash
# Tests of the managed-state scripts:
#   - uninstall.sh on a fake machine (KUBEHZ_UNINSTALL_ROOT), with stubs for
#     systemctl, k3s and the k3s uninstall scripts;
#   - render.sh with fake release assets;
#   - the rendered v1.sh on a fake machine (its paths moved into a folder),
#     as a server and as an agent node, and the installed uninstall after it.
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

# ── v1.sh (the install) ──────────────────────────────────────────────────
#
# The rendered script runs on a fake machine: a copy with its absolute paths
# (the readonly block) moved into ${iroot} and the root check removed.
# Stubs stand for systemctl, sleep, curl and install (no chown), the
# kubehz-agent binary (state-redeem writes what the Go redeemer writes),
# k3s and the k3s installer (which writes an uninstaller as the real one
# does). download() copies the fake assets. Every call goes to ${ilog}.

for f in kubehz-agent-linux-amd64 kubehz-agent-linux-arm64 install.yaml install-maintenance.yaml; do
	printf '%s\n' "${f}" >"${assets}/${f}"
done
bash "${here}/render.sh" v9.8.7 "${assets}" >"${tmp}/v1-rendered.sh"

test_cilium_sum="$(printf 'f%.0s' {1..64})"
istubs="${tmp}/istubs"
fakes="${tmp}/fakes"
ilog="${tmp}/install.log"
mkdir -p "${istubs}" "${fakes}"

cat >"${istubs}/systemctl" <<EOF
#!/usr/bin/env bash
printf 'systemctl %s\n' "\$*" >>"${ilog}"
if [[ "\$1" == "is-active" ]]; then exit "\$(cat "${tmp}/agent-active-rc")"; fi
exit 0
EOF
printf '#!/usr/bin/env bash\nexit 0\n' >"${istubs}/sleep"
cat >"${istubs}/curl" <<EOF
#!/usr/bin/env bash
printf 'curl %s\n' "\$*" >>"${ilog}"
exit 1
EOF
# install without -o/-g: the tests do not run as root.
cat >"${istubs}/install" <<'EOF'
#!/usr/bin/env bash
args=()
while (($# > 0)); do
	case "$1" in
	-o | -g) shift ;;
	*) args+=("$1") ;;
	esac
	shift
done
exec /usr/bin/install "${args[@]}"
EOF
chmod +x "${istubs}"/*

# The kubehz-agent binary. state-redeem reads the ticket from stdin and
# writes the files of the Go redeemer for the role (internal/staterenew,
# TestRedeemAgent and TestRedeemWritesTheStateFolder).
cat >"${fakes}/kubehz-agent" <<EOF
#!/usr/bin/env bash
printf 'kubehz-agent %s\n' "\$*" >>"${ilog}"
case "\$1" in
state-redeem)
	dir="" role="" kind=install
	while ((\$# > 0)); do
		case "\$1" in
		--dir) dir="\$2"; shift ;;
		--role) role="\$2"; shift ;;
		--recovery) kind=recovery ;;
		esac
		shift
	done
	read -r ticket
	[[ "\${ticket}" == "test-ticket" ]] || exit 1
	mkdir -p "\${dir}"
	: >"\${dir}/.lock"
	printf 'K10token\n' >"\${dir}/k3s-token"
	if [[ "\${role}" == "agent" ]]; then
		printf 'ROLE=agent\nMACHINE_ID=m-2\nSERVER_URL=https://k3s.example.com:6443\nK3S_VERSION=v1.37.2+k3s1\n' >"\${dir}/install.env"
	else
		mkdir -p "\${dir}/gen-1"
		echo key >"\${dir}/gen-1/client.key"
		echo '{}' >"\${dir}/renew.json"
		ln -s gen-1 "\${dir}/current"
		printf 'K10agenttoken\n' >"\${dir}/k3s-agent-token"
		has_token=0
		if [[ "\${kind}" == "install" ]]; then
			printf 'khz_agt_%064d' 0 >"\${dir}/agent-token"
			has_token=1
		fi
		printf 'ROLE=server\nKIND=%s\nCLUSTER_ID=c1\nMACHINE_ID=m-1\nSTATE_ENDPOINT=https://abc.state.kubehz.cloud:2379\nK3S_VERSION=v1.37.2+k3s1\nCILIUM_VERSION=%s\nCLUSTER_CIDR=10.42.0.0/16\nSERVICE_CIDR=10.43.0.0/16\nTLS_SANS=\nHAS_AUTHN_CONFIG=0\nHAS_AGENT_TOKEN=%s\n' \
			"\${kind}" "\$(cat "${tmp}/cilium-version")" "\${has_token}" >"\${dir}/install.env"
	fi
	;;
state-ack) exit "\$(cat "${tmp}/ack-rc")" ;;
esac
EOF
# k3s: kubectl answers ready, a node list, and no agent Secret.
cat >"${fakes}/k3s" <<EOF
#!/usr/bin/env bash
printf 'k3s %s\n' "\$*" >>"${ilog}"
case " \$* " in
*" get secret "*) exit 1 ;;
*" get nodes "*) printf 'node/%s\n' "${self_node}" ;;
*" -o yaml "*) printf 'kind: Namespace\n' ;;
*" -f - "*) cat >/dev/null ;;
esac
exit 0
EOF

# install_rendered: a fresh fake machine ${iroot} and the test copy of the
# rendered script ${icopy}.
install_rendered() {
	iroot="$(mktemp -d "${tmp}/imachine.XXXXXX")"
	icopy="${tmp}/v1-test.sh"
	: >"${ilog}"
	printf '0\n' >"${tmp}/ack-rc"
	printf '0\n' >"${tmp}/agent-active-rc"
	printf '1.20.4\n' >"${tmp}/cilium-version"
	mkdir -p "${tmp}/worktmp" "${iroot}/usr/local/bin" "${iroot}/etc/systemd/system"
	# The last line calls main. The embedded uninstall script has its own
	# call of main, which must stay.
	[[ "$(tail -n1 "${tmp}/v1-rendered.sh")" == 'main "$@"' ]] || fail "the rendered script does not end with the call of main"
	# A second Cilium pin (test only) tells the version of install.env from
	# the default.
	sed -E \
		-e "s|^(readonly [A-Z0-9_]+=\")/|\\1${iroot}/|" \
		-e 's|\[\[ "\$\{EUID\}" -eq 0 \]\]|true|' \
		-e "s|^readonly -a CILIUM_PINS=\\($|&\\n\\t\"1.99.1 ${test_cilium_sum}\"|" \
		-e '$d' \
		"${tmp}/v1-rendered.sh" >"${icopy}"
	# Each edit must hit (a missed anchor would test the real paths).
	local moved mains
	moved="$(grep -cE "^readonly [A-Z0-9_]+=\"${iroot}/" "${icopy}")" || true
	[[ "${moved}" == 8 ]] || fail "the test copy moved ${moved} paths, want 8"
	# shellcheck disable=SC2016 # a literal string
	grep -qF '"${EUID}" -eq 0' "${icopy}" && fail "the root check is still in the test copy"
	grep -qx "	\"1.99.1 ${test_cilium_sum}\"" "${icopy}" || fail "the test copy has no test Cilium pin"
	mains="$(grep -cx 'main "$@"' "${tmp}/v1-rendered.sh")" || true
	[[ "$(grep -cx 'main "$@"' "${icopy}")" == "$((mains - 1))" ]] || fail "the test copy did not lose exactly the last call of main"
	# The k3s installer: writes the unit and the uninstaller of the role, as
	# the real one does. The uninstaller removes what k3s-uninstall.sh and
	# k3s-agent-uninstall.sh remove.
	cat >"${fakes}/k3s-install.sh" <<EOF
#!/bin/sh
printf 'k3s-install INSTALL_K3S_EXEC=%s SKIP_START=%s SKIP_DOWNLOAD=%s\n' "\${INSTALL_K3S_EXEC}" "\${INSTALL_K3S_SKIP_START}" "\${INSTALL_K3S_SKIP_DOWNLOAD}" >>"${ilog}"
name=k3s
[ "\${INSTALL_K3S_EXEC}" = agent ] && name=k3s-agent
mkdir -p "${iroot}/etc/systemd/system" "${iroot}/usr/local/bin"
echo '[Unit]' >"${iroot}/etc/systemd/system/\${name}.service"
cat >"${iroot}/usr/local/bin/\${name}-uninstall.sh" <<UNINSTALL
#!/bin/sh
printf '%s-uninstall.sh\\\\n' "\${name}" >>"${ilog}"
rm -rf "${iroot}/etc/rancher/k3s" "${iroot}/var/lib/rancher/k3s" "${iroot}/usr/local/bin/k3s" "${iroot}/etc/systemd/system/\${name}.service"
rm -f "${iroot}/usr/local/bin/\${name}-uninstall.sh"
UNINSTALL
chmod +x "${iroot}/usr/local/bin/\${name}-uninstall.sh"
EOF
}

# run_install ARGS...: runs main of the test copy with the ticket on stdin.
# Sets ${rc} and ${out}.
run_install() {
	rc=0
	# shellcheck disable=SC2016 # the inner script expands its own variables
	out="$(printf 'test-ticket\n' | env -i PATH="${istubs}:/usr/bin:/bin" HOME="${tmp}" TMPDIR="${tmp}/worktmp" \
		FAKES="${fakes}" ILOG="${ilog}" bash -c '
			source "$0"
			download() {
				printf "download %s %s\n" "$1" "$3" >>"${ILOG}"
				case "$1" in
				*/kubehz-agent-linux-*) cp -- "${FAKES}/kubehz-agent" "$2" ;;
				*/k3s-io/k3s/*/install.sh) cp -- "${FAKES}/k3s-install.sh" "$2" ;;
				*/releases/download/*/k3s | */releases/download/*/k3s-arm64) cp -- "${FAKES}/k3s" "$2" ;;
				*/cilium-*.tgz) printf "chart\n" >"$2" ;;
				*/install.yaml | */install-maintenance.yaml) printf "kind: List\n" >"$2" ;;
				*) printf "unexpected download %s\n" "$1" >&2; return 1 ;;
				esac
			}
			main "$@"
		' "${icopy}" "$@" 2>&1)" || rc=$?
}

# iline PATTERN: the number of the first line of the install log that
# matches PATTERN, or 0.
iline() {
	local n
	n="$(grep -n -m1 -e "$1" "${ilog}" | cut -d: -f1)" || true
	printf '%s' "${n:-0}"
}

# run_uninstall: runs the uninstall script that the install wrote.
run_uninstall() {
	rc=0
	out="$(env -i PATH="${stubs}:/usr/bin:/bin" HOME="${tmp}" KUBEHZ_UNINSTALL_ROOT="${iroot}" \
		bash "${iroot}/usr/local/bin/kubehz-k3s-uninstall.sh" 2>&1)" || rc=$?
}

# assert_clean: no file and no link is left on the fake machine, and no
# work folder.
assert_clean() {
	local left
	left="$(find "${iroot}" -mindepth 1 ! -type d)"
	[[ -z "${left}" ]] || fail "the uninstall left files: ${left//${iroot}/}"
	left="$(find "${tmp}/worktmp" -mindepth 1)"
	[[ -z "${left}" ]] || fail "the install left its work folder: ${left}"
}

current="an agent node: no CSR flow, no certificate, no acknowledgement, no renewal timer"
install_rendered
run_install --role agent --label agent-1
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q '^kubehz-agent state-redeem .* --label agent-1 --role agent$' "${ilog}" ||
	fail "the ticket was not redeemed with --role agent: $(grep state-redeem "${ilog}")"
grep -q '^kubehz-agent state-\(ack\|renew\)' "${ilog}" && fail "an agent ran $(grep '^kubehz-agent state-' "${ilog}")"
for unit in kubehz-state-renew.service kubehz-state-renew.timer; do
	[[ -e "${iroot}/etc/systemd/system/${unit}" ]] && fail "an agent has ${unit}"
done
grep -q 'kubehz-state-renew' "${ilog}" && fail "systemctl touched the renewal timer: $(grep kubehz-state-renew "${ilog}")"
grep -qx 'k3s-install INSTALL_K3S_EXEC=agent SKIP_START=true SKIP_DOWNLOAD=true' "${ilog}" ||
	fail "k3s was not installed as an agent: $(grep k3s-install "${ilog}")"
grep -qx 'systemctl restart --no-block k3s-agent' "${ilog}" || fail "the k3s-agent service was not started"
grep -qx 'systemctl is-active --quiet k3s-agent' "${ilog}" || fail "the install did not wait for the agent"
grep -q '^k3s ' "${ilog}" && fail "kubectl ran on an agent node: $(grep '^k3s ' "${ilog}")"
grep -q 'cilium' "${ilog}" && fail "an agent downloaded Cilium"
cfg="${iroot}/etc/rancher/k3s/config.yaml"
if [[ -f "${cfg}" ]]; then
	grep -qx 'server: "https://k3s.example.com:6443"' "${cfg}" || fail "config.yaml does not name the server: $(cat "${cfg}")"
	grep -qx "token-file: ${iroot}/etc/kubehz/state/k3s-token" "${cfg}" || fail "config.yaml does not name the token file"
	grep -q 'datastore' "${cfg}" && fail "an agent's config.yaml has datastore settings"
	grep -q 'agent-token-file' "${cfg}" && fail "an agent's config.yaml has an agent-token-file"
	[[ "$(stat -c %a "${cfg}")" == 600 ]] || fail "config.yaml mode $(stat -c %a "${cfg}")"
else
	fail "no ${cfg}"
fi
state_files="$(cd "${iroot}/etc/kubehz/state" && find . -mindepth 1 | LC_ALL=C sort | tr '\n' ' ')"
[[ "${state_files}" == "./.lock ./install.env ./k3s-token " ]] || fail "an agent's state folder holds: ${state_files}"
[[ -e "${iroot}/etc/kubehz/kubehz-agent.yaml" ]] && fail "an agent applied the in-cluster agent"
[[ -e "${iroot}/var/lib/rancher/k3s/server" ]] && fail "an agent wrote server manifests"
grep -q 'joined https://k3s.example.com:6443' <<<"${out}" || fail "the output does not name the server: ${out}"

current="an agent node: a second run does not redeem again, a run as server stops"
: >"${ilog}"
run_install --role agent --label agent-1
[[ "${rc}" == 0 ]] || fail "second run: exit ${rc}: ${out}"
grep -q 'state-redeem' "${ilog}" && fail "the second run redeemed again"
: >"${ilog}"
run_install --label agent-1
[[ "${rc}" != 0 ]] || fail "a run as server on an agent node passed"
grep -q 'run the script with --role agent' <<<"${out}" || fail "the error does not name the role: ${out}"

current="an agent node: the uninstall removes everything the install wrote"
run_uninstall
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -qx 'k3s-agent-uninstall.sh' "${ilog}" || fail "k3s-agent-uninstall.sh did not run"
assert_clean

current="an agent role refuses the server options"
install_rendered
for args in "--role agent --recovery" "--role agent --maintenance" "--role worker"; do
	# shellcheck disable=SC2086 # the words are separate arguments
	run_install ${args}
	[[ "${rc}" != 0 ]] || fail "'${args}' passed"
done
grep -q 'state-redeem' "${ilog}" && fail "a refused run redeemed"

current="a server: the certificate is acknowledged after k3s runs, before the agent and the timer"
install_rendered
run_install --label server-1
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q '^kubehz-agent state-redeem .* --role server$' "${ilog}" || fail "the ticket was not redeemed with --role server (and without --recovery)"
start="$(iline '^systemctl restart --no-block k3s$')"
ready="$(iline '^k3s kubectl .* get --raw /readyz$')"
ack="$(iline "^kubehz-agent state-ack --dir ${iroot}/etc/kubehz/state --kubeconfig ${iroot}/etc/rancher/k3s/k3s.yaml$")"
apply="$(iline "^k3s kubectl .* apply -f ${tmp}/worktmp/.*/agent.yaml$")"
timer="$(iline '^systemctl enable --now kubehz-state-renew.timer$')"
((start > 0 && ready > start)) || fail "k3s did not start and answer (start ${start}, ready ${ready})"
((ack > ready)) || fail "the acknowledgement is not after k3s answered (ack line ${ack}, ready ${ready})"
((apply > ack && timer > apply)) || fail "the agent (${apply}) and the timer (${timer}) are not after the acknowledgement (${ack})"
grep -qx 'k3s-install INSTALL_K3S_EXEC=server SKIP_START=true SKIP_DOWNLOAD=true' "${ilog}" || fail "k3s was not installed as a server"
[[ -f "${iroot}/etc/systemd/system/kubehz-state-renew.timer" ]] || fail "no renewal timer"
grep -q '^datastore-certfile: .*/current/client.crt$' "${iroot}/etc/rancher/k3s/config.yaml" || fail "config.yaml has no datastore certificate"
# Two k3s tokens: the server token, and the agent token for agent nodes.
grep -qx "token-file: ${iroot}/etc/kubehz/state/k3s-token" "${iroot}/etc/rancher/k3s/config.yaml" || fail "config.yaml has no server token file"
grep -qx "agent-token-file: ${iroot}/etc/kubehz/state/k3s-agent-token" "${iroot}/etc/rancher/k3s/config.yaml" ||
	fail "config.yaml has no agent-token-file: $(cat "${iroot}/etc/rancher/k3s/config.yaml")"
# The kubehz agent token that the redemption made goes into the cluster
# before the in-cluster agent, then off the disk.
secret="$(iline "^k3s kubectl .* -n kubehz-system create secret generic kubehz-agent --from-file=agent-token=${iroot}/etc/kubehz/state/agent-token --dry-run=client -o yaml$")"
((secret > ack && secret < apply)) || fail "the agent token Secret (line ${secret}) is not between the acknowledgement (${ack}) and the agent (${apply})"
[[ -e "${iroot}/etc/kubehz/state/agent-token" ]] && fail "the agent token stayed on the disk"
grep -qx 'download https://helm.cilium.io/cilium-1.20.4.tgz 505abff8385811df1a949776fc42e9f10697989a332acc86d6e8933efcb855ed' "${ilog}" ||
	fail "Cilium is not the version of install.env with its pin: $(grep cilium "${ilog}")"

current="a server: the uninstall removes everything the install wrote"
run_uninstall
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
assert_clean

current="a server: a failed acknowledgement stops the install; the next run acknowledges without a new redemption"
install_rendered
printf '1\n' >"${tmp}/ack-rc"
run_install --label server-1
[[ "${rc}" != 0 ]] || fail "exit 0 after a failed acknowledgement"
grep -q 'Do not redeem the ticket again' <<<"${out}" || fail "the error does not say what to do: ${out}"
[[ -e "${iroot}/etc/systemd/system/kubehz-state-renew.timer" ]] && fail "the timer was installed before the acknowledgement"
grep -q ' apply -f .*/agent.yaml' "${ilog}" && fail "the in-cluster agent was installed before the acknowledgement"
printf '0\n' >"${tmp}/ack-rc"
: >"${ilog}"
run_install --label server-1
[[ "${rc}" == 0 ]] || fail "the next run: exit ${rc}: ${out}"
grep -q 'state-redeem' "${ilog}" && fail "the next run redeemed the ticket again"
grep -q '^kubehz-agent state-ack ' "${ilog}" || fail "the next run did not acknowledge"
run_uninstall
assert_clean

current="a server recovery: no agent token, the Node of the lost server goes"
install_rendered
run_install --label server-1 --recovery
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -q '^kubehz-agent state-redeem .* --role server --recovery$' "${ilog}" || fail "the ticket was not redeemed with --recovery"
grep -qx "^k3s kubectl .* delete node ${self_node} --ignore-not-found --wait=false" "${ilog}" || fail "the Node of the lost server was not removed"
grep -q 'create secret generic kubehz-agent' "${ilog}" && fail "a recovery replaced the agent token Secret"
grep -qx 'KIND=recovery' "${iroot}/etc/kubehz/state/install.env" || fail "install.env has no KIND=recovery"
run_uninstall
assert_clean

current="an interrupted recovery continues as a recovery, also without --recovery"
install_rendered
printf '1\n' >"${tmp}/ack-rc"
run_install --label server-1 --recovery
[[ "${rc}" != 0 ]] || fail "exit 0 after a failed acknowledgement"
printf '0\n' >"${tmp}/ack-rc"
: >"${ilog}"
run_install --label server-1
[[ "${rc}" == 0 ]] || fail "the next run: exit ${rc}: ${out}"
grep -q 'state-redeem' "${ilog}" && fail "the next run redeemed the ticket again"
grep -q 'continuing as a recovery' <<<"${out}" || fail "the next run did not continue as a recovery: ${out}"
grep -q " delete node ${self_node} " "${ilog}" || fail "the next run did not remove the Node of the lost server"
run_uninstall
assert_clean

current="an install ticket: a later run with --recovery stops"
install_rendered
printf '1\n' >"${tmp}/ack-rc"
run_install --label server-1
[[ "${rc}" != 0 ]] || fail "exit 0 after a failed acknowledgement"
: >"${ilog}"
run_install --label server-1 --recovery
[[ "${rc}" != 0 ]] || fail "a run with --recovery after an install ticket passed"
grep -q 'redeemed an install ticket; run the script without --recovery' <<<"${out}" || fail "the error does not say what to do: ${out}"
grep -q ' delete node ' "${ilog}" && fail "a run after an install ticket removed a Node"
run_uninstall
assert_clean

current="Cilium is the version of the install configuration"
install_rendered
printf '1.99.1\n' >"${tmp}/cilium-version"
run_install --label server-1
[[ "${rc}" == 0 ]] || fail "exit ${rc}: ${out}"
grep -qx "download https://helm.cilium.io/cilium-1.99.1.tgz ${test_cilium_sum}" "${ilog}" ||
	fail "Cilium is not the version of install.env: $(grep cilium "${ilog}")"
grep -q 'chart: https://%{KUBERNETES_API}%/static/charts/cilium-1.99.1.tgz' "${iroot}/var/lib/rancher/k3s/server/manifests/kubehz-cilium.yaml" ||
	fail "the HelmChart does not name the chart of install.env"
run_uninstall
assert_clean

current="a Cilium version without a pin stops before k3s"
install_rendered
printf '1.99.0\n' >"${tmp}/cilium-version"
run_install --label server-1
[[ "${rc}" != 0 ]] || fail "an unpinned Cilium version passed"
grep -q 'does not pin Cilium 1.99.0' <<<"${out}" || fail "the error does not name the version: ${out}"
grep -q 'k3s-install' "${ilog}" && fail "k3s was installed before the Cilium check"
run_uninstall
assert_clean

if ((failures > 0)); then
	printf '%d check(s) failed\n' "${failures}" >&2
	exit 1
fi
echo "ok: k3s-state script tests passed"
