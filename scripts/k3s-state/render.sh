#!/usr/bin/env bash
# Renders the release copy of the install script: replaces the @...@ pins
# of scripts/k3s-state/v1.sh with the version and the sha256 values of the
# release assets, puts scripts/k3s-state/uninstall.sh in its heredoc, and
# writes the result to stdout.
#
#   scripts/k3s-state/render.sh <version> <dir with the release assets> > k3s-state-v1.sh
#
# The folder must hold kubehz-agent-linux-amd64, kubehz-agent-linux-arm64,
# install.yaml and install-maintenance.yaml.
set -euo pipefail

if (($# != 2)); then
	echo "usage: $0 <version> <asset dir>" >&2
	exit 2
fi
version="$1"
dir="$2"
[[ "${version}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
	echo "error: version '${version}' is not vX.Y.Z" >&2
	exit 1
}

sum() {
	[[ -f "${dir}/$1" ]] || {
		echo "error: ${dir}/$1 is missing" >&2
		exit 1
	}
	sha256sum "${dir}/$1" | cut -d' ' -f1
}

src="$(dirname "$0")/v1.sh"
uninstall="$(dirname "$0")/uninstall.sh"
eof="KUBEHZ_UNINSTALL_EOF"
if grep -qx "${eof}" "${uninstall}"; then
	echo "error: ${uninstall} has the line ${eof}, which ends its heredoc in ${src}" >&2
	exit 1
fi
# One assignment per sum: "exit" in sum() then stops this script (in an
# argument of sed it would only end the subshell).
sum_amd64="$(sum kubehz-agent-linux-amd64)"
sum_arm64="$(sum kubehz-agent-linux-arm64)"
sum_install="$(sum install.yaml)"
sum_maintenance="$(sum install-maintenance.yaml)"
# The values first, then the uninstall script: its text is not changed.
out="$(sed \
	-e "s|@KUBEHZ_AGENT_VERSION@|${version}|g" \
	-e "s|@KUBEHZ_AGENT_SHA256_AMD64@|${sum_amd64}|g" \
	-e "s|@KUBEHZ_AGENT_SHA256_ARM64@|${sum_arm64}|g" \
	-e "s|@KUBEHZ_INSTALL_YAML_SHA256@|${sum_install}|g" \
	-e "s|@KUBEHZ_INSTALL_MAINTENANCE_YAML_SHA256@|${sum_maintenance}|g" \
	"${src}" | sed -e '/^@KUBEHZ_UNINSTALL_SCRIPT@$/{' -e "r ${uninstall}" -e 'd' -e '}')"

if grep -q '@KUBEHZ_' <<<"${out}"; then
	echo "error: a pin of ${src} was not replaced" >&2
	exit 1
fi
# The embedded copy must be the uninstall script, byte for byte.
if ! cmp -s "${uninstall}" <(sed -n "/<<'${eof}'\$/,/^${eof}\$/p" <<<"${out}" | sed '1d;$d'); then
	echo "error: the uninstall script in the rendered ${src} differs from ${uninstall}" >&2
	exit 1
fi
printf '%s\n' "${out}"
