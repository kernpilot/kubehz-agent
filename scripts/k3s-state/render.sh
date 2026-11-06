#!/usr/bin/env bash
# Renders the release copy of the install script: replaces the @...@ pins
# of scripts/k3s-state/v1.sh with the version and the sha256 values of the
# release assets, and writes the result to stdout.
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
out="$(sed \
	-e "s|@KUBEHZ_AGENT_VERSION@|${version}|g" \
	-e "s|@KUBEHZ_AGENT_SHA256_AMD64@|$(sum kubehz-agent-linux-amd64)|g" \
	-e "s|@KUBEHZ_AGENT_SHA256_ARM64@|$(sum kubehz-agent-linux-arm64)|g" \
	-e "s|@KUBEHZ_INSTALL_YAML_SHA256@|$(sum install.yaml)|g" \
	-e "s|@KUBEHZ_INSTALL_MAINTENANCE_YAML_SHA256@|$(sum install-maintenance.yaml)|g" \
	"${src}")"

if grep -q '@KUBEHZ_' <<<"${out}"; then
	echo "error: a pin of ${src} was not replaced" >&2
	exit 1
fi
printf '%s\n' "${out}"
