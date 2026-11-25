#!/usr/bin/env bash
#
# Fails when an authoritative-only CRD would reach a tenant-facing control
# plane, or when the authoritative cluster's install would miss one.
#
# Usage: hack/check-crd-placement.sh <kustomize>

set -euo pipefail

KUSTOMIZE="${1:?usage: $0 <kustomize>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly KUSTOMIZE REPO_ROOT
cd "$REPO_ROOT"

names() {
  awk '/^kind: CustomResourceDefinition$/ { crd = 1 }
       /^---$/ { crd = 0 }
       crd && /^  name: / { print $2; crd = 0 }' |
    sort -u
}

crd_names() {
  "$KUSTOMIZE" build "$1" | names
}

authoritative="$(crd_names config/crd-authoritative)"
tenant="$(crd_names config/crd)"
bases="$(cat config/crd/bases/*.yaml | names)"

for list in authoritative tenant bases; do
  if [[ -z "${!list}" ]]; then
    echo "FAIL: the ${list} list has no CRDs, so nothing was checked"
    exit 1
  fi
done

status=0
while read -r crd; do
  if grep -qxF "$crd" <<<"$tenant"; then
    echo "FAIL: ${crd} is authoritative-only but config/crd installs it"
    status=1
  fi
done <<<"$authoritative"
while read -r crd; do
  if ! grep -qxF "$crd" <<<"$tenant"; then
    echo "FAIL: ${crd} is in config/crd/bases but config/crd does not install it; list it there, or move it to config/crd-authoritative in the manifests task"
    status=1
  fi
done <<<"$bases"
[[ "$status" -eq 0 ]] || exit 1

agent="$(crd_names config/agent)"
while read -r crd; do
  if ! grep -qxF "$crd" <<<"$agent"; then
    echo "FAIL: ${crd} is authoritative-only but config/agent does not install it"
    status=1
  fi
done <<<"$authoritative"

if [[ "$status" -eq 0 ]]; then
  echo "OK: $(wc -l <<<"$authoritative" | tr -d ' ') authoritative-only CRD(s) stay off config/crd and ship in config/agent"
fi
exit "$status"
