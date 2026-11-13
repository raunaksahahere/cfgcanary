#!/usr/bin/env bash
# Installs pinned, checksum-verified kind, kubectl and helm into .tools/bin.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
bin="$root/.tools/bin"; mkdir -p "$bin"
KIND=v0.33.0 HELM=v4.3.1 KUBECTL=v1.37.0
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) os=linux arch=amd64 ;;
  Linux-aarch64) os=linux arch=arm64 ;;
  Darwin-x86_64) os=darwin arch=amd64 ;;
  Darwin-arm64) os=darwin arch=arm64 ;;
  *) echo "unsupported platform" >&2; exit 1 ;;
esac
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fetch() { curl -fsSL --retry 3 -o "$1" "$2"; }
if ! "$bin/kind" version 2>/dev/null | grep -q "$KIND"; then
  fetch "$tmp/kind" "https://github.com/kubernetes-sigs/kind/releases/download/$KIND/kind-$os-$arch"
  fetch "$tmp/kind.sha" "https://github.com/kubernetes-sigs/kind/releases/download/$KIND/kind-$os-$arch.sha256sum"
  (cd "$tmp" && echo "$(awk '{print $1}' kind.sha)  kind" | sha256sum -c - >/dev/null)
  install -m 0755 "$tmp/kind" "$bin/kind"
fi
if ! "$bin/helm" version --short 2>/dev/null | grep -q "$HELM"; then
  fetch "$tmp/helm.tgz" "https://get.helm.sh/helm-$HELM-$os-$arch.tar.gz"
  fetch "$tmp/helm.sha" "https://get.helm.sh/helm-$HELM-$os-$arch.tar.gz.sha256sum"
  (cd "$tmp" && echo "$(awk '{print $1}' helm.sha)  helm.tgz" | sha256sum -c - >/dev/null)
  tar -xzf "$tmp/helm.tgz" -C "$tmp" && install -m 0755 "$tmp/$os-$arch/helm" "$bin/helm"
fi
if ! "$bin/kubectl" version --client 2>/dev/null | grep -q "$KUBECTL"; then
  fetch "$tmp/kubectl" "https://dl.k8s.io/release/$KUBECTL/bin/$os/$arch/kubectl"
  fetch "$tmp/kubectl.sha" "https://dl.k8s.io/release/$KUBECTL/bin/$os/$arch/kubectl.sha256"
  (cd "$tmp" && echo "$(cat kubectl.sha)  kubectl" | sha256sum -c - >/dev/null)
  install -m 0755 "$tmp/kubectl" "$bin/kubectl"
fi
echo "kind $KIND, helm $HELM, kubectl $KUBECTL in $bin"
