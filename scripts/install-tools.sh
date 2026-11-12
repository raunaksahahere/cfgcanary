#!/usr/bin/env bash
# Installs the pinned, checksum-verified protoc into .tools/. Go plugins are
# pinned in go.mod (tool directives) and need no installation.
set -euo pipefail
PROTOC_VERSION=36.2
root="$(cd "$(dirname "$0")/.." && pwd)"
dest="$root/.tools/protoc"
if [ -x "$dest/bin/protoc" ] && "$dest/bin/protoc" --version | grep -q " $PROTOC_VERSION\$"; then exit 0; fi
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)  asset=linux-x86_64;  sum=121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278 ;;
  *) echo "install protoc $PROTOC_VERSION manually into $dest (unsupported host for auto-install)" >&2; exit 1 ;;
esac
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
curl -fsSL -o "$tmp/protoc.zip" "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${asset}.zip"
echo "$sum  $tmp/protoc.zip" | sha256sum -c - >/dev/null
rm -rf "$dest"; mkdir -p "$dest"; unzip -q "$tmp/protoc.zip" -d "$dest"
