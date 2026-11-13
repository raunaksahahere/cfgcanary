#!/usr/bin/env bash
# Cross-compiles release archives and checksums into dist/.
#   scripts/dist.sh 0.1.0
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=${1:?usage: scripts/dist.sh VERSION}
COMMIT=$(git rev-parse --short=12 HEAD)
DATE=$(git log -1 --format=%cI)
PKG=github.com/raunaksahahere/cfgcanary/internal/buildinfo
LDFLAGS="-s -w -X $PKG.Version=$VERSION -X $PKG.Commit=$COMMIT -X $PKG.Date=$DATE"
rm -rf dist && mkdir -p dist
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
  os=${target%/*} arch=${target#*/}
  name="cfgcanary_${VERSION}_${os}_${arch}"
  mkdir -p "dist/$name"
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "$LDFLAGS" -o "dist/$name/cfgcanary" ./cmd/cfgcanary
  cp LICENSE README.md CHANGELOG.md "dist/$name/"
  tar -C dist -czf "dist/$name.tar.gz" --owner=0 --group=0 --mtime="$DATE" "$name"
  rm -rf "dist/${name:?}"
done
(cd dist && sha256sum ./*.tar.gz | sed 's| \./| |' > SHA256SUMS)
cat dist/SHA256SUMS
