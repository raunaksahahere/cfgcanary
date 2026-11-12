#!/usr/bin/env bash
# Regenerates Go stubs, the REST gateway and the OpenAPI document from the proto.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
scripts/install-tools.sh
bin="$root/.tools/bin"; mkdir -p "$bin"
for t in google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc \
         github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2; do
  go build -o "$bin/$(basename "$t")" "$t"
done
rm -rf api/gen api/openapi; mkdir -p api/gen api/openapi
PATH="$bin:$PATH" .tools/protoc/bin/protoc \
  -I api -I third_party/googleapis -I .tools/protoc/include \
  --go_out=api/gen --go_opt=paths=source_relative \
  --go-grpc_out=api/gen --go-grpc_opt=paths=source_relative \
  --grpc-gateway_out=api/gen --grpc-gateway_opt=paths=source_relative \
  --openapiv2_out=api/openapi --openapiv2_opt=json_names_for_fields=true \
  cfgcanary/v1/cfgcanary.proto
