# syntax=docker/dockerfile:1
# Multi-stage build of the single cfgcanary binary. The runtime image is
# distroless (no shell), runs as a non-root user and contains the example
# config, payloads and WASM validator used by `cfgcanary demo`.
ARG GO_VERSION=1.27.4
FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=0.1.0-dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w -X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Version=${VERSION} \
      -X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Commit=${COMMIT} \
      -X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Date=${DATE}" \
      -o /out/cfgcanary ./cmd/cfgcanary && \
    GOOS=wasip1 GOARCH=wasm go build -o /out/examples/ratelimits/validator.wasm ./examples/ratelimits/validator && \
    cp -r examples/ratelimits/*.yaml examples/ratelimits/*.json examples/ratelimits/*.cue examples/ratelimits/payloads /out/examples/ratelimits/

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/raunaksahahere/cfgcanary" \
      org.opencontainers.image.description="Progressive delivery for configuration" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/cfgcanary /usr/local/bin/cfgcanary
COPY --from=build /out/examples /usr/share/cfgcanary/examples
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/cfgcanary"]
