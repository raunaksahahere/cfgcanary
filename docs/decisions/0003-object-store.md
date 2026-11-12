# 0003. S3 API with a pinned MinIO-compatible server for local use

**Status:** accepted (checked 2026-11-12)

**Context.** P2 names MinIO. The upstream `minio/minio` repository is archived and its Docker Hub
and Quay images no longer resolve (`docker manifest inspect minio/minio:latest` fails; the GitHub
API reports `archived: true`).

**Decision.** cfgcanary speaks the S3 API through `minio-go` (Apache-2.0) and is tested against
`pgsty/minio:RELEASE.2026-08-04T00-00-00Z`, a maintained fork of the MinIO server. The server is
AGPL-3.0 and runs as a separate process; nothing from it is linked or redistributed.

**Consequences.** Any S3-compatible store that supports presigned GET and conditional writes
should work, but only the pinned image is tested.
