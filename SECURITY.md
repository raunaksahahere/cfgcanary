# Security policy

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting for this
repository (Security tab, "Report a vulnerability"). Include the version, a description, and steps
to reproduce. You should receive an acknowledgement within a week.

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | yes (alpha) |

## Security model in brief

- Agents and operators authenticate with mTLS client certificates; identities are SPIFFE-style
  URI SANs. Authorization is enforced on the server for every API method.
- Artifacts and delivery decisions are signed with ed25519; agents verify signatures, scope, ring
  range and digest before applying, and again when loading from disk.
- Artifact downloads use short-lived presigned URLs to a private bucket, restricted to configured
  origins; redirects are not followed.
- Consumer validators run in a wazero sandbox without filesystem, environment or network access,
  with memory, time and output limits.
- Privileged actions and denied attempts are recorded in an append-only audit log.

Out of scope: compromise of the signing keys, the certificate authority or PostgreSQL. Config
payloads are not a secrets channel and are stored unencrypted. Development credentials created by
`cfgcanary dev bootstrap` are for local use only.
