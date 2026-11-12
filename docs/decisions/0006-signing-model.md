# 0006. The control plane signs artifacts and delivery statements

**Status:** accepted

**Context.** Blobs are fetched from the object store, off the control plane's path. Agents need to
know the bytes are the ones that were validated, for this config and environment, and that the
generation they are told to apply was issued by the control plane.

**Decision.** The control plane holds two ed25519 keys (purposes `artifact` and `delivery`).
Agents hold a trust bundle of public keys by key ID and purpose; rotation is "add new key to
bundles, switch signer, remove old key". Statements are canonical JSON with a fixed field order
and a domain-separation prefix.

**Not covered.** A compromised control plane can sign anything. Publisher-held offline keys and
Sigstore/cosign bundles are future work; the statement format has a `key_id` so a second signer
can be added without changing agents' verification flow.
