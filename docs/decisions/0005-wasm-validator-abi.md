# 0005. Consumer-parser validators are WASI commands run by wazero

**Status:** accepted

**Context.** CrowdStrike's Channel File 291 analysis describes a validator that accepted input the
interpreter could not handle. The fix is to run the consumer's own parser before rollout, without
trusting that parser's code.

**Decision.** ABI `cfgcanary-validator/v1`: the module is a WASI preview 1 command. The payload is
its standard input. It writes one JSON object `{"ok": bool, "errors": [string]}` to standard
output and exits 0. cfgcanary gives it no preopened directories, no environment, no arguments
beyond `argv[0]`, no sockets, a memory page limit, a wall-clock deadline that closes the module,
and a bounded stdout buffer. Timeout, trap, non-zero exit, oversized or malformed output all
reject the payload.

**Why this ABI.** Plain Go compiles to it (`GOOS=wasip1 GOARCH=wasm`) so the validator can import
the same parsing package as the consumer. No custom host functions to audit.

**Consequences.** One instantiation per validation; compiled modules are cached by digest.
