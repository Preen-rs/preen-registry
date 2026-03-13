# preen-registry

Signed plugin registry index for Preen. This repository is internal infrastructure for `preen-cli` registry resolution and trust verification.

## Scope

- Own and publish `registry-index.toml`.
- Own and publish `registry-index.toml.sig` (Sigstore keyless).
- Enforce index integrity and immutability rules in CI.
- Provide automation helper to update index entries from release tags.

## Security Invariants

- `rev` must be immutable (commit SHA or tag reference).
- `trusted_identity` must be a GitHub workflow identity.
- Registry signature issuer is fixed to GitHub OIDC.
- Preen CLI default registry identity:
  - `https://github.com/Preen-rs/preen-registry/.github/workflows/sign-index.yml@refs/heads/main`

## Distribution Endpoints

- Index:
  - `https://raw.githubusercontent.com/Preen-rs/preen-registry/main/registry-index.toml`
- Signature:
  - `https://raw.githubusercontent.com/Preen-rs/preen-registry/main/registry-index.toml.sig`

## Tooling

```bash
python3 scripts/registry_admin.py --help
python3 scripts/update_registry_entry.py --help
```
