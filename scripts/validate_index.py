#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
import re
import sys
import tomllib
from pathlib import Path


def fail(message: str) -> None:
    print(f"ERROR: {message}")
    raise SystemExit(1)


HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
TAG_RE = re.compile(r"^(refs/tags/.+|v\d+\.\d+\.\d+(?:[-+][A-Za-z0-9._-]+)?)$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-+][A-Za-z0-9._-]+)?$")
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
TRUSTED_IDENTITY_RE = re.compile(
    r"^https://github.com/[^/]+/[^/]+/"
    r"\.github/workflows/[A-Za-z0-9._-]+\.yml@refs/(heads|tags)/.+$"
)
MUTABLE_REFS = {
    "main",
    "master",
    "develop",
    "dev",
    "latest",
    "head",
}


def require_non_empty_string(table: dict, key: str, field: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        fail(f"{field} is required")
    return value.strip()


def validate_generated_at(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value.strip():
        fail("generated_at must be a non-empty RFC3339 string when set")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        fail(f"generated_at must be RFC3339-compatible ({exc})")


def is_immutable_rev(rev: str) -> bool:
    value = rev.strip()
    if not value:
        return False
    if value.lower() in MUTABLE_REFS:
        return False
    if value.startswith("refs/heads/"):
        return False
    if HEX40_RE.match(value):
        return True
    if TAG_RE.match(value):
        return True
    return False


def main() -> None:
    path = Path("registry-index.toml")
    if not path.exists():
        fail("registry-index.toml not found")

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"invalid TOML: {exc}")

    if data.get("schema_version") != 1:
        fail("schema_version must be 1")
    validate_generated_at(data.get("generated_at"))

    entries = data.get("entries")
    if not isinstance(entries, list) or not entries:
        fail("entries must be a non-empty array")

    seen = set()
    for idx, entry in enumerate(entries):
        prefix = f"entries[{idx}]"
        if not isinstance(entry, dict):
            fail(f"{prefix} must be a table")
        pack_id = require_non_empty_string(entry, "pack_id", f"{prefix}.pack_id")
        repo_url = require_non_empty_string(entry, "repo_url", f"{prefix}.repo_url")
        latest = require_non_empty_string(entry, "latest_version", f"{prefix}.latest_version")
        versions = entry.get("versions")

        if pack_id in seen:
            fail(f"duplicate pack_id: {pack_id}")
        seen.add(pack_id)
        if not repo_url.startswith("https://github.com/"):
            fail(f"{prefix}.repo_url must start with https://github.com/")
        if not SEMVER_RE.match(latest):
            fail(f"{prefix}.latest_version must be semver (x.y.z)")
        if not isinstance(versions, list) or not versions:
            fail(f"{prefix}.versions must be a non-empty array")

        found_latest = False
        seen_versions = set()
        for v_idx, version in enumerate(versions):
            vprefix = f"{prefix}.versions[{v_idx}]"
            if not isinstance(version, dict):
                fail(f"{vprefix} must be a table")
            ver = require_non_empty_string(version, "version", f"{vprefix}.version")
            rev = require_non_empty_string(version, "rev", f"{vprefix}.rev")
            if not SEMVER_RE.match(ver):
                fail(f"{vprefix}.version must be semver (x.y.z)")
            if ver in seen_versions:
                fail(f"duplicate version for {pack_id}: {ver}")
            seen_versions.add(ver)
            if not is_immutable_rev(rev):
                fail(
                    f"{vprefix}.rev must be an immutable ref (40-char commit SHA, refs/tags/*, or vX.Y.Z tag)"
                )
            manifest_hash = version.get("manifest_hash")
            if manifest_hash is not None:
                if not isinstance(manifest_hash, str) or not SHA256_RE.match(manifest_hash.strip()):
                    fail(f"{vprefix}.manifest_hash must match sha256:<64-hex>")
            trusted_identity = version.get("trusted_identity")
            if trusted_identity is not None:
                if (
                    not isinstance(trusted_identity, str)
                    or not TRUSTED_IDENTITY_RE.match(trusted_identity.strip())
                ):
                    fail(
                        f"{vprefix}.trusted_identity must match GitHub workflow identity format"
                    )
            if ver == latest:
                found_latest = True
        if not found_latest:
            fail(f"{prefix}.latest_version not found in versions")

    print("registry-index.toml is valid")


if __name__ == "__main__":
    main()
