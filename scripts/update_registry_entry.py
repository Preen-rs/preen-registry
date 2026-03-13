#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-+][A-Za-z0-9._-]+)?$")
GITHUB_REPO_RE = re.compile(r"^https://github.com/([^/]+)/([^/]+?)(?:\.git)?/?$")


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update a registry-index.toml entry from an immutable rulepack release tag."
    )
    parser.add_argument("--index", default="registry-index.toml", help="Path to registry index TOML.")
    parser.add_argument("--pack-id", required=True, help="Registry pack_id.")
    parser.add_argument("--tag", required=True, help="Release tag (for example: v1.0.7).")
    parser.add_argument("--identity", required=True, help="Trusted workflow identity.")
    parser.add_argument("--repo-url", help="Rulepack Git repository URL.")
    parser.add_argument("--name", help="Display name (required only when creating new entry).")
    parser.add_argument(
        "--description",
        help="Description (required only when creating new entry).",
    )
    parser.add_argument("--rev", help="Pinned commit SHA. If omitted, resolved from tag.")
    parser.add_argument(
        "--manifest-hash",
        help="Manifest hash in format sha256:<hex>. If omitted, computed from manifest.",
    )
    parser.add_argument(
        "--manifest-url",
        help="Explicit manifest URL for hash calculation. Defaults to GitHub raw URL from repo/tag.",
    )
    parser.add_argument(
        "--manifest-path",
        help="Local manifest.toml path for hash calculation (overrides --manifest-url).",
    )
    parser.add_argument(
        "--generated-at",
        help="Explicit generated_at (RFC3339). Default: current UTC timestamp.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print output instead of writing file.")
    return parser.parse_args()


def parse_version_from_tag(tag: str) -> str:
    value = tag.strip()
    if value.startswith("v"):
        value = value[1:]
    if not SEMVER_RE.match(value):
        fail(f"tag must map to semver, got: {tag}")
    return value


def resolve_rev(repo_url: str, tag: str) -> str:
    deref_ref = f"refs/tags/{tag}^{{}}"
    direct_ref = f"refs/tags/{tag}"
    try:
        output = subprocess.check_output(
            ["git", "ls-remote", repo_url, deref_ref, direct_ref],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    except subprocess.CalledProcessError as exc:
        fail(f"git ls-remote failed: {exc.output.strip()}")
    if not output:
        fail(f"tag not found: {tag}")
    rev_by_ref: dict[str, str] = {}
    for line in output.splitlines():
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        sha, ref = parts[0].strip().lower(), parts[1].strip()
        rev_by_ref[ref] = sha
    rev = rev_by_ref.get(deref_ref) or rev_by_ref.get(direct_ref)
    if not rev:
        fail(f"tag not found: {tag}")
    if not HEX40_RE.match(rev):
        fail(f"resolved rev is not a 40-char commit SHA: {rev}")
    return rev


def derive_manifest_url(repo_url: str, tag: str) -> str:
    match = GITHUB_REPO_RE.match(repo_url.strip())
    if not match:
        fail("cannot derive manifest URL from non-GitHub repo_url; pass --manifest-url")
    org, repo = match.groups()
    return f"https://raw.githubusercontent.com/{org}/{repo}/{tag}/manifest.toml"


def read_bytes_from_url(url: str) -> bytes:
    req = urllib.request.Request(url=url, headers={"User-Agent": "preen-registry-updater/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.read()
    except urllib.error.URLError as exc:
        fail(f"failed to fetch {url}: {exc}")
    raise AssertionError("unreachable")


def read_manifest_bytes(args: argparse.Namespace, repo_url: str) -> bytes:
    if args.manifest_path:
        path = Path(args.manifest_path)
        if not path.exists():
            fail(f"manifest path not found: {path}")
        return path.read_bytes()
    manifest_url = args.manifest_url or derive_manifest_url(repo_url, args.tag)
    return read_bytes_from_url(manifest_url)


def normalize_sha256(raw: str) -> str:
    value = raw.strip()
    if value.startswith("sha256:"):
        value = value[len("sha256:") :]
    value = value.lower()
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        fail("manifest hash must be sha256:<64-hex> or <64-hex>")
    return f"sha256:{value}"


def compute_manifest_hash(args: argparse.Namespace, repo_url: str) -> str:
    if args.manifest_hash:
        return normalize_sha256(args.manifest_hash)
    digest = hashlib.sha256(read_manifest_bytes(args, repo_url)).hexdigest()
    return f"sha256:{digest}"


def utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_generated_at(raw: str) -> str:
    value = raw.strip()
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        fail(f"generated_at must be RFC3339-compatible: {exc}")
    return value


def load_index(path: Path) -> dict:
    if not path.exists():
        fail(f"index file not found: {path}")
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        fail(f"failed to parse TOML index: {exc}")
    if data.get("schema_version") != 1:
        fail("registry schema_version must be 1")
    entries = data.get("entries")
    if not isinstance(entries, list):
        fail("registry entries must be an array")
    return data


def ensure_non_empty(value: str | None, field: str) -> str:
    if not value or not value.strip():
        fail(f"{field} is required")
    return value.strip()


def upsert_entry(
    data: dict,
    *,
    pack_id: str,
    name: str | None,
    description: str | None,
    repo_url: str | None,
    version: str,
    rev: str,
    manifest_hash: str,
    identity: str,
) -> None:
    entries = data["entries"]
    target = None
    for entry in entries:
        if isinstance(entry, dict) and entry.get("pack_id") == pack_id:
            target = entry
            break

    if target is None:
        target = {
            "pack_id": pack_id,
            "name": ensure_non_empty(name, "name"),
            "description": ensure_non_empty(description, "description"),
            "repo_url": ensure_non_empty(repo_url, "repo_url"),
            "latest_version": version,
            "versions": [],
        }
        entries.append(target)
    else:
        if repo_url:
            target["repo_url"] = repo_url.strip()
        if name:
            target["name"] = name.strip()
        if description:
            target["description"] = description.strip()

    target["latest_version"] = version
    versions = target.get("versions")
    if not isinstance(versions, list):
        versions = []
        target["versions"] = versions

    found = None
    for item in versions:
        if isinstance(item, dict) and item.get("version") == version:
            found = item
            break
    if found is None:
        found = {"version": version}
        versions.append(found)

    found["version"] = version
    found["rev"] = rev
    found["manifest_hash"] = manifest_hash
    found["trusted_identity"] = identity.strip()


def toml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def emit_index(data: dict) -> str:
    lines: list[str] = []
    lines.append("schema_version = 1")
    generated_at = data.get("generated_at")
    if isinstance(generated_at, str) and generated_at.strip():
        lines.append(f'generated_at = "{toml_escape(generated_at.strip())}"')
    lines.append("")
    for entry in data.get("entries", []):
        if not isinstance(entry, dict):
            continue
        lines.append("[[entries]]")
        lines.append(f'pack_id = "{toml_escape(str(entry.get("pack_id", "")).strip())}"')
        lines.append(f'name = "{toml_escape(str(entry.get("name", "")).strip())}"')
        lines.append(f'description = "{toml_escape(str(entry.get("description", "")).strip())}"')
        lines.append(f'repo_url = "{toml_escape(str(entry.get("repo_url", "")).strip())}"')
        lines.append(f'latest_version = "{toml_escape(str(entry.get("latest_version", "")).strip())}"')
        lines.append("")

        versions = entry.get("versions", [])
        if isinstance(versions, list):
            for version in versions:
                if not isinstance(version, dict):
                    continue
                lines.append("  [[entries.versions]]")
                lines.append(f'  version = "{toml_escape(str(version.get("version", "")).strip())}"')
                lines.append(f'  rev = "{toml_escape(str(version.get("rev", "")).strip())}"')
                manifest_hash = version.get("manifest_hash")
                if isinstance(manifest_hash, str) and manifest_hash.strip():
                    lines.append(f'  manifest_hash = "{toml_escape(manifest_hash.strip())}"')
                trusted_identity = version.get("trusted_identity")
                if isinstance(trusted_identity, str) and trusted_identity.strip():
                    lines.append(f'  trusted_identity = "{toml_escape(trusted_identity.strip())}"')
                lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    pack_id = ensure_non_empty(args.pack_id, "pack-id")
    identity = ensure_non_empty(args.identity, "identity")
    version = parse_version_from_tag(args.tag)
    index_path = Path(args.index)
    data = load_index(index_path)

    existing_repo_url = None
    for entry in data["entries"]:
        if isinstance(entry, dict) and entry.get("pack_id") == pack_id:
            value = entry.get("repo_url")
            if isinstance(value, str) and value.strip():
                existing_repo_url = value.strip()
            break
    repo_url = (args.repo_url or existing_repo_url or "").strip()
    if not repo_url:
        fail("repo-url is required for a new pack_id")

    rev = (args.rev or "").strip().lower()
    if not rev:
        rev = resolve_rev(repo_url, args.tag)
    if not HEX40_RE.match(rev):
        fail("rev must be a 40-char commit SHA")

    manifest_hash = compute_manifest_hash(args, repo_url)

    upsert_entry(
        data,
        pack_id=pack_id,
        name=args.name,
        description=args.description,
        repo_url=repo_url,
        version=version,
        rev=rev,
        manifest_hash=manifest_hash,
        identity=identity,
    )
    generated_at = args.generated_at or utc_now_rfc3339()
    data["generated_at"] = validate_generated_at(generated_at)

    output = emit_index(data)
    if args.dry_run:
        print(output, end="")
        return
    index_path.write_text(output, encoding="utf-8")
    print(
        f"updated {index_path} for {pack_id}@{version} rev={rev} manifest_hash={manifest_hash}"
    )


if __name__ == "__main__":
    main()
