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
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$")
GITHUB_REPO_RE = re.compile(r"^https://github.com/([^/]+)/([^/]+?)(?:\.git)?/?$")


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage registry-index.toml entries with add/update/remove subcommands."
    )
    parser.add_argument("--index", default="registry-index.toml", help="Path to registry index TOML.")
    parser.add_argument("--generated-at", help="RFC3339 generated_at override.")
    parser.add_argument("--dry-run", action="store_true", help="Print output instead of writing file.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    add_cmd = subparsers.add_parser("add", help="Add a new pack entry and its first version.")
    add_cmd.add_argument("--pack-id", required=True)
    add_cmd.add_argument("--name", required=True)
    add_cmd.add_argument("--description", required=True)
    add_cmd.add_argument("--repo-url", required=True)
    add_cmd.add_argument("--tag", required=True)
    add_cmd.add_argument("--identity")
    add_cmd.add_argument("--rev")
    add_cmd.add_argument("--manifest-hash")
    add_cmd.add_argument("--manifest-url")
    add_cmd.add_argument("--manifest-path")

    update_cmd = subparsers.add_parser("update", help="Add or update a version under an existing pack.")
    update_cmd.add_argument("--pack-id", required=True)
    update_cmd.add_argument("--tag", required=True)
    update_cmd.add_argument("--identity")
    update_cmd.add_argument("--rev")
    update_cmd.add_argument("--manifest-hash")
    update_cmd.add_argument("--manifest-url")
    update_cmd.add_argument("--manifest-path")

    remove_cmd = subparsers.add_parser("remove", help="Remove an entire pack or a single version.")
    remove_cmd.add_argument("--pack-id", required=True)
    remove_cmd.add_argument("--tag")

    return parser.parse_args()


def ensure_non_empty(value: str | None, field: str) -> str:
    if not value or not value.strip():
        fail(f"{field} is required")
    return value.strip()


def parse_tag_to_version(tag: str) -> str:
    value = tag.strip()
    if value.startswith("v"):
        value = value[1:]
    if not SEMVER_RE.match(value):
        fail(f"tag must map to semver, got: {tag}")
    return value


def validate_generated_at(raw: str) -> str:
    value = raw.strip()
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        fail(f"generated_at must be RFC3339-compatible: {exc}")
    return value


def utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    req = urllib.request.Request(url=url, headers={"User-Agent": "preen-registry-admin/1.0"})
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


def derive_default_identity(repo_url: str) -> str:
    match = GITHUB_REPO_RE.match(repo_url.strip())
    if not match:
        fail("cannot derive identity from non-GitHub repo_url; pass --identity explicitly")
    org, repo = match.groups()
    return (
        f"https://github.com/{org}/{repo}"
        "/.github/workflows/release-manual.yml@refs/heads/main"
    )


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


def entry_for_pack_id(data: dict, pack_id: str) -> dict | None:
    for entry in data.get("entries", []):
        if isinstance(entry, dict) and entry.get("pack_id") == pack_id:
            return entry
    return None


def semver_sort_key(version: str) -> tuple:
    match = SEMVER_RE.match(version)
    if not match:
        return (-1, -1, -1, -1, ())
    major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))
    prerelease = match.group(4)
    if prerelease is None:
        return (major, minor, patch, 1, ())
    tokens = []
    for token in prerelease.split("."):
        if token.isdigit():
            tokens.append((0, int(token)))
        else:
            tokens.append((1, token))
    return (major, minor, patch, 0, tuple(tokens))


def select_latest_version(versions: list[dict]) -> str:
    candidates = []
    for item in versions:
        if not isinstance(item, dict):
            continue
        value = item.get("version")
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())
    if not candidates:
        fail("cannot determine latest_version from empty versions")
    return max(candidates, key=semver_sort_key)


def add_entry(data: dict, args: argparse.Namespace) -> str:
    pack_id = ensure_non_empty(args.pack_id, "pack-id")
    if entry_for_pack_id(data, pack_id) is not None:
        fail(f"pack_id already exists: {pack_id}. use update instead")

    repo_url = ensure_non_empty(args.repo_url, "repo-url")
    version = parse_tag_to_version(args.tag)
    identity = args.identity.strip() if args.identity else derive_default_identity(repo_url)
    rev = args.rev.strip().lower() if args.rev else resolve_rev(repo_url, args.tag)
    if not HEX40_RE.match(rev):
        fail("rev must be a 40-char commit SHA")
    manifest_hash = compute_manifest_hash(args, repo_url)

    data["entries"].append(
        {
            "pack_id": pack_id,
            "name": ensure_non_empty(args.name, "name"),
            "description": ensure_non_empty(args.description, "description"),
            "repo_url": repo_url,
            "latest_version": version,
            "versions": [
                {
                    "version": version,
                    "rev": rev,
                    "manifest_hash": manifest_hash,
                    "trusted_identity": identity,
                }
            ],
        }
    )
    return f"added {pack_id}@{version} rev={rev} manifest_hash={manifest_hash}"


def update_entry(data: dict, args: argparse.Namespace) -> str:
    pack_id = ensure_non_empty(args.pack_id, "pack-id")
    entry = entry_for_pack_id(data, pack_id)
    if entry is None:
        fail(f"pack_id not found: {pack_id}. use add first")

    repo_url = ensure_non_empty(entry.get("repo_url"), "repo_url")
    version = parse_tag_to_version(args.tag)
    identity = args.identity.strip() if args.identity else derive_default_identity(repo_url)
    rev = args.rev.strip().lower() if args.rev else resolve_rev(repo_url, args.tag)
    if not HEX40_RE.match(rev):
        fail("rev must be a 40-char commit SHA")
    manifest_hash = compute_manifest_hash(args, repo_url)

    versions = entry.get("versions")
    if not isinstance(versions, list):
        versions = []
        entry["versions"] = versions
    target = None
    for item in versions:
        if isinstance(item, dict) and item.get("version") == version:
            target = item
            break
    if target is None:
        target = {"version": version}
        versions.append(target)

    target["version"] = version
    target["rev"] = rev
    target["manifest_hash"] = manifest_hash
    target["trusted_identity"] = identity
    entry["latest_version"] = version
    return f"updated {pack_id}@{version} rev={rev} manifest_hash={manifest_hash}"


def remove_entry(data: dict, args: argparse.Namespace) -> str:
    pack_id = ensure_non_empty(args.pack_id, "pack-id")
    entries = data.get("entries")
    if not isinstance(entries, list):
        fail("invalid entries structure")
    entry = entry_for_pack_id(data, pack_id)
    if entry is None:
        fail(f"pack_id not found: {pack_id}")

    if not args.tag:
        data["entries"] = [item for item in entries if item is not entry]
        return f"removed pack {pack_id}"

    version = parse_tag_to_version(args.tag)
    versions = entry.get("versions")
    if not isinstance(versions, list) or not versions:
        fail(f"pack has no versions: {pack_id}")
    filtered = []
    removed = False
    for item in versions:
        if isinstance(item, dict) and item.get("version") == version:
            removed = True
            continue
        filtered.append(item)
    if not removed:
        fail(f"version not found: {pack_id}@{version}")

    if not filtered:
        data["entries"] = [item for item in entries if item is not entry]
        return f"removed {pack_id}@{version} and deleted empty pack"

    entry["versions"] = filtered
    entry["latest_version"] = select_latest_version(filtered)
    return f"removed {pack_id}@{version}; latest_version={entry['latest_version']}"


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
        versions = entry.get("versions")
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


def write_atomic(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def main() -> None:
    args = parse_args()
    index_path = Path(args.index)
    data = load_index(index_path)

    if args.command == "add":
        message = add_entry(data, args)
    elif args.command == "update":
        message = update_entry(data, args)
    elif args.command == "remove":
        message = remove_entry(data, args)
    else:
        fail(f"unsupported command: {args.command}")

    generated_at = args.generated_at or utc_now_rfc3339()
    data["generated_at"] = validate_generated_at(generated_at)
    output = emit_index(data)
    if args.dry_run:
        print(output, end="")
        return
    write_atomic(index_path, output)
    print(message)


if __name__ == "__main__":
    main()
