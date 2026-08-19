"""Configuration loading, env-var interpolation, adapter construction.

Two rules this module enforces, both of which the assignment calls out:

1. **Secrets never live in the repo.** `config/databases.yaml` contains
   ``${VAR}`` references. This loader resolves them from the process
   environment and raises if any referenced variable is unset. A benchmark that
   silently ran against four of five platforms because one password was missing
   would produce a results file that looks complete and isn't.

2. **The adapter registry is the only place a platform name maps to code.**
   Adding a database is one YAML block plus, at most, one adapter class. The
   runner, the workloads, and the report never learn a vendor's name.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from .adapters.base import GraphAdapter

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class ConfigError(Exception):
    """Raised for anything that would make a run silently wrong."""


# --------------------------------------------------------------------------
# .env
# --------------------------------------------------------------------------

def load_dotenv(path: Path) -> None:
    """Minimal .env reader.

    Deliberately not python-dotenv: this is twenty lines, has no dependency,
    and keeps `pip install -r requirements.txt` shorter for someone reproducing
    the benchmark. Existing environment variables always win, so CI secrets are
    never clobbered by a stale local file.
    """
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


# --------------------------------------------------------------------------
# YAML + interpolation
# --------------------------------------------------------------------------

def _interpolate(value: Any, missing: list[str], path: str = "") -> Any:
    """Recursively replace ``${VAR}`` with os.environ, collecting misses.

    Misses are collected rather than raised one at a time so the user gets the
    full list of variables to set in a single error, instead of discovering
    them one failed run at a time.
    """
    if isinstance(value, dict):
        return {k: _interpolate(v, missing, f"{path}.{k}" if path else k)
                for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v, missing, f"{path}[{i}]")
                for i, v in enumerate(value)]
    if isinstance(value, str):
        def sub(m: re.Match) -> str:
            var = m.group(1)
            env = os.environ.get(var)
            if env is None or env == "":
                missing.append(f"{var} (referenced by {path or '<root>'})")
                return ""
            return env
        return _ENV_REF.sub(sub, value)
    return value


def load_yaml(path: Path) -> dict:
    """Parse YAML and resolve env references, failing loudly on any miss."""
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: expected a mapping at the top level")

    missing: list[str] = []
    resolved = _interpolate(raw, missing)
    if missing:
        raise ConfigError(
            f"{path}: unset environment variable(s):\n  - "
            + "\n  - ".join(sorted(set(missing)))
            + "\n\nCopy .env.example to .env and fill it in, or export them. "
              "Credentials are read from the environment only and are never "
              "committed.")
    return resolved


# --------------------------------------------------------------------------
# adapter registry
# --------------------------------------------------------------------------

def _registry() -> dict[str, type[GraphAdapter]]:
    """Import adapters lazily.

    A missing optional driver (say, python-arango) should only break the
    platform that needs it -- not prevent the whole harness from starting. The
    error surfaces at the point that platform is constructed, naming the pip
    package to install.
    """
    from .adapters.bolt import BoltAdapter

    reg: dict[str, type[GraphAdapter]] = {"bolt": BoltAdapter}
    try:
        from .adapters.others import ArangoAdapter, FalkorAdapter
        reg["falkordb"] = FalkorAdapter
        reg["arangodb"] = ArangoAdapter
    except ImportError:  # driver not installed; reported per-platform below
        pass
    return reg


_PACKAGE_HINT = {
    "bolt": "neo4j",
    "falkordb": "falkordb",
    "arangodb": "python-arango",
}


def load_databases(path: Path, only: list[str] | None = None
                   ) -> list[tuple[dict, GraphAdapter]]:
    """Build one adapter instance per enabled platform.

    Args:
        path: config/databases.yaml (cloud) or databases.local.yaml (Docker).
        only: optional list of ``name`` values to restrict the run to. Used
            when one provider is down and you don't want to re-run the others.

    Returns:
        (config_block, adapter) pairs, in file order. File order is preserved
        so results tables are stable across runs -- a table whose row order
        changes between runs is hard to diff.
    """
    doc = load_yaml(path)
    entries = doc.get("databases")
    if not isinstance(entries, list) or not entries:
        raise ConfigError(f"{path}: 'databases' must be a non-empty list")

    registry = _registry()
    wanted = {n.lower() for n in only} if only else None
    out: list[tuple[dict, GraphAdapter]] = []
    skipped_disabled: list[str] = []

    for i, cfg in enumerate(entries):
        if not isinstance(cfg, dict):
            raise ConfigError(f"{path}: databases[{i}] must be a mapping")
        name = cfg.get("name")
        if not name:
            raise ConfigError(f"{path}: databases[{i}] is missing 'name'")
        if not cfg.get("enabled", True):
            skipped_disabled.append(name)
            continue
        if wanted is not None and name.lower() not in wanted:
            continue

        kind = cfg.get("adapter")
        if not kind:
            raise ConfigError(f"{path}: {name} is missing 'adapter'")
        if kind not in registry:
            hint = _PACKAGE_HINT.get(kind)
            extra = (f" Its driver may not be installed -- try "
                     f"`pip install {hint}`." if hint else "")
            raise ConfigError(
                f"{path}: {name} uses unknown adapter {kind!r}. "
                f"Available: {', '.join(sorted(registry))}.{extra}")
        out.append((cfg, registry[kind](cfg)))

    if skipped_disabled:
        print(f"[config] disabled in {path.name}: "
              f"{', '.join(skipped_disabled)}")
    if wanted is not None:
        found = {db.name.lower() for _c, db in out}
        for w in sorted(wanted - found):
            print(f"[config] warning: --only {w!r} matched no enabled database")
    if not out:
        raise ConfigError(
            f"{path}: no databases enabled (after --only filtering). "
            f"Nothing to benchmark.")
    return out
