"""
common/service_profile.py
=========================
Loads a dataset profile YAML file and returns a ready-to-use service alias
dictionary compatible with the existing SERVICE_ALIAS_MAP contract.

Public API
----------
load_service_profile(path: str | Path) -> dict[str, str]
    Read a profile YAML, merge noise_services into the alias map under the
    NOISE label, and return a plain dict.  The returned dict is a drop-in
    replacement for SERVICE_ALIAS_MAP — all callers do `.get(key, fallback)`
    and need no changes.

Profile YAML schema
-------------------
profile_name: <str>          # human-readable name, shown in logs
service_aliases:             # mapping of lowercase process name → LABEL
  sshd: SYSTEM
  ...
noise_services:              # list of process names to map to NOISE
  - monitoring
  - health_check

Noise entries in `noise_services` are merged into `service_aliases`
automatically; they do not need to appear in both sections.

Error handling
--------------
- FileNotFoundError  : profile path does not exist.
- ValueError         : YAML is malformed or fails schema validation.

The module intentionally has no import-time side effects; loading is triggered
only when load_service_profile() is called (once, at config.py import time).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

import yaml

from common.config import NOISE_SERVICE_LABEL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = {"service_aliases", "noise_services"}


def _validate(data: object, path: str) -> None:
    """Raise ValueError with a clear message if the profile is malformed."""
    if not isinstance(data, dict):
        raise ValueError(
            f"Dataset profile '{path}': top-level must be a YAML mapping, "
            f"got {type(data).__name__}."
        )
    missing = _REQUIRED_KEYS - data.keys()
    if missing:
        raise ValueError(
            f"Dataset profile '{path}': missing required key(s): {sorted(missing)}. "
            f"Copy config/dataset_profiles/generic.yaml as a starting template."
        )
    aliases = data.get("service_aliases")
    noise = data.get("noise_services")
    if aliases is not None and not isinstance(aliases, dict):
        raise ValueError(
            f"Dataset profile '{path}': 'service_aliases' must be a YAML mapping "
            f"(key: VALUE pairs), got {type(aliases).__name__}."
        )
    if noise is not None and not isinstance(noise, list):
        raise ValueError(
            f"Dataset profile '{path}': 'noise_services' must be a YAML list, "
            f"got {type(noise).__name__}."
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_service_profile(path: Union[str, Path]) -> dict[str, str]:
    """Load a dataset profile YAML and return a service-alias dict.

    The returned dict maps lowercase process/daemon names to uppercase
    canonical subsystem labels (e.g. ``{"sshd": "SYSTEM", ...}``).
    Entries from ``noise_services`` are merged in automatically under the
    ``NOISE_SERVICE_LABEL`` value (default ``"NOISE"``).

    This dict is a drop-in replacement for the old hardcoded
    ``SERVICE_ALIAS_MAP`` in ``common/config.py`` — all callers use
    ``.get(name, fallback)`` and need no changes.

    Args:
        path: Absolute or relative path to a YAML profile file.

    Returns:
        dict mapping lowercase process name → canonical label string.

    Raises:
        FileNotFoundError: If the profile file does not exist.
        ValueError: If the YAML is malformed or fails schema validation.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Dataset profile not found: '{p}'. "
            f"Set the DATASET_PROFILE environment variable to a valid profile path, "
            f"or use the default 'config/dataset_profiles/hpe_synthetic.yaml'."
        )

    with p.open(encoding="utf-8") as fh:
        try:
            data = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ValueError(
                f"Dataset profile '{p}' contains invalid YAML: {exc}"
            ) from exc

    _validate(data, str(p))

    # Build alias map — normalise all keys to lowercase for case-insensitive lookup.
    aliases_raw: dict = data.get("service_aliases") or {}
    alias_map: dict[str, str] = {
        str(k).strip().lower(): str(v).strip().upper()
        for k, v in aliases_raw.items()
    }

    # Merge noise_services — they may or may not already be in service_aliases.
    noise_raw: list = data.get("noise_services") or []
    for entry in noise_raw:
        key = str(entry).strip().lower()
        if key:
            alias_map[key] = NOISE_SERVICE_LABEL

    profile_name = data.get("profile_name", p.stem)
    noise_count = sum(1 for v in alias_map.values() if v == NOISE_SERVICE_LABEL)
    logger.info(
        "Loaded dataset profile '%s' from '%s': %d alias(es), %d noise service(s).",
        profile_name, p, len(alias_map), noise_count,
    )

    return alias_map
