from __future__ import annotations

import copy
import json
from pathlib import Path

from config import PROVIDERS


def load_provider_pool(
    provider_names_text: str = "all",
    provider_config_file: Path | None = None,
) -> list[dict]:
    providers = _load_providers(provider_config_file)
    _validate_providers(providers)
    providers = [provider for provider in providers if provider.get("enabled", True)]

    requested = {
        name.strip() for name in provider_names_text.split(",") if name.strip()
    }
    if requested and "all" not in requested:
        providers = [provider for provider in providers if provider["name"] in requested]
    return providers


def _validate_providers(providers: list[dict]) -> None:
    names = []
    for index, provider in enumerate(providers):
        if not isinstance(provider, dict):
            raise ValueError(f"Provider at index {index} must be an object")
        name = str(provider.get("name", "")).strip()
        if not name:
            raise ValueError(f"Provider at index {index} has no non-empty name")
        names.append(name)

    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        raise ValueError(
            "Provider names must be unique; duplicate names: "
            + ", ".join(duplicate_names)
        )


def _load_providers(provider_config_file: Path | None) -> list[dict]:
    if provider_config_file is None:
        return copy.deepcopy(PROVIDERS)

    with provider_config_file.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if isinstance(payload, dict):
        providers = payload.get("providers", [])
    else:
        providers = payload
    if not isinstance(providers, list):
        raise ValueError("Provider config must be a list or an object with a providers list")
    return providers
