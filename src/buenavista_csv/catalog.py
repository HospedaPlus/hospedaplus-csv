"""
Shared Reference Catalog Loader (catalog_version 1.1.0).

Loads schemas/catalog.json to guarantee that F1 and F4 use the exact same
hotel codes (HP-XX-NNN), room types, currencies, channel codes (AG01..AG15),
commission percentages, country season calendars, and pricing multipliers as F2, F3, and F5.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_CATALOG_PATH = Path(__file__).resolve().parents[2] / "schemas" / "catalog.json"


def load_catalog(catalog_path: Path = DEFAULT_CATALOG_PATH) -> dict[str, Any]:
    if not catalog_path.exists():
        raise FileNotFoundError(f"Shared catalog not found at: {catalog_path}")
    return json.loads(catalog_path.read_text(encoding="utf-8"))


CATALOG: dict[str, Any] = load_catalog()
CATALOG_VERSION: str = CATALOG["catalog_version"]
REPORTING_CURRENCY: str = CATALOG["reporting_currency"]

COUNTRIES: dict[str, dict[str, Any]] = {c["code"]: c for c in CATALOG["countries"]}
ALLOWED_CURRENCIES: set[str] = {c["code"] for c in CATALOG["currencies"]}
HOTEL_CURRENCIES: set[str] = {c["code"] for c in CATALOG["currencies"] if "hotel currency" in c.get("role", "")}

ROOM_TYPES: dict[str, dict[str, Any]] = {r["code"]: r for r in CATALOG["room_types"]}
ALLOWED_ROOM_TYPES: list[str] = [r["code"] for r in CATALOG["room_types"]]

HOTELS: list[dict[str, Any]] = CATALOG["hotels"]
HOTELS_BY_CODE: dict[str, dict[str, Any]] = {h["code"]: h for h in HOTELS}
HOTEL_CODES: list[str] = [h["code"] for h in HOTELS]

# Map legacy hotel-001..hotel-028 to canonical HP-XX-NNN codes for backward compatibility
LEGACY_HOTEL_MAP: dict[str, str] = {
    f"hotel-{i:03d}": code for i, code in enumerate(HOTEL_CODES, start=1)
}

# Explicit F1 profile mapping ("City Hotel" -> "HP-PT-001", "Resort Hotel" -> "HP-PT-002")
F1_PROFILE_TO_HOTEL: dict[str, dict[str, Any]] = {
    h["f1_profile"]: h for h in HOTELS if "f1_profile" in h
}

CHANNELS_BY_CODE: dict[str, dict[str, Any]] = {ch["code"]: ch for ch in CATALOG["channels"]}
F4_AGENCIES: list[dict[str, Any]] = [
    ch for ch in CATALOG["channels"] if ch.get("source") == "F4" and ch.get("type") == "agency"
]

SEASONS_BY_COUNTRY: dict[str, Any] = {
    k: v for k, v in CATALOG["seasons"].items() if not k.startswith("_")
}
PRICING_RULES: dict[str, Any] = CATALOG["pricing_rules"]
SEASON_MULTIPLIERS: dict[str, float] = PRICING_RULES["season_multipliers"]


def _in_mmdd_range(mmdd: str, start_mmdd: str, end_mmdd: str) -> bool:
    """Evaluate inclusive MM-DD range, supporting year-end wraparound (e.g. 12-15 to 01-15)."""
    if start_mmdd <= end_mmdd:
        return start_mmdd <= mmdd <= end_mmdd
    return mmdd >= start_mmdd or mmdd <= end_mmdd


def get_season_for_date(country_code: str, date_str: str) -> tuple[str, float]:
    """Return (season_code, multiplier) for a given country code and YYYY-MM-DD (or MM-DD) date."""
    mmdd = date_str[-5:] if len(date_str) >= 5 else date_str
    country_seasons = SEASONS_BY_COUNTRY.get(country_code, {})
    for season_code in ("HIGH", "LOW"):
        for start_mmdd, end_mmdd in country_seasons.get(season_code, []):
            if _in_mmdd_range(mmdd, start_mmdd, end_mmdd):
                return season_code, float(SEASON_MULTIPLIERS[season_code])
    return "MID", float(SEASON_MULTIPLIERS["MID"])
