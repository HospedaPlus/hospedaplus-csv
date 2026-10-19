"""
US-25 / US-26 / US-5 — Traditional Agency File Generator (F4).

Aligned with shared catalog.json (v1.1.0):
  - Uses the 15 official agency channels (AG01..AG15) and commission_pct (10% -> 0.10)
  - Uses the 28 official hotel codes (HP-PT-001 .. HP-ES-003)
  - Uses the 4 official room types (SINGLE, DOUBLE, TWIN, SUITE)
  - Uses catalog-approved currencies (hotel local currency or USD accepted from agencies)
  - Injects Defect 3 (--missing-currency-rate, default 0.10) and Defect 4 (cross-channel
    duplicates cloned from F2 reservation.created events with +/-1 day check-in shift).
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from buenavista_csv.catalog import (
    ALLOWED_ROOM_TYPES,
    CATALOG_VERSION,
    F4_AGENCIES,
    HOTEL_CODES,
    HOTELS_BY_CODE,
    LEGACY_HOTEL_MAP,
)
from buenavista_csv.defects import DEFAULT_DEFECT_RATES


DEFAULT_F2_EVENTS_PATH = Path(__file__).resolve().parents[2] / "data" / "f2_reference" / "events.jsonl"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output" / "f4_agency_files"

AGENCY_NAMES = [ag["name"] for ag in F4_AGENCIES]

FIRST_NAMES = [
    "Maria", "Carlos", "Ana", "John", "Laura", "Pedro", "Sofia", "Lucas",
    "Camila", "Diego", "Julia", "Marco", "Elena", "Rafael", "Patricia",
]
LAST_NAMES = [
    "Garcia", "Silva", "Smith", "Lopez", "Santos", "Martinez", "Ferreira",
    "Rossi", "Fernandez", "Gomez", "Pereira", "Costa",
]

LAYOUT_A_COLUMNS = [
    "agency_booking_id", "guest_name", "hotel_id", "check_in", "check_out",
    "room_type", "currency", "total_amount", "commission_rate",
]
LAYOUT_B_COLUMNS = [
    "booking_ref", "hotel_id", "guest_full_name", "checkin_date", "checkout_date",
    "room_category", "amount", "currency", "commission_pct",
]


def _derive_guest_name(reservation_id: str) -> str:
    """Deterministically derive canonical guest name from F2 reservation_id."""
    rng = random.Random(reservation_id)
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _vary_name(name: str, rng: random.Random) -> str:
    """Introduce slight, realistic transcription variation in guest name (Section 5.3)."""
    parts = name.split(" ")
    variation_type = rng.choice(["swap_order", "drop_accent_like", "typo", "middle_initial"])

    if variation_type == "swap_order" and len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    if variation_type == "middle_initial" and len(parts) == 2:
        return f"{parts[0]} {parts[1][0]}."
    if variation_type == "typo" and len(parts[-1]) > 3:
        last = list(parts[-1])
        idx = rng.randint(1, len(last) - 2)
        last[idx], last[idx - 1] = last[idx - 1], last[idx]
        parts[-1] = "".join(last)
        return " ".join(parts)
    return f"{parts[0]} {parts[-1].lower()}"


def _normalize_hotel_code(code: str) -> str:
    """Ensure any legacy 'hotel-001' reference is translated to canonical 'HP-XX-NNN'."""
    return LEGACY_HOTEL_MAP.get(code, code)


@dataclass
class AgencyBooking:
    agency_booking_id: str
    agency_code: str
    agency_name: str
    hotel_id: str
    guest_name: str
    check_in: str
    check_out: str
    room_type: str
    currency: Optional[str]
    total_amount: float
    commission_rate: float
    duplicate_of_reservation_id: Optional[str] = None


def _load_f2_reservations(f2_events_path: Path) -> list[dict]:
    if not f2_events_path.exists():
        return []
    reservations = []
    with f2_events_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev.get("event_type") == "reservation.created":
                ev["hotel_id"] = _normalize_hotel_code(ev.get("hotel_id", ""))
                reservations.append(ev)
    return reservations


def generate_agency_files(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    f2_events_path: Path = DEFAULT_F2_EVENTS_PATH,
    seed: int = 7,
    bookings_per_agency: int = 12,
    duplicate_count: int = 3,
    missing_currency_rate: float = DEFAULT_DEFECT_RATES.missing_currency_rate,
) -> dict:
    if not 0.0 <= missing_currency_rate <= 1.0:
        raise ValueError(f"missing_currency_rate must be between 0.0 and 1.0, got {missing_currency_rate}")

    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    f2_reservations = _load_f2_reservations(f2_events_path)
    duplicate_sources = rng.sample(f2_reservations, k=min(duplicate_count, len(f2_reservations)))

    warning = None
    if not f2_events_path.exists():
        warning = f"The F2 events file does not exist: {f2_events_path}."
    elif not f2_reservations:
        warning = f"The F2 events file exists but contains no 'reservation.created' events: {f2_events_path}."
    elif duplicate_count > len(f2_reservations):
        warning = (
            f"Requested {duplicate_count} duplicates but only {len(f2_reservations)} "
            f"'created' reservations available in F2; generated {len(duplicate_sources)}."
        )

    summary = {
        "catalog_version": CATALOG_VERSION,
        "agencies": {},
        "duplicates_created": [],
        "warning": warning,
        "missing_currency_rate_configured": missing_currency_rate,
        "total_rows": 0,
        "rows_missing_currency": 0,
    }
    booking_counter = 0

    for agency_idx, agency_info in enumerate(F4_AGENCIES, start=1):
        agency_code = agency_info["code"]  # AG01 .. AG15 from catalog.json
        agency_name = agency_info["name"]
        commission_rate = round(float(agency_info["commission_pct"]) / 100.0, 4)  # 10 -> 0.10
        layout = LAYOUT_A_COLUMNS if agency_idx % 2 == 0 else LAYOUT_B_COLUMNS
        rows: list[AgencyBooking] = []

        for _ in range(bookings_per_agency):
            booking_counter += 1
            hotel_id = rng.choice(HOTEL_CODES)
            check_in = datetime(2026, 11, 1) + timedelta(days=rng.randint(0, 90))
            check_out = check_in + timedelta(days=rng.randint(1, 6))
            guest_name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
            room_type = rng.choice(ALLOWED_ROOM_TYPES)
            hotel_currency = HOTELS_BY_CODE[hotel_id]["currency"]
            currency = None if rng.random() < missing_currency_rate else rng.choice([hotel_currency, "USD"])
            total_amount = round(rng.uniform(80, 2000), 2)

            rows.append(
                AgencyBooking(
                    agency_booking_id=f"{agency_code}-{booking_counter:05d}",
                    agency_code=agency_code,
                    agency_name=agency_name,
                    hotel_id=hotel_id,
                    guest_name=guest_name,
                    check_in=check_in.strftime("%Y-%m-%d"),
                    check_out=check_out.strftime("%Y-%m-%d"),
                    room_type=room_type,
                    currency=currency,
                    total_amount=total_amount,
                    commission_rate=commission_rate,
                )
            )

        if duplicate_sources and agency_idx <= len(duplicate_sources):
            source_ev = duplicate_sources[agency_idx - 1]
            booking_counter += 1
            real_name = _derive_guest_name(source_ev["reservation_id"])
            varied_name = _vary_name(real_name, rng)
            data = source_ev["data"]

            dup_row = AgencyBooking(
                agency_booking_id=f"{agency_code}-{booking_counter:05d}",
                agency_code=agency_code,
                agency_name=agency_name,
                hotel_id=_normalize_hotel_code(source_ev["hotel_id"]),
                guest_name=varied_name,
                check_in=(
                    datetime.strptime(data["check_in"], "%Y-%m-%d")
                    + timedelta(days=rng.choice([-1, 0, 1]))
                ).strftime("%Y-%m-%d"),
                check_out=data["check_out"],
                room_type=data["room_type"],
                currency=data["currency"],
                total_amount=data["total_amount"],
                commission_rate=commission_rate,
                duplicate_of_reservation_id=source_ev["reservation_id"],
            )
            rows.append(dup_row)
            summary["duplicates_created"].append({
                "agency_code": agency_code,
                "agency": agency_name,
                "agency_booking_id": dup_row.agency_booking_id,
                "original_reservation_id": source_ev["reservation_id"],
                "original_channel": data["channel"],
                "real_guest_name": real_name,
                "agency_guest_name": varied_name,
                "hotel_id": dup_row.hotel_id,
                "agency_check_in": dup_row.check_in,
                "original_check_in": data["check_in"],
                "check_out": dup_row.check_out,
            })

        file_path = output_dir / f"{agency_name.replace(' ', '_').lower()}_week.csv"
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(layout)
            for row in rows:
                if layout is LAYOUT_A_COLUMNS:
                    writer.writerow([
                        row.agency_booking_id, row.guest_name, row.hotel_id,
                        row.check_in, row.check_out, row.room_type,
                        row.currency or "", row.total_amount, row.commission_rate,
                    ])
                else:
                    writer.writerow([
                        row.agency_booking_id, row.hotel_id, row.guest_name,
                        row.check_in, row.check_out, row.room_type,
                        row.total_amount, row.currency or "", row.commission_rate,
                    ])

        summary["agencies"][agency_name] = {
            "channel_code": agency_code,
            "file": file_path.name,
            "rows": len(rows),
            "layout": "A" if layout is LAYOUT_A_COLUMNS else "B",
        }
        summary["total_rows"] += len(rows)
        summary["rows_missing_currency"] += sum(1 for row in rows if row.currency is None)

    summary_path = output_dir / "_generation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate F4 weekly agency CSV files (Catalog v1.1.0).")
    parser.add_argument("--f2-events", type=Path, default=DEFAULT_F2_EVENTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--bookings-per-agency", type=int, default=12)
    parser.add_argument("--duplicate-count", type=int, default=3)
    parser.add_argument("--missing-currency-rate", type=float, default=DEFAULT_DEFECT_RATES.missing_currency_rate)
    args = parser.parse_args()

    summary = generate_agency_files(
        output_dir=args.output_dir,
        f2_events_path=args.f2_events,
        seed=args.seed,
        bookings_per_agency=args.bookings_per_agency,
        duplicate_count=args.duplicate_count,
        missing_currency_rate=args.missing_currency_rate,
    )
    print(f"Generated {len(summary['agencies'])} F4 agency CSVs ({summary['total_rows']} total rows, Catalog v{summary['catalog_version']}).")
    print(f"Cross-channel duplicates injected: {len(summary['duplicates_created'])}")
    print(f"Missing currency rows: {summary['rows_missing_currency']}/{summary['total_rows']}")


if __name__ == "__main__":
    main()
