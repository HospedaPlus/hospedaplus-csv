"""
US-25 / US-26 / US-5 — Traditional Agency File Generator (F4).

One file per agency and per ISO week (statement: "CSV/Excel semanal por agencia, ~15 agencias"):

    output/f4_agency_files/2026-W45/AG01_2026-W45.csv
    output/f4_agency_files/2026-W45/AG03_2026-W45.xlsx
    ...
    output/f4_agency_files/2026-W45/_generation_summary.json

Every row carries what operation.reservar() (F3, Neon) needs: agency channel code
(from the file name), the agency booking id (external code), hotel, room type,
guest_ref (G-0000001.., the same pseudonymous guest catalog as F2 and F3), dates,
rooms, adults, children and refundable / non-refundable rate.

Aligned with shared catalog.json (v1.1.0): 15 agencies AG01..AG15 (10% commission),
28 hotels, 4 room types, hotel currency or USD.

Injected defects (US-26):
  - Defect 3: currency left empty (--missing-currency-rate, default 0.10). The amount is
    then in the hotel's currency, so "infer the hotel currency" is the right fix in Silver.
  - Defect 4: cross-channel duplicates (RN-08). A real F2 booking is copied with the SAME
    guest_ref, hotel and room type, the guest name slightly changed and check-in ±1 day.
    The F2 bookings come from Neon (operation.reservations, read-only role) when a
    database URL is given, otherwise from data/f2_reference/events.jsonl (offline/tests).
"""

from __future__ import annotations

import argparse
import json
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from buenavista_csv.catalog import (
    ALLOWED_ROOM_TYPES,
    CATALOG_VERSION,
    F4_AGENCIES,
    HOTEL_CODES,
    HOTELS_BY_CODE,
    LEGACY_HOTEL_MAP,
    ROOM_TYPES,
    get_season_for_date,
)
from buenavista_csv.defects import DEFAULT_DEFECT_RATES
from buenavista_csv.f4_layouts import (
    LAYOUT_A,
    LAYOUT_B,
    WEEK_RE,
    Layout,
    agency_file_name,
    layout_for_agency,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_F2_EVENTS_PATH = REPO_ROOT / "data" / "f2_reference" / "events.jsonl"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "f4_agency_files"

# Same guest pool as F2 events and the F3 seed (G-0000001 .. G-0060000).
GUEST_POOL = 60_000

# Nightly base rates by hotel currency, same values as the F3 seed (seed/catalog.py),
# so agency amounts are in the same range as the rest of the platform.
BASE_RATES = {
    "COP": {"SINGLE": 240_000, "DOUBLE": 320_000, "TWIN": 330_000, "SUITE": 680_000},
    "MXN": {"SINGLE": 1_300, "DOUBLE": 1_700, "TWIN": 1_750, "SUITE": 3_600},
    "BRL": {"SINGLE": 380, "DOUBLE": 500, "TWIN": 520, "SUITE": 1_050},
    "EUR": {"SINGLE": 90, "DOUBLE": 120, "TWIN": 125, "SUITE": 260},
}
RESORT_FACTOR = 1.15
# Units of each hotel currency per 1 USD. Only used to quote USD amounts in the
# simulation; real conversion in Silver/Gold uses the F7 rate of the day.
SIMULATION_UNITS_PER_USD = {"COP": 4_000.0, "MXN": 18.0, "BRL": 5.3, "EUR": 0.92}

NON_REFUNDABLE_RATE = 0.25
TWO_ROOMS_RATE = 0.10
MIN_LEAD_DAYS = 7      # agencies book ahead; also keeps check-in after the load date
MAX_LEAD_DAYS = 120

FIRST_NAMES = [
    "Maria", "Carlos", "Ana", "John", "Laura", "Pedro", "Sofia", "Lucas",
    "Camila", "Diego", "Julia", "Marco", "Elena", "Rafael", "Patricia",
]
LAST_NAMES = [
    "Garcia", "Silva", "Smith", "Lopez", "Santos", "Martinez", "Ferreira",
    "Rossi", "Fernandez", "Gomez", "Pereira", "Costa",
]


def current_iso_week(today: Optional[date] = None) -> str:
    y, w, _ = (today or date.today()).isocalendar()
    return f"{y}-W{w:02d}"


def week_monday(week: str) -> date:
    if not WEEK_RE.match(week):
        raise ValueError(f"week must look like 2026-W45, got {week!r}")
    year, num = week.split("-W")
    return date.fromisocalendar(int(year), int(num), 1)


def guest_ref(number: int) -> str:
    return f"G-{number:07d}"


def _derive_guest_name(key: str) -> str:
    """Deterministic synthetic name for a key (offline mode: names are not in the files we read)."""
    rng = random.Random(key)
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _derive_guest_ref(key: str) -> str:
    return guest_ref(random.Random(f"guest-ref:{key}").randint(1, GUEST_POOL))


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


def stay_amount(hotel_id: str, room_type: str, check_in: date, nights: int, rooms: int) -> float:
    """Price of the stay in the hotel's currency: base rate x resort factor x season, per night."""
    hotel = HOTELS_BY_CODE[hotel_id]
    base = BASE_RATES[hotel["currency"]][room_type]
    if hotel["category"] == "resort":
        base *= RESORT_FACTOR
    total = 0.0
    for i in range(nights):
        night = check_in + timedelta(days=i)
        _, mult = get_season_for_date(hotel["country"], night.isoformat())
        total += base * mult
    total *= rooms
    return round(total) if hotel["currency"] == "COP" else round(total, 2)


def to_usd(amount_local: float, currency: str) -> float:
    return round(amount_local / SIMULATION_UNITS_PER_USD[currency], 2)


# --- reference F2 bookings used to inject duplicates ---------------------------------

@dataclass
class ReferenceBooking:
    """An F2 (OTA) booking an agency may also sell (RN-08)."""
    reservation_id: str
    channel: str
    hotel_id: str
    room_type: str
    guest_ref: str
    guest_name: str
    check_in: str
    check_out: str
    rooms: int = 1
    adults: int = 1
    children: int = 0
    refundable: bool = True


def reference_from_events(f2_events_path: Path) -> list[ReferenceBooking]:
    """Offline source: reservation.created events of the old F2 sample file.
    That file has no guest_ref, so a stable one is derived from reservation_id."""
    if not f2_events_path or not f2_events_path.exists():
        return []
    out = []
    with f2_events_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev.get("event_type") != "reservation.created":
                continue
            data = ev.get("data", {})
            rid = ev["reservation_id"]
            out.append(ReferenceBooking(
                reservation_id=rid,
                channel=data.get("channel", ""),
                hotel_id=_normalize_hotel_code(ev.get("hotel_id", "")),
                room_type=data.get("room_type", "DOUBLE"),
                guest_ref=_derive_guest_ref(rid),
                guest_name=_derive_guest_name(rid),
                check_in=data["check_in"],
                check_out=data["check_out"],
            ))
    return out


NEON_REFERENCE_SQL = """
SELECT r.external_code, c.code, h.code, rt.code, g.guest_ref, g.full_name,
       r.check_in, r.check_out, r.rooms_count, r.adults, r.children, r.is_refundable
  FROM operation.reservations r
  JOIN operation.channels   c  ON c.id  = r.channel_id
  JOIN operation.hotels     h  ON h.id  = r.hotel_id
  JOIN operation.room_types rt ON rt.id = r.room_type_id
  JOIN operation.guests     g  ON g.id  = r.guest_id
 WHERE r.source = 'F2' AND r.status = 'confirmed' AND r.check_in >= %s
 ORDER BY md5(r.code || %s)          -- stable "random" order for a given week
 LIMIT %s
"""


def reference_from_neon(database_url: str, not_before: date, week: str, limit: int) -> list[ReferenceBooking]:
    """Live source: confirmed F2 bookings in Neon (use the read-only etl_reader role)."""
    import psycopg  # optional dependency: uv sync --extra neon

    with psycopg.connect(database_url) as conn:
        rows = conn.execute(NEON_REFERENCE_SQL, (not_before, week, limit)).fetchall()
    return [
        ReferenceBooking(
            reservation_id=r[0], channel=r[1], hotel_id=r[2], room_type=r[3],
            guest_ref=r[4], guest_name=r[5], check_in=r[6].isoformat(),
            check_out=r[7].isoformat(), rooms=r[8], adults=r[9], children=r[10],
            refundable=r[11],
        )
        for r in rows
    ]


# --- rows and files ---------------------------------------------------------------------

@dataclass
class AgencyBooking:
    agency_booking_id: str
    agency_code: str
    guest_ref: str
    guest_name: str
    hotel_id: str
    check_in: str
    check_out: str
    room_type: str
    rooms: int
    adults: int
    children: int
    refundable: bool
    currency: Optional[str]
    total_amount: float
    commission_rate: float
    duplicate_of_reservation_id: Optional[str] = None


def _row_values(layout: Layout, b: AgencyBooking) -> list:
    if layout is LAYOUT_A:
        return [
            b.agency_booking_id, b.guest_ref, b.guest_name, b.hotel_id, b.check_in, b.check_out,
            b.room_type, b.rooms, b.adults, b.children,
            "REFUNDABLE" if b.refundable else "NON_REFUNDABLE",
            b.currency or "", b.total_amount, b.commission_rate,
        ]
    if layout is LAYOUT_B:
        return [
            b.agency_booking_id, b.hotel_id, b.guest_ref, b.guest_name, b.check_in, b.check_out,
            b.room_type, b.rooms, b.adults, b.children, "N" if b.refundable else "Y",
            b.total_amount, b.currency or "", round(b.commission_rate * 100, 2),
        ]
    # Layout C (Excel): real date cells, empty cell for a missing currency
    return [
        b.agency_booking_id, b.guest_ref, b.guest_name, b.hotel_id,
        date.fromisoformat(b.check_in), date.fromisoformat(b.check_out),
        b.room_type, b.rooms, b.adults, b.children,
        "reembolsable" if b.refundable else "no_reembolsable",
        b.total_amount, b.currency, b.commission_rate,
    ]


def _write_file(path: Path, layout: Layout, rows: list[AgencyBooking]) -> None:
    if layout.extension == "csv":
        import csv

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(layout.columns)
            for b in rows:
                writer.writerow(_row_values(layout, b))
        return

    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "reservas"
    ws.append(layout.columns)
    for b in rows:
        ws.append(_row_values(layout, b))
    for col in ("E", "F"):  # fecha_llegada, fecha_salida
        for cell in ws[col][1:]:
            cell.number_format = "yyyy-mm-dd"
    # fixed timestamps so the same seed gives byte-identical files
    wb.properties.created = wb.properties.modified = datetime(2026, 1, 1)
    wb.save(path)


def _random_booking(rng: random.Random, agency_code: str, booking_id: str, monday: date,
                    commission_rate: float, missing_currency_rate: float) -> AgencyBooking:
    hotel_id = rng.choice(HOTEL_CODES)
    room_type = rng.choice(ALLOWED_ROOM_TYPES)
    check_in = monday + timedelta(days=rng.randint(MIN_LEAD_DAYS, MAX_LEAD_DAYS))
    nights = rng.randint(1, 6)
    rooms = 2 if rng.random() < TWO_ROOMS_RATE else 1
    capacity = ROOM_TYPES[room_type]["max_guests"] * rooms
    adults = rng.randint(1, capacity)
    children = rng.randint(0, capacity - adults) if capacity > adults and rng.random() < 0.3 else 0
    n = rng.randint(1, GUEST_POOL)

    hotel_currency = HOTELS_BY_CODE[hotel_id]["currency"]
    amount = stay_amount(hotel_id, room_type, check_in, nights, rooms)
    if rng.random() < missing_currency_rate:
        currency = None                       # Defect 3: amount stays in hotel currency
    elif rng.random() < 0.3:
        currency, amount = "USD", to_usd(amount, hotel_currency)
    else:
        currency = hotel_currency

    return AgencyBooking(
        agency_booking_id=booking_id,
        agency_code=agency_code,
        guest_ref=guest_ref(n),
        guest_name=_derive_guest_name(f"guest:{n}"),
        hotel_id=hotel_id,
        check_in=check_in.isoformat(),
        check_out=(check_in + timedelta(days=nights)).isoformat(),
        room_type=room_type,
        rooms=rooms,
        adults=adults,
        children=children,
        refundable=rng.random() >= NON_REFUNDABLE_RATE,
        currency=currency,
        total_amount=amount,
        commission_rate=commission_rate,
    )


def _duplicate_booking(rng: random.Random, src: ReferenceBooking, agency_code: str,
                       booking_id: str, commission_rate: float) -> AgencyBooking:
    """Same guest_ref, hotel and room as the F2 booking; name slightly changed; check-in ±1."""
    ci = date.fromisoformat(src.check_in)
    co = date.fromisoformat(src.check_out)
    shift = rng.choice([-1, 0, 1])
    new_ci = ci + timedelta(days=shift)
    if new_ci >= co:
        new_ci = ci
    hotel_currency = HOTELS_BY_CODE[src.hotel_id]["currency"]
    return AgencyBooking(
        agency_booking_id=booking_id,
        agency_code=agency_code,
        guest_ref=src.guest_ref,
        guest_name=_vary_name(src.guest_name, rng),
        hotel_id=src.hotel_id,
        check_in=new_ci.isoformat(),
        check_out=src.check_out,
        room_type=src.room_type,
        rooms=src.rooms,
        adults=src.adults,
        children=src.children,
        refundable=src.refundable,
        currency=hotel_currency,
        total_amount=stay_amount(src.hotel_id, src.room_type, new_ci, (co - new_ci).days, src.rooms),
        commission_rate=commission_rate,
        duplicate_of_reservation_id=src.reservation_id,
    )


def generate_agency_files(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    f2_events_path: Path = DEFAULT_F2_EVENTS_PATH,
    week: Optional[str] = None,
    seed: int = 7,
    bookings_per_agency: int = 12,
    duplicate_count: Optional[int] = None,
    duplicate_rate: float = DEFAULT_DEFECT_RATES.duplicate_rate,
    missing_currency_rate: float = DEFAULT_DEFECT_RATES.missing_currency_rate,
    database_url: Optional[str] = None,
) -> dict:
    """Write one file per agency into <output_dir>/<week>/ and return the summary."""
    if not 0.0 <= missing_currency_rate <= 1.0:
        raise ValueError(f"missing_currency_rate must be between 0.0 and 1.0, got {missing_currency_rate}")

    if not 0.0 <= duplicate_rate <= 1.0:
        raise ValueError(f"duplicate_rate must be between 0.0 and 1.0, got {duplicate_rate}")
    if duplicate_count is None:  # statement 5.3: configurable rate over the week's bookings
        duplicate_count = round(duplicate_rate * bookings_per_agency * len(F4_AGENCIES))

    week = week or current_iso_week()
    monday = week_monday(week)
    rng = random.Random(f"{seed}:{week}")
    week_dir = output_dir / week
    week_dir.mkdir(parents=True, exist_ok=True)

    if database_url:
        reference = reference_from_neon(database_url, monday + timedelta(days=2), week, duplicate_count)
        reference_source = "neon:operation.reservations"
    else:
        reference = reference_from_events(f2_events_path)
        reference_source = str(f2_events_path)
    duplicate_sources = rng.sample(reference, k=min(duplicate_count, len(reference)))

    warning = None
    if duplicate_count > len(reference):
        warning = (
            f"Requested {duplicate_count} duplicates but only {len(reference)} F2 bookings "
            f"available in {reference_source}; generated {len(duplicate_sources)}."
        )

    summary = {
        "catalog_version": CATALOG_VERSION,
        "week": week,
        "week_start": monday.isoformat(),
        "seed": seed,
        "duplicate_reference_source": reference_source,
        "agencies": {},
        "duplicates_created": [],
        "warning": warning,
        "missing_currency_rate_configured": missing_currency_rate,
        "duplicate_rate_configured": duplicate_rate,
        "total_rows": 0,
        "rows_missing_currency": 0,
    }
    week_tag = week.replace("-", "")  # 2026W45

    for agency_idx, agency_info in enumerate(F4_AGENCIES, start=1):
        agency_code = agency_info["code"]
        commission_rate = round(float(agency_info["commission_pct"]) / 100.0, 4)
        layout = layout_for_agency(agency_code)
        rows: list[AgencyBooking] = []

        def next_id() -> str:
            return f"{agency_code}-{week_tag}-{len(rows) + 1:04d}"

        for _ in range(bookings_per_agency):
            rows.append(_random_booking(rng, agency_code, next_id(), monday,
                                        commission_rate, missing_currency_rate))

        # duplicates spread round-robin over the agencies
        for src in duplicate_sources[agency_idx - 1::len(F4_AGENCIES)]:
            dup = _duplicate_booking(rng, src, agency_code, next_id(), commission_rate)
            rows.insert(rng.randint(0, len(rows)), dup)
            summary["duplicates_created"].append({
                "agency_code": agency_code,
                "agency_booking_id": dup.agency_booking_id,
                "original_reservation_id": src.reservation_id,
                "original_channel": src.channel,
                "guest_ref": dup.guest_ref,
                "real_guest_name": src.guest_name,
                "agency_guest_name": dup.guest_name,
                "hotel_id": dup.hotel_id,
                "agency_check_in": dup.check_in,
                "original_check_in": src.check_in,
                "check_out": dup.check_out,
            })

        file_path = week_dir / agency_file_name(agency_code, week)
        _write_file(file_path, layout, rows)

        summary["agencies"][agency_code] = {
            "name": agency_info["name"],
            "file": file_path.name,
            "rows": len(rows),
            "layout": layout.name,
        }
        summary["total_rows"] += len(rows)
        summary["rows_missing_currency"] += sum(1 for row in rows if row.currency is None)

    (week_dir / "_generation_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    summary["week_dir"] = str(week_dir)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the weekly F4 agency files (Catalog v1.1.0).")
    parser.add_argument("--week", default=os.getenv("F4_WEEK"), help="ISO week, e.g. 2026-W45 (default: this week)")
    parser.add_argument("--f2-events", type=Path, default=DEFAULT_F2_EVENTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=int(os.getenv("F4_SEED", "7")))
    parser.add_argument("--bookings-per-agency", type=int, default=int(os.getenv("F4_BOOKINGS_PER_AGENCY", "12")))
    parser.add_argument("--duplicate-rate", type=float,
                        default=float(os.getenv("F4_DUPLICATE_RATE", DEFAULT_DEFECT_RATES.duplicate_rate)),
                        help="share of the week's bookings that duplicate an F2 booking (default 0.10)")
    parser.add_argument("--duplicate-count", type=int,
                        default=int(os.environ["F4_DUPLICATE_COUNT"]) if os.getenv("F4_DUPLICATE_COUNT") else None,
                        help="fixed number of duplicates instead of --duplicate-rate")
    parser.add_argument("--missing-currency-rate", type=float,
                        default=float(os.getenv("F4_MISSING_CURRENCY_RATE", DEFAULT_DEFECT_RATES.missing_currency_rate)))
    parser.add_argument("--database-url", default=os.getenv("NEON_READ_URL"),
                        help="Neon URL (etl_reader) to copy real F2 bookings as duplicates. "
                             "Default: env NEON_READ_URL; without it the offline events.jsonl is used.")
    args = parser.parse_args()

    summary = generate_agency_files(
        output_dir=args.output_dir,
        f2_events_path=args.f2_events,
        week=args.week,
        seed=args.seed,
        bookings_per_agency=args.bookings_per_agency,
        duplicate_count=args.duplicate_count,
        duplicate_rate=args.duplicate_rate,
        missing_currency_rate=args.missing_currency_rate,
        database_url=args.database_url,
    )
    print(f"Week {summary['week']}: {len(summary['agencies'])} agency files "
          f"({summary['total_rows']} rows, Catalog v{summary['catalog_version']}) in {summary['week_dir']}")
    print(f"Cross-channel duplicates injected: {len(summary['duplicates_created'])} "
          f"(from {summary['duplicate_reference_source']})")
    print(f"Missing currency rows: {summary['rows_missing_currency']}/{summary['total_rows']}")
    if summary["warning"]:
        print(f"WARNING: {summary['warning']}")


if __name__ == "__main__":
    main()
