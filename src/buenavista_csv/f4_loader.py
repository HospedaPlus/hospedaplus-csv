"""
F4 loader: reads one week of agency files (any layout, CSV or Excel), normalizes every
row to the canonical shape and books it in Neon through operation.reservar(), the same
function F2 and F3 use. That keeps one write path for inventory, so agency sales also
respect the overbooking limit and get rejected when the hotel is full (RN-03, RN-04).

    uv run -m buenavista_csv.f4_loader --week-dir output/f4_agency_files/2026-W45
    uv run -m buenavista_csv.f4_loader --week-dir ... --dry-run     # normalize only, no DB

Database: env NEON_DATABASE_URL (role app_writer).

Per row:
  - Bad rows (unknown hotel, bad dates, guests that do not fit...) are reported as
    "invalid" and never sent to the database.
  - Missing currency (Defect 3): the hotel currency is used and currency_inferred = true.
  - Amount: if the agency billed in the hotel currency, its nightly rate is passed to
    reservar(); if it billed in USD, reservar() prices the stay from operation.rates
    (USD -> hotel currency is a Silver/Gold job with F7 rates).
  - reservar() is idempotent on (agency code, agency booking id): loading the same week
    again returns "already_exists" and changes nothing.

Each row runs in its own transaction, so one failing row does not undo the others.
The report (<week-dir>/_load_report.json) lists the result of every row.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from buenavista_csv.catalog import ALLOWED_ROOM_TYPES, CHANNELS_BY_CODE, HOTELS_BY_CODE, ROOM_TYPES
from buenavista_csv.f4_layouts import (
    detect_layout,
    list_agency_files,
    parse_file_name,
    read_raw,
    to_canonical,
)

GUEST_REF_PREFIX = "G-"

RESERVAR_SQL = """
SELECT result, reservation_code, reservation_id, nights_without_room
  FROM operation.reservar(
       p_channel => %(channel)s::text, p_external_code => %(external_code)s::text,
       p_hotel => %(hotel)s::text, p_room_type => %(room_type)s::text,
       p_guest_ref => %(guest_ref)s::text,
       p_check_in => %(check_in)s::date, p_check_out => %(check_out)s::date,
       p_rooms => %(rooms)s::int, p_adults => %(adults)s::int,
       p_refundable => %(refundable)s::boolean, p_children => %(children)s::int,
       p_nightly_rate => %(nightly_rate)s::numeric,
       p_occurred_at => %(occurred_at)s::timestamptz)
"""


def _is_guest_ref(v: str) -> bool:
    return len(v) == 9 and v.startswith(GUEST_REF_PREFIX) and v[2:].isdigit()


def normalize_row(agency_code: str, row: dict[str, str]) -> dict[str, Any]:
    """Canonical strings -> typed booking. Raises ValueError with the reason if the row is unusable."""
    errors = []
    booking_id = row["agency_booking_id"]
    if not booking_id:
        errors.append("missing agency_booking_id")
    guest = row["guest_ref"]
    if not _is_guest_ref(guest):
        errors.append(f"guest_ref {guest!r} is not G-0000000")
    hotel = HOTELS_BY_CODE.get(row["hotel_id"])
    if hotel is None:
        errors.append(f"unknown hotel {row['hotel_id']!r}")
    room_type = row["room_type"]
    if room_type not in ALLOWED_ROOM_TYPES:
        errors.append(f"unknown room type {room_type!r}")

    try:
        check_in = date.fromisoformat(row["check_in"])
        check_out = date.fromisoformat(row["check_out"])
        if check_out <= check_in:
            errors.append(f"check_out {check_out} must be after check_in {check_in}")
    except ValueError:
        check_in = check_out = None
        errors.append(f"bad dates {row['check_in']!r} / {row['check_out']!r}")

    def as_int(field: str, default: Optional[int] = None) -> int:
        v = row[field]
        if v == "" and default is not None:
            return default
        try:
            return int(float(v))
        except ValueError:
            errors.append(f"{field} {v!r} is not a number")
            return 0

    rooms = as_int("rooms", 1)
    adults = as_int("adults", 1)
    children = as_int("children", 0)
    if rooms < 1 or adults < 1 or children < 0:
        errors.append(f"rooms={rooms} adults={adults} children={children}")
    elif room_type in ROOM_TYPES and adults + children > ROOM_TYPES[room_type]["max_guests"] * rooms:
        errors.append(f"{adults + children} guests do not fit in {rooms} {room_type}")

    refundable = {"true": True, "false": False}.get(row["refundable"].lower())
    if refundable is None:
        errors.append(f"unknown rate type {row['refundable']!r}")

    try:
        total_amount = float(row["total_amount"])
        if total_amount <= 0:
            errors.append(f"amount must be > 0, got {total_amount}")
    except ValueError:
        total_amount = None
        errors.append(f"amount {row['total_amount']!r} is not a number")

    if errors:
        raise ValueError("; ".join(errors))

    currency = row["currency"] or hotel["currency"]
    currency_inferred = not row["currency"]
    nights = (check_out - check_in).days
    nightly_rate = (
        round(total_amount / nights / rooms, 2) if currency == hotel["currency"] else None
    )
    return {
        "channel": agency_code,
        "external_code": booking_id,
        "hotel": hotel["code"],
        "room_type": room_type,
        "guest_ref": guest,
        "check_in": check_in,
        "check_out": check_out,
        "rooms": rooms,
        "adults": adults,
        "children": children,
        "refundable": refundable,
        "currency": currency,
        "currency_inferred": currency_inferred,
        "total_amount": total_amount,
        "nightly_rate": nightly_rate,
    }


def read_week(week_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read every agency file of a week. Returns (bookings, problems)."""
    bookings, problems = [], []
    for path in list_agency_files(week_dir):
        parsed = parse_file_name(path)
        if parsed is None or parsed[0] not in CHANNELS_BY_CODE:
            problems.append({"file": path.name, "row": None, "status": "invalid",
                             "reason": "file name is not <AGxx>_<YYYY-Www>.csv|xlsx"})
            continue
        agency_code, _ = parsed
        raw = read_raw(path)
        layout = detect_layout(raw[0]) if raw else None
        if layout is None:
            problems.append({"file": path.name, "row": None, "status": "invalid",
                             "reason": "header does not match layout A, B or C"})
            continue
        for n, raw_row in enumerate(raw[1:], start=2):
            row = to_canonical(layout, raw_row)
            try:
                b = normalize_row(agency_code, row)
            except ValueError as exc:
                problems.append({"file": path.name, "row": n, "status": "invalid",
                                 "external_code": row["agency_booking_id"], "reason": str(exc)})
                continue
            b["file"], b["row"] = path.name, n
            bookings.append(b)
    return bookings, problems


def book(conn, b: dict[str, Any], occurred_at: datetime) -> dict[str, Any]:
    """Call operation.reservar() for one booking in its own transaction."""
    import psycopg

    params = {k: b[k] for k in ("channel", "external_code", "hotel", "room_type", "guest_ref",
                                "check_in", "check_out", "rooms", "adults", "refundable",
                                "children", "nightly_rate")}
    params["occurred_at"] = occurred_at
    try:
        with conn.transaction():
            result, code, _rid, full = conn.execute(RESERVAR_SQL, params).fetchone()
        out = {"status": result, "reservation_code": code}
        if full:
            out["nights_without_room"] = [d.isoformat() for d in full]
        return out
    except psycopg.Error as exc:
        return {"status": "error", "reason": str(exc).splitlines()[0]}


def load_week(week_dir: Path, database_url: Optional[str], dry_run: bool = False) -> dict[str, Any]:
    bookings, problems = read_week(week_dir)
    occurred_at = datetime.now(timezone.utc)
    results = []

    if dry_run:
        results = [{"file": b["file"], "row": b["row"], "external_code": b["external_code"],
                    "status": "ready", "currency_inferred": b["currency_inferred"]} for b in bookings]
    else:
        import psycopg

        if not database_url:
            raise SystemExit("Set NEON_DATABASE_URL (app_writer) or use --dry-run")
        with psycopg.connect(database_url, autocommit=True) as conn:
            for b in bookings:
                r = book(conn, b, occurred_at)
                results.append({"file": b["file"], "row": b["row"], "external_code": b["external_code"],
                                "currency_inferred": b["currency_inferred"], **r})

    all_rows = problems + results
    counts: dict[str, int] = {}
    for r in all_rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    report = {
        "week_dir": str(week_dir),
        "loaded_at": occurred_at.isoformat(),
        "dry_run": dry_run,
        "files": len(list_agency_files(week_dir)),
        "rows": len(all_rows),
        "currency_inferred_rows": sum(1 for b in bookings if b["currency_inferred"]),
        "counts": counts,
        "rows_detail": all_rows,
    }
    (week_dir / "_load_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Book one week of F4 agency files in Neon via operation.reservar().")
    parser.add_argument("--week-dir", type=Path, required=True, help="Folder with AGxx_<week>.csv|xlsx files")
    parser.add_argument("--database-url", default=os.getenv("NEON_DATABASE_URL"))
    parser.add_argument("--dry-run", action="store_true", help="Only read and normalize; no database")
    args = parser.parse_args()

    report = load_week(args.week_dir, args.database_url, args.dry_run)
    print(f"{report['files']} files, {report['rows']} rows -> {report['counts']}")
    print(f"Currency inferred from hotel: {report['currency_inferred_rows']} rows")
    print(f"Report: {args.week_dir / '_load_report.json'}")


if __name__ == "__main__":
    main()
