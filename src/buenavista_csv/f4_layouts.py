"""
F4 file layouts: how each agency writes its weekly file, and how to read any of them
back into one canonical row.

Agencies send files in three different formats (statement 5.2, "agencias con formatos
distintos"):

  Layout A  CSV, English headers, rate_type REFUNDABLE / NON_REFUNDABLE, commission 0.10
  Layout B  CSV, other English headers, non_refundable Y / N, commission in percent (10)
  Layout C  Excel (.xlsx), Spanish headers, tarifa reembolsable / no_reembolsable

File name: <agency code>_<ISO week>.<csv|xlsx>, e.g. AG07_2026-W45.xlsx, inside a
folder per week. The name says which agency sent it (that is the channel code
operation.reservar() needs) and which week, so loading the same week twice is idempotent.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

# Canonical row: every layout is read into these keys (all values as strings).
CANONICAL_FIELDS = [
    "agency_booking_id", "guest_ref", "guest_name", "hotel_id", "check_in", "check_out",
    "room_type", "rooms", "adults", "children", "refundable", "currency",
    "total_amount", "commission_rate",
]

FILE_NAME_RE = re.compile(r"^(AG\d{2})_(\d{4}-W\d{2})\.(csv|xlsx)$")
WEEK_RE = re.compile(r"^\d{4}-W\d{2}$")


def _refundable_from_rate_type(v: str) -> str:
    return {"REFUNDABLE": "true", "NON_REFUNDABLE": "false"}.get(v.strip().upper(), v)


def _refundable_from_yn(v: str) -> str:  # column says NON-refundable
    return {"N": "true", "Y": "false"}.get(v.strip().upper(), v)


def _refundable_from_tarifa(v: str) -> str:
    return {"reembolsable": "true", "no_reembolsable": "false"}.get(v.strip().lower(), v)


def _commission_from_pct(v: str) -> str:
    try:
        return str(round(float(v) / 100.0, 4))
    except ValueError:
        return v


@dataclass(frozen=True)
class Layout:
    name: str
    extension: str
    columns: list[str]                  # header as the agency writes it
    to_canonical: dict[str, str]        # canonical field -> agency column
    transforms: dict[str, Callable[[str], str]]


LAYOUT_A = Layout(
    name="A",
    extension="csv",
    columns=[
        "agency_booking_id", "guest_ref", "guest_name", "hotel_id", "check_in", "check_out",
        "room_type", "rooms", "adults", "children", "rate_type", "currency",
        "total_amount", "commission_rate",
    ],
    to_canonical={
        "agency_booking_id": "agency_booking_id", "guest_ref": "guest_ref", "guest_name": "guest_name",
        "hotel_id": "hotel_id", "check_in": "check_in", "check_out": "check_out",
        "room_type": "room_type", "rooms": "rooms", "adults": "adults", "children": "children",
        "refundable": "rate_type", "currency": "currency", "total_amount": "total_amount",
        "commission_rate": "commission_rate",
    },
    transforms={"refundable": _refundable_from_rate_type},
)

LAYOUT_B = Layout(
    name="B",
    extension="csv",
    columns=[
        "booking_ref", "hotel_id", "guest_code", "guest_full_name", "checkin_date", "checkout_date",
        "room_category", "num_rooms", "pax_adults", "pax_children", "non_refundable", "amount",
        "currency", "commission_pct",
    ],
    to_canonical={
        "agency_booking_id": "booking_ref", "guest_ref": "guest_code", "guest_name": "guest_full_name",
        "hotel_id": "hotel_id", "check_in": "checkin_date", "check_out": "checkout_date",
        "room_type": "room_category", "rooms": "num_rooms", "adults": "pax_adults",
        "children": "pax_children", "refundable": "non_refundable", "currency": "currency",
        "total_amount": "amount", "commission_rate": "commission_pct",
    },
    transforms={"refundable": _refundable_from_yn, "commission_rate": _commission_from_pct},
)

LAYOUT_C = Layout(
    name="C",
    extension="xlsx",
    columns=[
        "localizador", "cod_huesped", "nombre_huesped", "hotel", "fecha_llegada", "fecha_salida",
        "tipo_habitacion", "habitaciones", "adultos", "ninos", "tarifa", "importe", "moneda",
        "comision",
    ],
    to_canonical={
        "agency_booking_id": "localizador", "guest_ref": "cod_huesped", "guest_name": "nombre_huesped",
        "hotel_id": "hotel", "check_in": "fecha_llegada", "check_out": "fecha_salida",
        "room_type": "tipo_habitacion", "rooms": "habitaciones", "adults": "adultos",
        "children": "ninos", "refundable": "tarifa", "currency": "moneda",
        "total_amount": "importe", "commission_rate": "comision",
    },
    transforms={"refundable": _refundable_from_tarifa},
)

LAYOUTS = {"A": LAYOUT_A, "B": LAYOUT_B, "C": LAYOUT_C}

# Agencies that send Excel. The rest alternate A (even) / B (odd).
EXCEL_AGENCIES = {"AG03", "AG07", "AG11", "AG15"}


def layout_for_agency(agency_code: str) -> Layout:
    if agency_code in EXCEL_AGENCIES:
        return LAYOUT_C
    return LAYOUT_A if int(agency_code[2:]) % 2 == 0 else LAYOUT_B


def agency_file_name(agency_code: str, week: str) -> str:
    return f"{agency_code}_{week}.{layout_for_agency(agency_code).extension}"


def parse_file_name(path: Path) -> tuple[str, str] | None:
    """Return (agency_code, week) from AG07_2026-W45.xlsx, or None if the name is wrong."""
    m = FILE_NAME_RE.match(path.name)
    return (m.group(1), m.group(2)) if m else None


def list_agency_files(directory: Path) -> list[Path]:
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.suffix in (".csv", ".xlsx") and not p.name.startswith("_")
    )


def _cell_to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if v != v:  # NaN
            return ""
        return str(int(v)) if v.is_integer() else str(v)
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, date):
        return v.isoformat()
    return str(v).strip()


def read_raw(path: Path) -> list[list[str]]:
    """Read a CSV or XLSX file as a list of rows (header first), every cell as a string."""
    if path.suffix == ".xlsx":
        from openpyxl import load_workbook

        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb.worksheets[0]
        rows = [[_cell_to_str(c) for c in row] for row in ws.iter_rows(values_only=True)]
        wb.close()
        # drop fully empty trailing rows
        return [r for r in rows if any(c != "" for c in r)]
    with path.open("r", encoding="utf-8", newline="") as f:
        return [row for row in csv.reader(f)]


def detect_layout(header: list[str]) -> Layout | None:
    for layout in LAYOUTS.values():
        if header == layout.columns:
            return layout
    return None


def to_canonical(layout: Layout, raw_row: list[str]) -> dict[str, str]:
    by_col = dict(zip(layout.columns, raw_row))
    out: dict[str, str] = {}
    for field in CANONICAL_FIELDS:
        value = (by_col.get(layout.to_canonical[field]) or "").strip()
        fn = layout.transforms.get(field)
        out[field] = fn(value) if fn and value else value
    return out
