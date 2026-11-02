"""
US-6 — Non-Destructive File Quality Controls and Batch Validation Readiness (F1 & F4).

Aligned with shared catalog.json (v1.1.0):
  - Validates hotel_id against the 28 canonical hotel codes (HP-PT-001 .. HP-ES-003)
  - Validates room_type against catalog room_types (SINGLE, DOUBLE, TWIN, SUITE)
  - Validates currency against catalog currencies (COP, MXN, BRL, EUR, USD)
  - Validates agency booking prefix against catalog F4 channels (AG01 .. AG15)
  - Reports schema mismatches, missing required values, duplicates, invalid types,
    and business anomalies without modifying raw CSV files.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
import pandas as pd

from buenavista_csv.catalog import (
    ALLOWED_CURRENCIES,
    ALLOWED_ROOM_TYPES,
    CATALOG_VERSION,
    F4_AGENCIES,
    HOTELS_BY_CODE,
)
from buenavista_csv.catalog import ROOM_TYPES
from buenavista_csv.f4_generator import ReferenceBooking, reference_from_events
from buenavista_csv.f4_layouts import (
    WEEK_RE,
    detect_layout,
    list_agency_files,
    parse_file_name,
    read_raw,
    to_canonical,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_F1_PATH = REPO_ROOT / "data" / "f1_historical" / "hotel_bookings.csv"
DEFAULT_F4_DIR = REPO_ROOT / "output" / "f4_agency_files"
DEFAULT_F2_PATH = REPO_ROOT / "data" / "f2_reference" / "events.jsonl"
DEFAULT_REPORT_DIR = REPO_ROOT / "output" / "validation_reports"

EXPECTED_F1_COLUMNS = [
    "hotel", "is_canceled", "lead_time", "arrival_date_year", "arrival_date_month",
    "arrival_date_week_number", "arrival_date_day_of_month", "stays_in_weekend_nights",
    "stays_in_week_nights", "adults", "children", "babies", "meal", "country",
    "market_segment", "distribution_channel", "is_repeated_guest",
    "previous_cancellations", "previous_bookings_not_canceled", "reserved_room_type",
    "assigned_room_type", "booking_changes", "deposit_type", "agent", "company",
    "days_in_waiting_list", "customer_type", "adr", "required_car_parking_spaces",
    "total_of_special_requests", "reservation_status", "reservation_status_date",
]

ALLOWED_ROOM_TYPES_SET = set(ALLOWED_ROOM_TYPES)
VALID_AGENCY_CODES = {a["code"] for a in F4_AGENCIES}
GUEST_REF_RE = re.compile(r"^G-\d{7}$")


def validate_f1_file(csv_path: Path) -> dict[str, Any]:
    """Validate F1 CSV non-destructively and return structured anomaly report."""
    if not csv_path.exists():
        return {
            "source": "F1",
            "catalog_version": CATALOG_VERSION,
            "file": str(csv_path),
            "valid_schema": False,
            "status": "FAILED",
            "errors": [{"type": "FILE_NOT_FOUND", "field": None, "detail": f"File not found: {csv_path}"}],
        }

    df_raw = pd.read_csv(csv_path, keep_default_na=False, low_memory=False)
    actual_cols = list(df_raw.columns)
    missing_cols = [c for c in EXPECTED_F1_COLUMNS if c not in actual_cols]
    extra_cols = [c for c in actual_cols if c not in EXPECTED_F1_COLUMNS]

    defects: list[dict[str, Any]] = []
    if missing_cols or extra_cols:
        defects.append({
            "type": "SCHEMA_MISMATCH",
            "file": csv_path.name,
            "field": ",".join(missing_cols + extra_cols),
            "record": "header",
            "detail": f"Missing columns: {missing_cols}; Unexpected columns: {extra_cols}",
        })
        return {
            "source": "F1",
            "catalog_version": CATALOG_VERSION,
            "file": csv_path.name,
            "rows": len(df_raw),
            "valid_schema": False,
            "status": "FAILED_SCHEMA",
            "defects": defects,
        }

    adr_series = pd.to_numeric(df_raw["adr"], errors="coerce")
    invalid_adr_rows = df_raw[adr_series.isna()].index.tolist()
    for idx in invalid_adr_rows[:20]:
        defects.append({
            "type": "INVALID_TYPE",
            "file": csv_path.name,
            "field": "adr",
            "record": f"row_{idx + 2}",
            "detail": f"Non-numeric ADR value: '{df_raw.loc[idx, 'adr']}'",
        })

    valid_adr = adr_series.dropna()
    q1 = float(valid_adr.quantile(0.25)) if len(valid_adr) else 0.0
    q3 = float(valid_adr.quantile(0.75)) if len(valid_adr) else 0.0
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    neg_adr_indices = df_raw[adr_series < 0].index.tolist()
    for idx in neg_adr_indices[:20]:
        defects.append({
            "type": "NEGATIVE_ADR",
            "file": csv_path.name,
            "field": "adr",
            "record": f"row_{idx + 2}",
            "detail": f"Negative ADR value: {adr_series.loc[idx]}",
        })

    zero_adr_count = int((adr_series == 0).sum())

    adults_num = pd.to_numeric(df_raw["adults"], errors="coerce").fillna(-1)
    children_num = pd.to_numeric(df_raw["children"].replace({"NA": "0", "NULL": "0", "": "0"}), errors="coerce").fillna(0)
    babies_num = pd.to_numeric(df_raw["babies"], errors="coerce").fillna(0)

    zero_guest_mask = (adults_num == 0) & (children_num == 0) & (babies_num == 0)
    zero_guest_indices = df_raw[zero_guest_mask].index.tolist()
    for idx in zero_guest_indices[:10]:
        defects.append({
            "type": "ZERO_GUESTS_ANOMALY",
            "file": csv_path.name,
            "field": "adults,children,babies",
            "record": f"row_{idx + 2}",
            "detail": "Booking has 0 adults, 0 children, and 0 babies (Defect 1)",
        })

    textual_null_counts = {
        col: int((df_raw[col] == "NULL").sum())
        for col in ["company", "agent", "country"]
    }
    textual_na_children = int((df_raw["children"] == "NA").sum())
    exact_duplicate_rows = int(df_raw.duplicated().sum())

    status = "PASSED_WITH_KNOWN_ANOMALIES" if not invalid_adr_rows else "FAILED_VALIDATION"

    return {
        "source": "F1",
        "catalog_version": CATALOG_VERSION,
        "file": csv_path.name,
        "rows": len(df_raw),
        "valid_schema": True,
        "status": status,
        "summary_metrics": {
            "exact_duplicate_rows": exact_duplicate_rows,
            "textual_null_counts": textual_null_counts,
            "textual_na_children": textual_na_children,
            "zero_guest_rows": len(zero_guest_indices),
            "negative_adr_rows": len(neg_adr_indices),
            "zero_adr_rows": zero_adr_count,
            "iqr_outlier_rows": int(((adr_series < lower_bound) | (adr_series > upper_bound)).sum()),
            "iqr_bounds": {"q1": round(q1, 2), "q3": round(q3, 2), "iqr": round(iqr, 2), "lower": round(lower_bound, 2), "upper": round(upper_bound, 2)},
            "invalid_adr_type_rows": len(invalid_adr_rows),
        },
        "sample_defects": defects,
    }


def _normalize_name_tokens(name: str) -> set[str]:
    cleaned = re.sub(r"[^a-zA-Z\s]", "", name.lower()).split()
    return set(cleaned)


def _is_fuzzy_name_match(name_a: str, name_b: str) -> bool:
    if name_a.lower().strip() == name_b.lower().strip():
        return True
    tokens_a = _normalize_name_tokens(name_a)
    tokens_b = _normalize_name_tokens(name_b)
    if tokens_a == tokens_b and len(tokens_a) > 0:
        return True
    parts_a = name_a.lower().replace(".", "").split()
    parts_b = name_b.lower().replace(".", "").split()
    if len(parts_a) >= 2 and len(parts_b) >= 2 and parts_a[0] == parts_b[0]:
        s1, s2 = parts_a[-1], parts_b[-1]
        if len(s2) == 1 and s1.startswith(s2):
            return True
        if len(s1) == len(s2) and sorted(s1) == sorted(s2):
            return True
    return False


def _defect(dtype: str, file: str, field: str | None, record: str | None, detail: str) -> dict[str, Any]:
    return {"type": dtype, "file": file, "field": field, "record": record, "detail": detail}


def validate_f4_file(path: Path, reference: list[ReferenceBooking] | None = None) -> dict[str, Any]:
    """Validate one weekly agency file (CSV or XLSX, layout A/B/C) non-destructively."""
    name = path.name
    if not path.exists():
        return {"file": str(path), "valid_schema": False, "layout": None, "rows": 0,
                "defects": [_defect("FILE_NOT_FOUND", name, None, None, "File does not exist")]}

    raw = read_raw(path)
    if not raw:
        return {"file": name, "valid_schema": False, "layout": None, "rows": 0,
                "defects": [_defect("EMPTY_FILE", name, "header", "row_1", "File is empty")]}

    defects: list[dict[str, Any]] = []
    parsed = parse_file_name(path)
    agency_code = parsed[0] if parsed else None
    if parsed is None or agency_code not in VALID_AGENCY_CODES:
        defects.append(_defect("INVALID_FILE_NAME", name, "file_name", None,
                               "Expected <AG01..AG15>_<YYYY-Www>.csv|xlsx"))

    layout = detect_layout(raw[0])
    if layout is None:
        defects.append(_defect("SCHEMA_MISMATCH", name, "header", "row_1",
                               f"Header {raw[0]} does not match layout A, B or C."))
        return {"file": name, "valid_schema": False, "layout": None, "rows": len(raw) - 1, "defects": defects}

    seen_booking_ids: set[str] = set()
    data_rows = raw[1:]
    n_cols = len(layout.columns)

    for idx, raw_row in enumerate(data_rows, start=2):
        if len(raw_row) != n_cols:
            defects.append(_defect("COLUMN_COUNT_MISMATCH", name, "row", f"row_{idx}",
                                   f"Expected {n_cols} columns, found {len(raw_row)}"))
            continue
        row = to_canonical(layout, raw_row)
        booking_id = row["agency_booking_id"]
        rec_id = booking_id or f"row_{idx}"

        if not booking_id:
            defects.append(_defect("MISSING_REQUIRED_VALUE", name, "agency_booking_id", rec_id, "Missing booking ID"))
        elif booking_id in seen_booking_ids:
            defects.append(_defect("INTRA_FILE_DUPLICATE", name, "agency_booking_id", rec_id, f"Duplicate booking ID {booking_id}"))
        elif agency_code and not booking_id.startswith(agency_code):
            defects.append(_defect("BOOKING_ID_AGENCY_MISMATCH", name, "agency_booking_id", rec_id,
                                   f"Booking ID does not start with {agency_code}"))
        seen_booking_ids.add(booking_id)

        if not GUEST_REF_RE.match(row["guest_ref"]):
            defects.append(_defect("INVALID_GUEST_REF", name, "guest_ref", rec_id,
                                   f"guest_ref '{row['guest_ref']}' is not G-0000000 (needed by operation.reservar())"))
        if not row["guest_name"]:
            defects.append(_defect("MISSING_REQUIRED_VALUE", name, "guest_name", rec_id, "Missing guest name"))
        if row["hotel_id"] not in HOTELS_BY_CODE:
            defects.append(_defect("INVALID_HOTEL_ID", name, "hotel_id", rec_id,
                                   f"Hotel code '{row['hotel_id']}' not in catalog.json (expected HP-XX-NNN)"))
        if row["room_type"] not in ALLOWED_ROOM_TYPES_SET:
            defects.append(_defect("INVALID_ROOM_TYPE", name, "room_type", rec_id, f"Invalid room_type '{row['room_type']}'"))
        if row["refundable"] not in ("true", "false"):
            defects.append(_defect("INVALID_RATE_TYPE", name, "refundable", rec_id, f"Unknown rate type '{row['refundable']}'"))

        # Defect 3: missing currency or currency not in catalog.json
        if not row["currency"]:
            defects.append(_defect("MISSING_CURRENCY", name, "currency", rec_id,
                                   "Currency code is empty (Section 5.3 Defect 3)"))
        elif row["currency"] not in ALLOWED_CURRENCIES:
            defects.append(_defect("INVALID_CURRENCY", name, "currency", rec_id,
                                   f"Unsupported currency '{row['currency']}' (allowed: {sorted(ALLOWED_CURRENCIES)})"))

        ci_dt = None
        try:
            ci_dt = datetime.strptime(row["check_in"], "%Y-%m-%d")
            co_dt = datetime.strptime(row["check_out"], "%Y-%m-%d")
            if co_dt <= ci_dt:
                defects.append(_defect("INVALID_DATE_RANGE", name, "check_in,check_out", rec_id,
                                       f"check_out ({row['check_out']}) <= check_in ({row['check_in']}) (Section 5.3 Defect 2)"))
        except ValueError:
            defects.append(_defect("INVALID_TYPE", name, "check_in,check_out", rec_id,
                                   f"Unparseable dates: check_in='{row['check_in']}', check_out='{row['check_out']}'"))

        ints = {}
        for field in ("rooms", "adults", "children"):
            try:
                ints[field] = int(float(row[field]))
            except ValueError:
                defects.append(_defect("INVALID_TYPE", name, field, rec_id, f"Non-numeric {field} '{row[field]}'"))
        if len(ints) == 3 and row["room_type"] in ROOM_TYPES:
            if ints["rooms"] < 1 or ints["adults"] < 1 or ints["children"] < 0:
                defects.append(_defect("INVALID_OCCUPANCY", name, "rooms,adults,children", rec_id,
                                       f"rooms={ints['rooms']} adults={ints['adults']} children={ints['children']}"))
            elif ints["adults"] + ints["children"] > ROOM_TYPES[row["room_type"]]["max_guests"] * ints["rooms"]:
                defects.append(_defect("INVALID_OCCUPANCY", name, "adults,children", rec_id,
                                       f"{ints['adults'] + ints['children']} guests do not fit in {ints['rooms']} {row['room_type']}"))

        try:
            amt = float(row["total_amount"])
            if amt <= 0:
                defects.append(_defect("NON_POSITIVE_AMOUNT", name, "total_amount", rec_id, f"Amount must be > 0, got {amt}"))
        except ValueError:
            defects.append(_defect("INVALID_TYPE", name, "total_amount", rec_id, f"Non-numeric amount '{row['total_amount']}'"))

        try:
            comm = float(row["commission_rate"])
            if not 0.0 <= comm <= 1.0:
                defects.append(_defect("INVALID_COMMISSION_RATE", name, "commission_rate", rec_id,
                                       f"Commission rate out of [0,1]: {comm}"))
        except ValueError:
            defects.append(_defect("INVALID_TYPE", name, "commission_rate", rec_id,
                                   f"Non-numeric commission '{row['commission_rate']}'"))

        # Defect 4 / RN-08: same guest_ref (or a near-identical name) + hotel + check-in ±1 day
        if reference and ci_dt is not None:
            for ref in reference:
                if ref.hotel_id != row["hotel_id"]:
                    continue
                ref_ci = datetime.strptime(ref.check_in, "%Y-%m-%d")
                if abs((ci_dt - ref_ci).days) > 1:
                    continue
                if ref.guest_ref == row["guest_ref"] or _is_fuzzy_name_match(row["guest_name"], ref.guest_name):
                    defects.append(_defect(
                        "CROSS_CHANNEL_DUPLICATE", name, "guest_ref,hotel_id,check_in", rec_id,
                        f"Cross-channel duplicate of F2 {ref.reservation_id} ({ref.channel}): guest {row['guest_ref']} "
                        f"'{row['guest_name']}' vs '{ref.guest_name}', check_in {row['check_in']} vs {ref.check_in} (RN-08)",
                    ))
                    break

    return {
        "file": name,
        "valid_schema": True,
        "layout": layout.name,
        "rows": len(data_rows),
        "defect_count": len(defects),
        "defects": defects,
    }


def latest_week_dir(base: Path) -> Path:
    """output/f4_agency_files -> its newest <YYYY-Www> subfolder (or base itself if it has none)."""
    weeks = sorted(p for p in base.iterdir() if p.is_dir() and WEEK_RE.match(p.name)) if base.exists() else []
    return weeks[-1] if weeks else base


def validate_f4_directory(agency_dir: Path, f2_events_path: Path | None = DEFAULT_F2_PATH,
                          reference: list[ReferenceBooking] | None = None) -> dict[str, Any]:
    """Validate all agency files of one week (CSV and XLSX)."""
    files = list_agency_files(agency_dir) if agency_dir.exists() else []
    if reference is None:
        reference = reference_from_events(f2_events_path) if f2_events_path else []

    file_reports = []
    all_defects = []
    total_rows = 0
    layout_counts = {"A": 0, "B": 0, "C": 0, "INVALID": 0}
    expected_count = len(F4_AGENCIES)

    for fpath in files:
        rep = validate_f4_file(fpath, reference=reference)
        file_reports.append(rep)
        total_rows += rep["rows"]
        layout_counts[rep["layout"] or "INVALID"] += 1
        all_defects.extend(rep["defects"])

    defect_counts_by_type: dict[str, int] = {}
    for d in all_defects:
        defect_counts_by_type[d["type"]] = defect_counts_by_type.get(d["type"], 0) + 1

    structural_errors = sum(
        defect_counts_by_type.get(k, 0)
        for k in (
            "SCHEMA_MISMATCH", "COLUMN_COUNT_MISMATCH", "INVALID_TYPE", "INVALID_FILE_NAME",
            "MISSING_REQUIRED_VALUE", "INVALID_DATE_RANGE", "INVALID_HOTEL_ID", "INVALID_GUEST_REF",
            "INVALID_ROOM_TYPE", "INVALID_CURRENCY", "INVALID_COMMISSION_RATE", "INVALID_RATE_TYPE",
            "INVALID_OCCUPANCY", "BOOKING_ID_AGENCY_MISMATCH",
        )
    )
    status = "PASSED_WITH_CATALOGUED_DEFECTS" if structural_errors == 0 and len(files) == expected_count else "FAILED_VALIDATION"

    return {
        "source": "F4",
        "catalog_version": CATALOG_VERSION,
        "directory": str(agency_dir),
        "agency_files_found": len(files),
        "expected_agency_files": expected_count,
        "total_rows": total_rows,
        "layout_distribution": layout_counts,
        "status": status,
        "defect_counts_by_type": defect_counts_by_type,
        "defects": all_defects,
        "files": file_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run US-6 non-destructive quality checks on F1 and F4.")
    parser.add_argument("--f1-path", type=Path, default=DEFAULT_F1_PATH)
    parser.add_argument("--f4-dir", type=Path, default=DEFAULT_F4_DIR)
    parser.add_argument("--f2-events", type=Path, default=DEFAULT_F2_PATH)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    args = parser.parse_args()

    args.report_dir.mkdir(parents=True, exist_ok=True)
    f1_res = validate_f1_file(args.f1_path)
    f4_res = validate_f4_directory(latest_week_dir(args.f4_dir), args.f2_events)

    combined = {"catalog_version": CATALOG_VERSION, "f1_validation": f1_res, "f4_validation": f4_res}
    out_file = args.report_dir / "quality_validation_report.json"
    out_file.write_text(json.dumps(combined, indent=2, ensure_ascii=False), encoding="utf-8")

    print("=" * 70)
    print(f"Catalog Version:      v{CATALOG_VERSION}")
    print(f"F1 Validation Status: {f1_res['status']} ({f1_res.get('rows', 0):,} rows)")
    print(f"F4 Validation Status: {f4_res['status']} ({f4_res['agency_files_found']} files, {f4_res['total_rows']} rows)")
    print(f"F4 Defects by Type:   {f4_res['defect_counts_by_type']}")
    print(f"Report written to:    {out_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
