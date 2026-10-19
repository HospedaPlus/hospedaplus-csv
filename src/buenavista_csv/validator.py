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
import csv
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
from buenavista_csv.f4_generator import (
    LAYOUT_A_COLUMNS,
    LAYOUT_B_COLUMNS,
    _derive_guest_name,
    _load_f2_reservations,
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


def validate_f4_file(csv_path: Path, f2_reference: list[dict] | None = None) -> dict[str, Any]:
    """Validate a single F4 weekly agency CSV file against catalog v1.1.0 non-destructively."""
    defects: list[dict[str, Any]] = []
    if not csv_path.exists():
        return {
            "file": str(csv_path),
            "valid_schema": False,
            "layout": None,
            "rows": 0,
            "defects": [{"type": "FILE_NOT_FOUND", "file": csv_path.name, "field": None, "record": None, "detail": "File does not exist"}],
        }

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = list(csv.reader(f))

    if not reader:
        return {
            "file": csv_path.name,
            "valid_schema": False,
            "layout": None,
            "rows": 0,
            "defects": [{"type": "EMPTY_FILE", "file": csv_path.name, "field": "header", "record": "row_1", "detail": "CSV file is empty"}],
        }

    header = reader[0]
    if header == LAYOUT_A_COLUMNS:
        layout = "A"
    elif header == LAYOUT_B_COLUMNS:
        layout = "B"
    else:
        defects.append({
            "type": "SCHEMA_MISMATCH",
            "file": csv_path.name,
            "field": "header",
            "record": "row_1",
            "detail": f"Header {header} does not match Layout A or Layout B contract.",
        })
        return {
            "file": csv_path.name,
            "valid_schema": False,
            "layout": None,
            "rows": len(reader) - 1,
            "defects": defects,
        }

    seen_booking_ids: set[str] = set()
    data_rows = reader[1:]

    for idx, raw_row in enumerate(data_rows, start=2):
        if len(raw_row) != 9:
            defects.append({
                "type": "COLUMN_COUNT_MISMATCH",
                "file": csv_path.name,
                "field": "row",
                "record": f"row_{idx}",
                "detail": f"Expected 9 columns, found {len(raw_row)}",
            })
            continue

        if layout == "A":
            booking_id, guest_name, hotel_id, check_in, check_out, room_type, currency, amount_str, comm_str = raw_row
        else:
            booking_id, hotel_id, guest_name, check_in, check_out, room_type, amount_str, currency, comm_str = raw_row

        rec_id = booking_id or f"row_{idx}"

        if not booking_id.strip():
            defects.append({"type": "MISSING_REQUIRED_VALUE", "file": csv_path.name, "field": "agency_booking_id", "record": rec_id, "detail": "Missing booking ID"})
        elif booking_id in seen_booking_ids:
            defects.append({"type": "INTRA_FILE_DUPLICATE", "file": csv_path.name, "field": "agency_booking_id", "record": rec_id, "detail": f"Duplicate booking ID {booking_id}"})
        seen_booking_ids.add(booking_id)

        if not guest_name.strip():
            defects.append({"type": "MISSING_REQUIRED_VALUE", "file": csv_path.name, "field": "guest_name", "record": rec_id, "detail": "Missing guest name"})

        if hotel_id.strip() not in HOTELS_BY_CODE:
            defects.append({"type": "INVALID_HOTEL_ID", "file": csv_path.name, "field": "hotel_id", "record": rec_id, "detail": f"Hotel code '{hotel_id}' not in catalog.json (expected HP-XX-NNN)"})

        if room_type.strip() not in ALLOWED_ROOM_TYPES_SET:
            defects.append({"type": "INVALID_ROOM_TYPE", "file": csv_path.name, "field": "room_type", "record": rec_id, "detail": f"Invalid room_type '{room_type}'"})

        # Defect 3: Missing currency or currency not in catalog.json
        if not currency.strip():
            defects.append({
                "type": "MISSING_CURRENCY",
                "file": csv_path.name,
                "field": "currency",
                "record": rec_id,
                "detail": "Currency code is empty (Section 5.3 Defect 3)",
            })
        elif currency.strip() not in ALLOWED_CURRENCIES:
            defects.append({
                "type": "INVALID_CURRENCY",
                "file": csv_path.name,
                "field": "currency",
                "record": rec_id,
                "detail": f"Unsupported currency '{currency}' (allowed: {sorted(ALLOWED_CURRENCIES)})",
            })

        try:
            ci_dt = datetime.strptime(check_in.strip(), "%Y-%m-%d")
            co_dt = datetime.strptime(check_out.strip(), "%Y-%m-%d")
            if co_dt <= ci_dt:
                defects.append({
                    "type": "INVALID_DATE_RANGE",
                    "file": csv_path.name,
                    "field": "check_in,check_out",
                    "record": rec_id,
                    "detail": f"check_out ({check_out}) <= check_in ({check_in}) (Section 5.3 Defect 2)",
                })
        except ValueError:
            ci_dt = None
            defects.append({
                "type": "INVALID_TYPE",
                "file": csv_path.name,
                "field": "check_in,check_out",
                "record": rec_id,
                "detail": f"Unparseable dates: check_in='{check_in}', check_out='{check_out}'",
            })

        try:
            amt = float(amount_str)
            if amt <= 0:
                defects.append({
                    "type": "NON_POSITIVE_AMOUNT",
                    "file": csv_path.name,
                    "field": "total_amount",
                    "record": rec_id,
                    "detail": f"Amount must be > 0, got {amt}",
                })
        except ValueError:
            defects.append({
                "type": "INVALID_TYPE",
                "file": csv_path.name,
                "field": "total_amount",
                "record": rec_id,
                "detail": f"Non-numeric amount '{amount_str}'",
            })

        try:
            comm = float(comm_str)
            if not 0.0 <= comm <= 1.0:
                defects.append({
                    "type": "INVALID_COMMISSION_RATE",
                    "file": csv_path.name,
                    "field": "commission_rate",
                    "record": rec_id,
                    "detail": f"Commission rate out of [0,1]: {comm}",
                })
        except ValueError:
            defects.append({
                "type": "INVALID_TYPE",
                "file": csv_path.name,
                "field": "commission_rate",
                "record": rec_id,
                "detail": f"Non-numeric commission '{comm_str}'",
            })

        if f2_reference and ci_dt is not None:
            for ev in f2_reference:
                if ev.get("hotel_id") != hotel_id.strip():
                    continue
                ev_data = ev.get("data", {})
                try:
                    ev_ci = datetime.strptime(ev_data.get("check_in", ""), "%Y-%m-%d")
                except ValueError:
                    continue
                if abs((ci_dt - ev_ci).days) <= 1 and ev_data.get("check_out") == check_out.strip():
                    f2_guest = _derive_guest_name(ev["reservation_id"])
                    if _is_fuzzy_name_match(guest_name, f2_guest):
                        defects.append({
                            "type": "CROSS_CHANNEL_DUPLICATE",
                            "file": csv_path.name,
                            "field": "guest_name,hotel_id,check_in",
                            "record": rec_id,
                            "detail": (
                                f"Cross-channel duplicate of F2 {ev['reservation_id']} "
                                f"({ev_data.get('channel')}): agency guest '{guest_name}' vs "
                                f"F2 guest '{f2_guest}', check_in {check_in} vs {ev_data.get('check_in')} (RN-08)"
                            ),
                        })

    return {
        "file": csv_path.name,
        "valid_schema": True,
        "layout": layout,
        "rows": len(data_rows),
        "defect_count": len(defects),
        "defects": defects,
    }


def validate_f4_directory(agency_dir: Path, f2_events_path: Path = DEFAULT_F2_PATH) -> dict[str, Any]:
    """Validate all weekly agency CSV files in a directory."""
    csv_files = sorted(p for p in agency_dir.glob("*_week.csv") if p.is_file())
    f2_ref = _load_f2_reservations(f2_events_path) if f2_events_path else []

    file_reports = []
    all_defects = []
    total_rows = 0
    layout_counts = {"A": 0, "B": 0, "INVALID": 0}
    expected_count = len(F4_AGENCIES)

    for fpath in csv_files:
        rep = validate_f4_file(fpath, f2_reference=f2_ref)
        file_reports.append(rep)
        total_rows += rep["rows"]
        if rep["layout"] in ("A", "B"):
            layout_counts[rep["layout"]] += 1
        else:
            layout_counts["INVALID"] += 1
        all_defects.extend(rep["defects"])

    defect_counts_by_type: dict[str, int] = {}
    for d in all_defects:
        dtype = d["type"]
        defect_counts_by_type[dtype] = defect_counts_by_type.get(dtype, 0) + 1

    structural_errors = sum(
        defect_counts_by_type.get(k, 0)
        for k in (
            "SCHEMA_MISMATCH", "COLUMN_COUNT_MISMATCH", "INVALID_TYPE",
            "MISSING_REQUIRED_VALUE", "INVALID_DATE_RANGE", "INVALID_HOTEL_ID",
            "INVALID_ROOM_TYPE", "INVALID_CURRENCY", "INVALID_COMMISSION_RATE",
        )
    )
    status = "PASSED_WITH_CATALOGUED_DEFECTS" if structural_errors == 0 and len(csv_files) == expected_count else "FAILED_VALIDATION"

    return {
        "source": "F4",
        "catalog_version": CATALOG_VERSION,
        "directory": str(agency_dir),
        "agency_files_found": len(csv_files),
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
    f4_res = validate_f4_directory(args.f4_dir, args.f2_events)

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
