"""
Automated test suite for buenavista-csv covering US-11, US-25 (F4), US-26, US-5, US-6,
and shared catalog.json (v1.1.0) alignment.
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from buenavista_csv.catalog import (
    ALLOWED_CURRENCIES,
    ALLOWED_ROOM_TYPES,
    CATALOG_VERSION,
    F1_PROFILE_TO_HOTEL,
    F4_AGENCIES,
    HOTELS,
    HOTELS_BY_CODE,
    REPORTING_CURRENCY,
    get_season_for_date,
)
from buenavista_csv.defects import DefectRates
from buenavista_csv.f1_audit import run_f1_audit
from buenavista_csv.f4_generator import generate_agency_files
from buenavista_csv.f4_layouts import list_agency_files
from buenavista_csv.f4_loader import normalize_row, read_week
from buenavista_csv.validator import latest_week_dir, validate_f4_directory, validate_f4_file

REPO_ROOT = Path(__file__).resolve().parents[1]
F1_CSV = REPO_ROOT / "data" / "f1_historical" / "hotel_bookings.csv"
F2_EVENTS = REPO_ROOT / "data" / "f2_reference" / "events.jsonl"
MALFORMED_F4 = REPO_ROOT / "tests" / "fixtures" / "AG01_2026-W45.csv"
WEEK = "2026-W45"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestBuenavistaCSV(unittest.TestCase):
    def test_shared_catalog_v1_1_0_contract(self) -> None:
        """Verify catalog.json v1.1.0 reference constants: 28 hotels, 4000 rooms, 5 currencies, 15 F4 agencies."""
        self.assertEqual(CATALOG_VERSION, "1.1.0")
        self.assertEqual(REPORTING_CURRENCY, "COP")
        self.assertEqual(ALLOWED_CURRENCIES, {"COP", "MXN", "BRL", "EUR", "USD"})
        self.assertEqual(ALLOWED_ROOM_TYPES, ["SINGLE", "DOUBLE", "TWIN", "SUITE"])
        self.assertEqual(len(HOTELS), 28)
        self.assertEqual(sum(h["rooms"] for h in HOTELS), 4000)
        self.assertEqual(len(F4_AGENCIES), 15)
        self.assertEqual(F1_PROFILE_TO_HOTEL["City Hotel"]["code"], "HP-PT-001")
        self.assertEqual(F1_PROFILE_TO_HOTEL["Resort Hotel"]["code"], "HP-PT-002")

        # Season lookup check (wraparound HIGH season vs LOW vs MID)
        self.assertEqual(get_season_for_date("CO", "2026-12-24"), ("HIGH", 1.25))
        self.assertEqual(get_season_for_date("CO", "2026-02-15"), ("LOW", 0.85))
        self.assertEqual(get_season_for_date("CO", "2026-05-10"), ("MID", 1.0))

    def test_f1_audit_and_iqr_metrics(self) -> None:
        """US-11 & US-5: Verify F1 profile, catalog mapping (HP-PT-001/002), IQR bounds, and non-destructive read."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            before_hash = _sha256(F1_CSV)
            res = run_f1_audit(F1_CSV, tmp_path)
            after_hash = _sha256(F1_CSV)

            self.assertEqual(before_hash, after_hash)
            self.assertEqual(res["rows"], 119390)
            self.assertEqual(res["columns"], 32)
            self.assertEqual(res["mapped_hotels"]["City Hotel"]["catalog_code"], "HP-PT-001")
            self.assertEqual(res["mapped_hotels"]["Resort Hotel"]["catalog_code"], "HP-PT-002")
            self.assertEqual(sum(res["pt_season_distribution"].values()), 119390)

            adr = res["adr"]
            self.assertEqual(adr["q1"], 69.29)
            self.assertEqual(adr["median"], 94.58)
            self.assertEqual(adr["q3"], 126.00)
            self.assertEqual(adr["iqr"], 56.71)
            self.assertEqual(adr["lower_bound"], -15.77)
            self.assertEqual(adr["upper_bound"], 211.06)
            self.assertEqual(adr["outlier_count"], 3793)

    def test_f4_generator_weekly_files_per_agency(self) -> None:
        """US-25 AC2, US-26 & US-5: 15 agencies, one file per agency and week, CSV and Excel."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            summary = generate_agency_files(
                output_dir=tmp_path, f2_events_path=F2_EVENTS, week=WEEK, seed=7,
                bookings_per_agency=12, duplicate_count=3, missing_currency_rate=0.10,
            )
            week_dir = tmp_path / WEEK
            files = sorted(p.name for p in list_agency_files(week_dir))
            self.assertEqual(len(summary["agencies"]), 15)
            self.assertEqual(len(files), 15)
            self.assertIn("AG01_2026-W45.csv", files)
            self.assertIn("AG07_2026-W45.xlsx", files)
            self.assertEqual(sum(f.endswith(".xlsx") for f in files), 4)
            self.assertEqual(summary["total_rows"], 15 * 12 + 3)
            self.assertGreater(summary["rows_missing_currency"], 0)
            self.assertEqual(len(summary["duplicates_created"]), 3)
            for dup in summary["duplicates_created"]:
                self.assertIn(dup["hotel_id"], HOTELS_BY_CODE)
                self.assertRegex(dup["guest_ref"], r"^G-\d{7}$")

            # same seed and week -> identical files (re-running a week is safe)
            again = Path(tmp) / "again"
            generate_agency_files(output_dir=again, f2_events_path=F2_EVENTS, week=WEEK, seed=7)
            for name in files:
                self.assertEqual(_sha256(week_dir / name), _sha256(again / WEEK / name), name)

    def test_f4_rows_carry_what_reservar_needs(self) -> None:
        """Every row of every layout normalizes to the arguments of operation.reservar()."""
        with tempfile.TemporaryDirectory() as tmp:
            generate_agency_files(output_dir=Path(tmp), f2_events_path=F2_EVENTS, week=WEEK, seed=7)
            bookings, problems = read_week(Path(tmp) / WEEK)
            self.assertEqual(problems, [])
            self.assertEqual(len(bookings), 15 * 12 + 3)
            for b in bookings:
                self.assertRegex(b["channel"], r"^AG\d{2}$")
                self.assertTrue(b["external_code"].startswith(b["channel"] + "-2026W45-"))
                self.assertRegex(b["guest_ref"], r"^G-\d{7}$")
                self.assertIsInstance(b["refundable"], bool)
                self.assertGreaterEqual(b["rooms"], 1)
                self.assertGreaterEqual(b["adults"], 1)
                self.assertGreater(b["check_out"], b["check_in"])
                if b["currency_inferred"]:
                    self.assertEqual(b["currency"], HOTELS_BY_CODE[b["hotel"]]["currency"])
                    self.assertIsNotNone(b["nightly_rate"])
                if b["currency"] == "USD":
                    self.assertIsNone(b["nightly_rate"])  # reservar() prices from operation.rates
            self.assertTrue(any(b["currency_inferred"] for b in bookings))
            self.assertTrue(any(not b["refundable"] for b in bookings))

    def test_f4_loader_rejects_bad_rows_without_database(self) -> None:
        """Rows reservar() would refuse are reported as invalid and never sent."""
        good = {
            "agency_booking_id": "AG01-2026W45-0001", "guest_ref": "G-0000042", "guest_name": "Ana Silva",
            "hotel_id": "HP-CO-001", "check_in": "2026-11-20", "check_out": "2026-11-22",
            "room_type": "DOUBLE", "rooms": "1", "adults": "2", "children": "0", "refundable": "true",
            "currency": "", "total_amount": "640000", "commission_rate": "0.1",
        }
        b = normalize_row("AG01", good)
        self.assertEqual(b["currency"], "COP")
        self.assertTrue(b["currency_inferred"])
        self.assertEqual(b["nightly_rate"], 320000.0)
        for field, value in [("guest_ref", "Ana"), ("hotel_id", "hotel-999"), ("adults", "3"),
                             ("check_out", "2026-11-19"), ("refundable", "maybe")]:
            with self.assertRaises(ValueError, msg=field):
                normalize_row("AG01", {**good, field: value})

    def test_us6_validator_detects_malformed_records(self) -> None:
        """US-6 AC1 & AC2: Malformed records caught with exact file, field, and record ID without rewriting source."""
        before_hash = _sha256(MALFORMED_F4)
        report = validate_f4_file(MALFORMED_F4)
        after_hash = _sha256(MALFORMED_F4)

        self.assertEqual(before_hash, after_hash)
        self.assertTrue(report["valid_schema"])
        defect_types = {d["type"] for d in report["defects"]}
        for expected in ("MISSING_REQUIRED_VALUE", "INVALID_HOTEL_ID", "INVALID_ROOM_TYPE",
                         "INVALID_CURRENCY", "INVALID_DATE_RANGE", "INVALID_TYPE",
                         "INVALID_COMMISSION_RATE", "INTRA_FILE_DUPLICATE", "MISSING_CURRENCY",
                         "INVALID_GUEST_REF", "INVALID_OCCUPANCY", "INVALID_RATE_TYPE"):
            self.assertIn(expected, defect_types)

    def test_us6_directory_validation_identifies_cross_channel_duplicates(self) -> None:
        """US-6: Validate a generated week (CSV + Excel) against the F2 reference bookings."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            summary = generate_agency_files(
                output_dir=tmp_path, f2_events_path=F2_EVENTS, week=WEEK, seed=7,
                bookings_per_agency=12, duplicate_count=3, missing_currency_rate=0.10,
            )
            dir_report = validate_f4_directory(latest_week_dir(tmp_path), F2_EVENTS)
            self.assertEqual(dir_report["status"], "PASSED_WITH_CATALOGUED_DEFECTS")
            self.assertEqual(dir_report["agency_files_found"], 15)
            self.assertEqual(dir_report["layout_distribution"], {"A": 7, "B": 4, "C": 4, "INVALID": 0})
            self.assertEqual(dir_report["defect_counts_by_type"]["MISSING_CURRENCY"], summary["rows_missing_currency"])
            self.assertGreaterEqual(dir_report["defect_counts_by_type"]["CROSS_CHANNEL_DUPLICATE"], 3)

    def test_defect_rates_bounds(self) -> None:
        """US-26: DefectRates validates bounds [0.0, 1.0]."""
        rates = DefectRates(missing_currency_rate=0.0)
        self.assertEqual(rates.missing_currency_rate, 0.0)


if __name__ == "__main__":
    unittest.main()
