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
from buenavista_csv.validator import validate_f4_directory, validate_f4_file

REPO_ROOT = Path(__file__).resolve().parents[1]
F1_CSV = REPO_ROOT / "data" / "f1_historical" / "hotel_bookings.csv"
F2_EVENTS = REPO_ROOT / "data" / "f2_reference" / "events.jsonl"
MALFORMED_F4 = REPO_ROOT / "tests" / "fixtures" / "malformed_f4_week.csv"


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

    def test_f4_generator_and_cross_channel_duplicates(self) -> None:
        """US-25 AC2, US-26 & US-5: Verify 15 agencies, HP-XX-NNN hotel codes, and F2 duplicates."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            summary = generate_agency_files(
                output_dir=tmp_path,
                f2_events_path=F2_EVENTS,
                seed=7,
                bookings_per_agency=12,
                duplicate_count=3,
                missing_currency_rate=0.10,
            )
            self.assertEqual(len(summary["agencies"]), 15)
            self.assertEqual(summary["total_rows"], 183)
            self.assertEqual(summary["rows_missing_currency"], 14)
            self.assertEqual(len(summary["duplicates_created"]), 3)
            for dup in summary["duplicates_created"]:
                self.assertIn(dup["hotel_id"], HOTELS_BY_CODE)

    def test_us6_validator_detects_malformed_records(self) -> None:
        """US-6 AC1 & AC2: Malformed records caught with exact file, field, and record ID without rewriting source."""
        before_hash = _sha256(MALFORMED_F4)
        report = validate_f4_file(MALFORMED_F4)
        after_hash = _sha256(MALFORMED_F4)

        self.assertEqual(before_hash, after_hash)
        self.assertTrue(report["valid_schema"])
        defect_types = {d["type"] for d in report["defects"]}
        self.assertIn("MISSING_REQUIRED_VALUE", defect_types)
        self.assertIn("INVALID_HOTEL_ID", defect_types)
        self.assertIn("INVALID_ROOM_TYPE", defect_types)
        self.assertIn("INVALID_CURRENCY", defect_types)
        self.assertIn("INVALID_DATE_RANGE", defect_types)
        self.assertIn("INVALID_TYPE", defect_types)
        self.assertIn("INVALID_COMMISSION_RATE", defect_types)
        self.assertIn("INTRA_FILE_DUPLICATE", defect_types)
        self.assertIn("MISSING_CURRENCY", defect_types)

    def test_us6_directory_validation_identifies_cross_channel_duplicates(self) -> None:
        """US-6: Validate generated F4 directory against F2 reference events."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            generate_agency_files(
                output_dir=tmp_path,
                f2_events_path=F2_EVENTS,
                seed=7,
                bookings_per_agency=12,
                duplicate_count=3,
                missing_currency_rate=0.10,
            )
            dir_report = validate_f4_directory(tmp_path, F2_EVENTS)
            self.assertEqual(dir_report["status"], "PASSED_WITH_CATALOGUED_DEFECTS")
            self.assertEqual(dir_report["agency_files_found"], 15)
            self.assertEqual(dir_report["defect_counts_by_type"]["MISSING_CURRENCY"], 14)
            self.assertEqual(dir_report["defect_counts_by_type"]["CROSS_CHANNEL_DUPLICATE"], 3)

    def test_defect_rates_bounds(self) -> None:
        """US-26: DefectRates validates bounds [0.0, 1.0]."""
        rates = DefectRates(missing_currency_rate=0.0)
        self.assertEqual(rates.missing_currency_rate, 0.0)


if __name__ == "__main__":
    unittest.main()
