# buenavista-csv

Standalone repository for the file-based data sources (**F1** and **F4**) of the **HospedaPlus Revenue** platform (`US-5` & `US-6`), standardized against the shared platform reference catalog (`schemas/catalog.json`, **v1.1.0**). Decoupled from `buenavista-streaming` (F2), `buenavista-simulated-db` (F3), and `buenavista-api` (F5/F6/F7).

## 1. Data Sources Scope & Catalog v1.1.0 Alignment

| ID | Source | Type | Frequency | Catalog v1.1.0 Mapping & Scope | Provenance / License |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **F1** | Hotel Booking Demand | Historical CSV | One-time / Backfill | 119,390 rows × 32 cols mapped to `HP-PT-001` (*City Hotel*, 207 rooms, `EUR`) & `HP-PT-002` (*Resort Hotel*, 171 rooms, `EUR`) | [Kaggle Dataset](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand) (`CC BY 4.0`) |
| **F4** | Traditional Agency Files | Simulated CSV / Excel, one file per agency and week | Weekly Batch | 15 agencies (`AG01`..`AG15`, `10%` commission), 28 hotels, room types (`SINGLE`, `DOUBLE`, `TWIN`, `SUITE`), currencies (`COP`, `MXN`, `BRL`, `EUR`, `USD`), guests `G-0000001`.. shared with F2/F3. Loaded into Neon through `operation.reservar()` | Synthetic generator (`seed=7`) |

## 2. Repository Structure

```text
buenavista-csv/
├── pyproject.toml                  # Package configuration & dependencies
├── .env.example                    # Environment variables template
├── schemas/
│   ├── catalog.json                # Shared platform reference catalog (v1.1.0)
│   ├── f1_schema.json              # Canonical 32-column contract & IQR bounds for F1
│   └── f4_layouts_schema.json      # Layouts A/B (CSV) and C (Excel) contract for F4
├── data/
│   ├── f1_historical/
│   │   └── hotel_bookings.csv      # F1 raw Kaggle dataset
│   └── f2_reference/
│       └── events.jsonl            # Reference F2 events aligned with HP-XX-NNN hotels
├── src/buenavista_csv/
│   ├── catalog.py                  # Shared catalog.json loader & season/currency helpers
│   ├── defects.py                  # Unified DefectRates configuration (US-14 / US-26)
│   ├── f1_audit.py                 # F1 schema profiler, PT season mapper & IQR auditor (US-11)
│   ├── f4_layouts.py               # The three agency formats + canonical reader
│   ├── f4_generator.py             # F4 weekly CSV/Excel generator (US-25 / US-26)
│   ├── f4_loader.py                # Books F4 rows in Neon via operation.reservar()
│   └── validator.py                # Non-destructive batch quality validator (US-6)
├── output/                         # Generated audit, agency CSVs, and validation reports
├── tests/                          # Unit tests & malformed CSV fixtures (US-6 AC2)
└── docs/data-sources/              # Full Docusaurus technical documentation
```

## 3. Requirements & Setup

* **Python:** `>=3.11`
* **Package Manager:** `uv` (recommended) or `pip`

```bash
git clone <repo-url>
cd buenavista-csv
uv sync --extra test --extra neon
```

## 4. Usage & Validation Commands

Run all commands from the root of `buenavista-csv`:

```bash
# 1. Profile F1 schema, PT season distribution, null tokens, and ADR outliers via IQR (US-11 / US-5)
uv run -m buenavista_csv.f1_audit

# 2. Generate one week of F4 files: 15 agencies, 11 CSV + 4 Excel (US-25 / US-26)
#    Add --database-url $NEON_READ_URL (etl_reader) to copy real F2 bookings from Neon as duplicates
uv run -m buenavista_csv.f4_generator --week 2026-W45 --seed 7 --bookings-per-agency 12 --duplicate-count 3 --missing-currency-rate 0.10

# 3. Run non-destructive quality checks on F1 and the latest F4 week (US-6)
uv run -m buenavista_csv.validator

# 4. Book the week in Neon through operation.reservar() (needs `uv sync --extra neon`)
uv run -m buenavista_csv.f4_loader --week-dir output/f4_agency_files/2026-W45 --dry-run   # no database
NEON_DATABASE_URL=postgresql://app_writer:...@.../hospedaplus \
  uv run -m buenavista_csv.f4_loader --week-dir output/f4_agency_files/2026-W45

# 5. Run automated unit tests
uv run python -m unittest discover -s tests -v
```

### Run every week with Docker

```bash
cp .env.example .env          # optional: NEON_READ_URL (etl_reader) and NEON_DATABASE_URL (app_writer)
docker compose up -d --build
docker compose logs -f f4-weekly
```

The `f4-weekly` service generates, validates and (if `NEON_DATABASE_URL` is set) books the current week as soon as it starts, then again every Monday 06:00 UTC (`F4_RUN_WEEKDAY`, `F4_RUN_HOUR_UTC`). Files are written to `./output/f4_agency_files/<YYYY-Www>/` on the host. Restarting is safe: an existing week is not regenerated and `reservar()` is idempotent. Uploading to S3 is left to the Airflow pipeline.

## 5. Key Outputs

* **F1 Audit Report:** `output/f1_audit/audit_report.csv` and `output/f1_audit/audit_summary.txt`
* **F4 Agency Files:** `output/f4_agency_files/<YYYY-Www>/<AGxx>_<YYYY-Www>.csv` / `.xlsx` (15 files) and `_generation_summary.json`
* **F4 Load Report:** `output/f4_agency_files/<YYYY-Www>/_load_report.json` (result of `reservar()` per row; not committed)
* **Quality Validation Report:** `output/validation_reports/quality_validation_report.json`

## 6. Documentation

Detailed architectural role, Medallion (`Bronze` → `Silver` → `Gold`) consumption contracts, shared catalog (`v1.1.0`) mappings, IQR outlier mathematics, and business rules (`RN-01`, `RN-02`, `RN-03`, `RN-05`, `RN-06`, `RN-08`, `RN-11`) are documented in `docs/data-sources/index.md` for the shared Docusaurus site.
