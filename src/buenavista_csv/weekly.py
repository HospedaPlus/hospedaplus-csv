"""
Weekly F4 runner for `docker compose up -d`.

Every week (default: Monday 06:00 UTC) it does, for the current ISO week:
  1. generate the 15 agency files      (f4_generator)
  2. validate them                     (validator; stops if there are structural errors)
  3. book them in Neon via reservar()  (f4_loader; only if NEON_DATABASE_URL is set)

On start it also runs the current week if that week was not generated yet, so the first
`docker compose up -d` already produces files. A week that already has its files is not
generated again (same seed + week = same files, so nothing would change anyway), and the
loader is idempotent, so restarts are safe.

Env: F4_RUN_WEEKDAY (0=Mon), F4_RUN_HOUR_UTC (6), plus the generator/loader variables.
Upload to S3 is not done here: that is the pipeline's job (Airflow) once it exists.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from buenavista_csv.f4_generator import DEFAULT_OUTPUT_DIR, current_iso_week, generate_agency_files
from buenavista_csv.f4_loader import load_week
from buenavista_csv.validator import validate_f4_directory


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z] {msg}", flush=True)


def run_week(week: str, output_dir: Path) -> None:
    week_dir = output_dir / week
    if (week_dir / "_generation_summary.json").exists():
        log(f"{week}: files already exist, not generating again")
    else:
        s = generate_agency_files(
            output_dir=output_dir,
            week=week,
            seed=int(os.getenv("F4_SEED", "7")),
            bookings_per_agency=int(os.getenv("F4_BOOKINGS_PER_AGENCY", "12")),
            duplicate_count=int(os.environ["F4_DUPLICATE_COUNT"]) if os.getenv("F4_DUPLICATE_COUNT") else None,
            duplicate_rate=float(os.getenv("F4_DUPLICATE_RATE", "0.10")),
            missing_currency_rate=float(os.getenv("F4_MISSING_CURRENCY_RATE", "0.10")),
            database_url=os.getenv("NEON_READ_URL") or None,
        )
        log(f"{week}: {s['total_rows']} rows in {len(s['agencies'])} files, "
            f"{len(s['duplicates_created'])} duplicates from {s['duplicate_reference_source']}")

    report = validate_f4_directory(week_dir, f2_events_path=None)
    log(f"{week}: validation {report['status']} {report['defect_counts_by_type']}")
    if report["status"] != "PASSED_WITH_CATALOGUED_DEFECTS":
        log(f"{week}: not loading into Neon because validation failed")
        return

    if os.getenv("NEON_DATABASE_URL"):
        r = load_week(week_dir, os.environ["NEON_DATABASE_URL"])
        log(f"{week}: Neon load {r['counts']}")
    else:
        log(f"{week}: NEON_DATABASE_URL not set, skipping the Neon load")


def next_run(now: datetime, weekday: int, hour: int) -> datetime:
    candidate = (now + timedelta(days=(weekday - now.weekday()) % 7)).replace(
        hour=hour, minute=0, second=0, microsecond=0)
    return candidate if candidate > now else candidate + timedelta(days=7)


def main() -> None:
    output_dir = Path(os.getenv("F4_OUTPUT_DIR", DEFAULT_OUTPUT_DIR))
    weekday = int(os.getenv("F4_RUN_WEEKDAY", "0"))
    hour = int(os.getenv("F4_RUN_HOUR_UTC", "6"))

    run_week(current_iso_week(), output_dir)
    while True:
        at = next_run(datetime.now(timezone.utc), weekday, hour)
        log(f"next run {at:%Y-%m-%d %H:%M}Z")
        time.sleep(max(1.0, (at - datetime.now(timezone.utc)).total_seconds()))
        try:
            run_week(current_iso_week(), output_dir)
        except Exception as exc:  # keep the service alive; the next week retries
            log(f"run failed: {exc!r}")


if __name__ == "__main__":
    main()
