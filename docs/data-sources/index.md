--- 
id: file-sources-f1-f4 
title: "File-Based Data Sources: F1 (Historical Demand) & F4 (Traditional Agencies)" 
sidebar_label: "F1 & F4: File Sources (CSV)" 
sidebar_position: 2 
description: "Comprehensive technical specification, architectural role, shared catalog (v1.1.0) alignment, data profiling, IQR outlier mathematics, defect injection contracts, and batch consumption strategy for HospedaPlus Revenue sources F1 and F4." 
tags: \[data-sources, f1, f4, batch-layer, medallion-bronze, data-quality, spark, airflow, catalog\]
---
# File-Based Data Sources: F1 (Historical Demand) & F4 (Traditional Agencies)

| Attribute | Specification | 
| ----- | ----- | 
| **Canonical Repository** | `buenavista-csv` | 
| **Domain Owner** | Oscar Corzo (Data Engineering — File Sources Domain) | 
| **Shared Reference Contract** | `schemas/catalog.json` (**Catalog Version `1.1.0`** — synchronized across `F1`–`F7`) | 
| **Covered Sources** | **F1** (`hotel_bookings.csv` — Kaggle Historical CSV) and **F4** (`<AGxx>_<YYYY-Www>.csv` / `.xlsx` — Simulated Traditional Agency Files) | 
| **Architectural Layer** | **Batch Layer** (Daily / Weekly Scheduled Ingestion → Amazon S3 `Bronze` → PySpark `Silver` & `Gold`) | 
| **Related User Stories** | `US-11` (F1 Exploration & IQR Audit), `US-14` (Defects Catalog), `US-24` (F2 Event Coupling), `US-25` (F4 Generator), `US-26` (Configurable Defect Injection), `US-5` (Repo Separation), `US-6` (Quality Controls & Batch Readiness) | 
| **Business Rules Supported** | `RN-01` (Occupancy, Net ADR, Net RevPAR), `RN-02` (Channel Commissions), `RN-03` (90-Day Cancellation Baseline for Overbooking), `RN-05` (Season & 14-Day YoY Pickup Multipliers), `RN-06` (Event Multiplier `1.15x`), `RN-08` (Cross-Channel Deduplication ±1 Day), `RN-11` (Guest PII Pseudonymization) | 
| **Runtime Status** | `Done` — Audited, Catalog-Aligned (`v1.1.0`), Deterministically Seeded, and Batch-Ready | 

:::info Repository Boundary & Shared Catalog Contract (US-5, US-6 & Catalog v1.1.0) Per the team's domain separation architecture, `buenavista-csv` exclusively houses standalone CSV/Excel data sources (**F1** and **F4**), their canonical JSON schemas, deterministic file generators, and non-destructive data-quality validators. It operates with **zero runtime coupling** to the FastAPI guest reviews service (**F5**, `buenavista-api`), the RabbitMQ streaming broker (**F2**, `buenavista-streaming`), or the operational PostgreSQL database (**F3**, `buenavista-simulated-db`), while enforcing 100% referential parity through the shared `schemas/catalog.json` (`v1.1.0`) specification. :::

## 1. Architectural Role and Cross-Source Standardization (`catalog.json` v1.1.0)

HospedaPlus Revenue implements a hybrid **Lambda + Medallion Architecture** on AWS (`us-east-2`) to reconcile two competing operational demands: sub-250 ms real-time inventory protection (`RNF-01`, `RNF-02`) and deep historical recalculation of dynamic rates and overbooking ceilings before **06:00 local hotel time** (`RNF-03`).

Within this topology, **F1** and **F4** serve as the foundational file-based inputs of the **Batch Layer**, governed by the platform-wide reference catalog (`schemas/catalog.json`, `v1.1.0`):

| Catalog Dimension | Standardized Specification (`catalog_version: 1.1.0`) | Impact on `buenavista-csv` (`F1` & `F4`) | 
| ----- | ----- | ----- | 
| **Countries & Hotel Currencies** | `CO` (`COP`), `MX` (`MXN`), `BR` (`BRL`), `PT` (`EUR`), `ES` (`EUR`) | Defines each hotel's native billing currency (`hotels[].currency`) and local timezone (`America/Bogota`, `Europe/Lisbon`, etc.). | 
| **Agency & Reporting Currencies** | `USD` (*"accepted from agencies (F4); not a hotel currency"*); `reporting_currency = "COP"` | `F4` agency rows bill in either the hotel's native currency (`COP`, `MXN`, `BRL`, `EUR`) or `USD`. All metrics convert to `COP` in Silver/Gold via `F7`. | 
| **28 Standardized Hotels** | `HP-PT-001`..`003` (3), `HP-CO-001`..`010` (10), `HP-MX-001`..`006` (6), `HP-BR-001`..`006` (6), `HP-ES-001`..`003` (3) — **4,000 rooms total** | Replaces legacy `hotel-001` IDs. Explicitly maps F1's `"City Hotel"` → **`HP-PT-001`** (207 rooms) and `"Resort Hotel"` → **`HP-PT-002`** (171 rooms). | 
| **Room Types** | `SINGLE` (max 1), `DOUBLE` (max 2), `TWIN` (max 2), `SUITE` (max 4) | Enforced across all `F4` agency exports (`room_type` / `room_category`) and validated by `buenavista_csv.validator`. | 
| **Channels & Commissions (`RN-02`)** | **F3 Direct/Corp (`0%`)**: `direct_web`, `corporate` | **F2 OTA (`18%`)**: `booking_channel_a`, `booking_channel_b`, `expedia`, `direct_ota` | **F4 Agencies (`10%`)**: `AG01`..`AG15` | Maps all 15 traditional agencies to canonical channel codes `AG01` (`ViajaFacil`) through `AG15` (`Viajes Amanecer`) with `commission_pct = 10` (`0.10`). | 
| **Country Seasons & Pricing (`RN-05`, `RN-06`)** | Per-country `MM-DD` ranges for `HIGH` (`1.25x`), `MID` (`1.00x`), and `LOW` (`0.85x`); `event_multiplier = 1.15x`; `pickup` (`14d` window, `±15%` threshold → `1.10x` / `0.92x`) | Used by `buenavista_csv.f1_audit` to classify F1 historical bookings into Portugal (`PT`) seasonal buckets and by Spark Silver/Gold for dynamic pricing. | 

### End-to-End Data Flow across the Medallion Architecture

<img width="1536" height="1024" alt="CSV-Arquitecture" src="https://github.com/user-attachments/assets/3626d21d-4a87-4049-9fcd-dae6559fc2db" />


## 2. Source F1: Hotel Booking Demand (`hotel_bookings.csv`) Profiles Historical Seasonality and Cancellation Risk

### Provenance, Licensing, and Catalog v1.1.0 Property Mapping

| Metadata Field | Value / Specification | 
| ----- | ----- | 
| **Source ID & Name** | **F1** — Hotel Booking Demand Dataset (`DS1` in Sprint 0 backlog) | 
| **Upstream Origin** | [Kaggle — Hotel Booking Demand](https://www.google.com/search?q=https%3A%2F%2Fwww.kaggle.com%2Fdatasets%2Fjessemostipak%2Fhotel-booking-demand) (Originally curated by Nuno Antonio, Ana de Almeida, and Luis Nunes, *Data in Brief*, 2019) | 
| **License** | Creative Commons Attribution 4.0 International (`CC BY 4.0`) | 
| **File Path in Repo** | `data/f1_historical/hotel_bookings.csv` | 
| **File Format & Encoding** | Comma-separated values (`.csv`), `UTF-8` encoding, header row on line 1 | 
| **Dataset Volume** | **119,390 rows** × **32 columns** (**16.86 MB** on disk; **104.83 MB** in-memory DataFrame footprint) | 
| **Temporal Coverage** | Arrival dates spanning **July 2015 through August 2017** (`2015`: 21,996 rows; `2016`: 56,707 rows; `2017`: 40,687 rows) | 
| **Catalog `v1.1.0` Hotel Mapping** | • `"City Hotel"` (**79,330 rows**, 66.45%) → **`HP-PT-001`** (*HospedaPlus Lisboa City*, Urban, Lisbon, `PT`, `Europe/Lisbon`, `EUR`, **207 rooms**)  • `"Resort Hotel"` (**40,060 rows**, 33.55%) → **`HP-PT-002`** (*HospedaPlus Algarve Resort*, Resort, Faro, `PT`, `Europe/Lisbon`, `EUR`, **171 rooms**) | 
| **Catalog `v1.1.0` PT Season Profile (`RN-05`)** | • **`HIGH` Season (`1.25x` — `06-15..09-15`, `12-20..01-05`)**: **41,027 rows (`34.36%`)**  • **`MID` Season (`1.00x` — all unlisted dates)**: **44,885 rows (`37.60%`)**  • **`LOW` Season (`0.85x` — `11-01..12-19`, `01-06..03-31`)**: **33,478 rows (`28.04%`)** | 
| **Ingestion Frequency** | Historical one-time seed into S3 `Bronze` (`f1_historical/`), with idempotent partition overwrite support for backfills (`CT-08`) | 

### Complete 32-Column Canonical Schema and Audit Profile (US-11)

The table below reflects the exact output produced by `buenavista_csv.f1_audit` (`output/f1_audit/audit_report.csv`) across all **119,390 records**:

| \# | Column Name | Pandas Dtype | PySpark Target Type | Non-Null Count | Null / Missing Count | Null % | Unique Values | Sample Values & Catalog Mapping Notes | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| 1 | `hotel` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 2 | `City Hotel` (`HP-PT-001`), `Resort Hotel` (`HP-PT-002`) | 
| 2 | `is_canceled` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 2 | `0` (75,166 active), `1` (44,224 canceled/no-show; 37.04%) | 
| 3 | `lead_time` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 479 | `0` to `737` days between booking and arrival (`RN-03`) | 
| 4 | `arrival_date_year` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 3 | `2015`, `2016`, `2017` | 
| 5 | `arrival_date_month` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 12 | `January` .. `December` (mapped to `MM-DD` for `PT` seasons) | 
| 6 | `arrival_date_week_number` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 53 | ISO week `1` .. `53` | 
| 7 | `arrival_date_day_of_month` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 31 | Day `1` .. `31` | 
| 8 | `stays_in_weekend_nights` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 17 | `0`, `1`, `2`, up to `19` weekend nights | 
| 9 | `stays_in_week_nights` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 35 | `0`, `1`, `2`, up to `50` weekday nights | 
| 10 | `adults` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 14 | `1`, `2`, `3`, `0` (includes 180 zero-guest anomalies) | 
| 11 | `children` | `float64` | `IntegerType` | 119,386 | **4** | **0.00%** | 5 | `0.0`, `1.0`, `2.0`, `10.0` (4 literal `"NA"` tokens in raw CSV) | 
| 12 | `babies` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 5 | `0`, `1`, `2`, `9`, `10` | 
| 13 | `meal` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 5 | `BB` (Bed & Breakfast), `HB`, `FB`, `SC`, `Undefined` | 
| 14 | `country` | `str` (`object`) | `StringType` | 118,902 | **488** | **0.41%** | 177 | ISO-3 guest origin codes (`PRT`, `GBR`, `FRA`, `ESP`); 488 `"NULL"` | 
| 15 | `market_segment` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 8 | `Online TA` (56,477), `Offline TA/TO` (24,219), `Groups` (19,811), `Direct` (12,606), `Corporate` (5,295), `Complementary` (743), `Aviation` (237), `Undefined` (2) | 
| 16 | `distribution_channel` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 5 | `TA/TO` (97,870), `Direct` (14,645), `Corporate` (6,677), `GDS` (193), `Undefined` (5) | 
| 17 | `is_repeated_guest` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 2 | `0` (first-time), `1` (repeated guest) | 
| 18 | `previous_cancellations` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 15 | Historical cancellations prior to current booking | 
| 19 | `previous_bookings_not_canceled` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 73 | Historical non-canceled bookings prior to current booking | 
| 20 | `reserved_room_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 10 | Anonymized codes `A`..`P` (mapped in Silver to `SINGLE`, `DOUBLE`, `TWIN`, `SUITE`) | 
| 21 | `assigned_room_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 12 | Codes `A`..`P` (differs from reserved on upgrade/overbooking) | 
| 22 | `booking_changes` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 21 | Number of modifications (`0` .. `21`) | 
| 23 | `deposit_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 3 | `No Deposit` (104,641), `Non Refund` (14,587), `Refundable` (162) | 
| 24 | `agent` | `float64` | `StringType` / `IntegerType` | 103,050 | **16,340** | **13.69%** | 333 | Travel agency ID (`304`, `240`); **16,340 literal `"NULL"` strings** | 
| 25 | `company` | `float64` | `StringType` / `IntegerType` | 6,797 | **112,593** | **94.31%** | 352 | Corporate account ID (`110`, `270`); **112,593 literal `"NULL"` strings** | 
| 26 | `days_in_waiting_list` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 128 | Days spent on waiting list before confirmation | 
| 27 | `customer_type` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 4 | `Transient`, `Transient-Party`, `Contract`, `Group` | 
| 28 | `adr` | `float64` | `DecimalType(12,2)` | 119,390 | 0 | 0.00% | 8,879 | Average Daily Rate in `EUR` (`Min: -6.38`, `Median: 94.58`, `Max: 5400.00`) | 
| 29 | `required_car_parking_spaces` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 5 | `0`, `1`, `2`, `3`, `8` | 
| 30 | `total_of_special_requests` | `int64` | `IntegerType` | 119,390 | 0 | 0.00% | 6 | `0` .. `5` guest special requests | 
| 31 | `reservation_status` | `str` (`object`) | `StringType` | 119,390 | 0 | 0.00% | 3 | `Check-Out` (75,166), `Canceled` (43,017), `No-Show` (1,207) | 
| 32 | `reservation_status_date` | `str` (`object`) | `DateType` | 119,390 | 0 | 0.00% | 926 | ISO date `YYYY-MM-DD` of last status transition | 

### Statistical Outlier Analysis on `adr` via the Interquartile Range (IQR) Method

**ADR (Average Daily Rate)** represents the average revenue earned per occupied room night (`RN-01`). Because both F1 properties (`HP-PT-001` and `HP-PT-002`) are located in Portugal (`country = "PT"`), their native `adr` values are denominated in **`EUR`** before conversion to the platform reporting currency (`COP`). In statistical profiling, **outliers** are atypical or extreme observations that deviate significantly from the central distribution of the dataset, whereas the **Interquartile Range (IQR) method** (Tukey's fences) provides a non-parametric mathematical rule to detect them without being distorted by extreme skew.

#### Step-by-Step Mathematical Derivation on F1 (119,390 Bookings)

When all **119,390** `adr` values in `hotel_bookings.csv` are sorted in ascending order and partitioned into four equal quartiles (**25% of observations per quartile**):

* **Q1 (First Quartile = `$69.29` / `€69.29`)**: Exactly 25% of historical bookings cost less than `69.29` per night.

* **Median / Q2 (Second Quartile = `$94.58` / `€94.58`)**: Exactly 50% of bookings cost less than `94.58` per night (the robust center of the distribution, compared to the parametric mean of **`101.83`**).

* **Q3 (Third Quartile = `$126.00` / `€126.00`)**: Exactly 75% of bookings cost less than `126.00` per night.

* **IQR (Interquartile Range = `56.71`)**: Computed as `Q3 - Q1 = 126.00 - 69.29 = 56.71`. This measures the spread of the central 50% of "normal" room rates.

To establish the mathematical boundary between normal pricing and statistical outliers, the IQR method subtracts and adds `1.5 × IQR` (`1.5 × 56.71 = 85.065`) from the quartiles:

<img width="1774" height="887" alt="Infografía del Rango Intercuartílico (IQR)" src="https://github.com/user-attachments/assets/e75fd6e3-446a-4fff-828e-eada93ef9194" />

Our automated audit (`buenavista_csv.f1_audit`) identified **3,793 bookings (`3.18%`)** above the upper Tukey fence of `211.06` (and **5,753 bookings** when combining upper statistical outliers with non-positive rates `<= 0.00`).

#### Classification and Treatment of the Three ADR Anomaly Profiles

| Anomaly Profile | Observed Count | Extreme Value(s) | Root Cause & Hospitality Domain Interpretation | Bronze → Silver PySpark Treatment | 
| ----- | ----- | ----- | ----- | ----- | 
| **1. The Giant Outlier** | **1 row** (within 3,793 upper outliers) | **`5,400.00`** (vs. 2nd highest `510.00`, 3rd `508.00`, 4th `451.50`) | Single canceled `City Hotel` (`HP-PT-001`) booking (`2016-03-25`). Because the second-highest rate in the entire dataset is `510.00`, `5,400.00` is a decimal/keystroke data-entry error (intended as `54.00` or `540.00`) that artificially inflates the parametric mean. | **Quarantine & Exclude from ADR/RevPAR**: Flag with `is_adr_giant_outlier = true` and exclude `adr > 1000.0` from Silver commercial aggregations (`RN-01`). | 
| **2. The Negative Value** | **1 row** | **`-6.38`** | Single `Resort Hotel` (`HP-PT-002`) booking (`2017-03-05`). A hotel never pays a guest to occupy a room; this represents an accounting adjustment or PMS export glitch. | **Hard Filter / Quarantine**: Reject rows where `adr < 0` during Silver ingestion and log to the data-quality quarantine table. | 
| **3. The Zero Values** | **1,959 rows** (`1.64%`) | **`0.00`** | Legitimate hospitality occurrences: complimentary stays (`743` rows in `Complementary` segment), loyalty redemptions, staff usage, or package bookings billed on master folios. However, including `0.00` stays in commercial rate averages distorts real room pricing. | **Retain for Occupancy, Flag for Commercial ADR**: Keep valid zero-rate stays for physical room occupancy (`RN-01` room nights sold), but tag `is_complimentary = (adr == 0)` so commercial pricing baselines (`RN-05`) filter on `adr > 0`. | 

### Additional Structural Anomalies in F1 (Section 5.1 & 5.3)

1. **Literal `"NULL"` and `"NA"` Textual Tokens**:

   * In the raw CSV file, missing values are **not** encoded as empty strings `""` in three columns; instead, they appear as the literal 4-character ASCII string `"NULL"` in `company` (**112,593 rows**, 94.31%), `agent` (**16,340 rows**, 13.69%), and `country` (**488 rows**, 0.41%), and as `"NA"` in `children` (**4 rows**).

   * **Impact on Ingestion**: Standard pandas `read_csv()` converts `"NULL"` and `"NA"` to `NaN`, which forces integer ID columns (`agent`, `company`, `children`) into floating-point types (`float64`, e.g., `304.0`). In PySpark (`Topic 13`), the reader **must** specify `.option("nullValue", "NULL")` alongside an explicit `StructType` and handle `"NA"` in `children` (coalescing to `0` integer) before casting.

2. **Zero-Guest Reservations (Section 5.3 Defect 1 in Natural Historical Form)**:

   * Exactly **180 rows (`0.15%`)** in F1 have `adults = 0`, `children = 0`, and `babies = 0` simultaneously.

   * **Impact on Ingestion**: A booking with zero occupants is logically invalid for guest-count analytics and must be filtered out during the Bronze → Silver transformation (`adults + children + babies >= 1`).

## 3. Source F4: Traditional Agency Files (one CSV/Excel file per agency and week)

### Provenance, Scope, and Catalog v1.1.0 Alignment (US-25 & US-26)

Traditional travel agencies send a weekly booking file. Per **Section 5**, **Section 5.2**, and **`schemas/catalog.json` (`v1.1.0`)**, F4 is a simulated source owned by `buenavista-csv` (`buenavista_csv.f4_generator`) that produces **one file per agency (`AG01`..`AG15`) and per ISO week**, in CSV or Excel, with deterministic seeding (`--seed 7` + week).

| Parameter | Specification (`Catalog v1.1.0`) |
| ----- | ----- |
| **Source ID & Name** | **F4** — Traditional Agency Files (`Archivos de agencias tradicionales`) |
| **Generator Module** | `src/buenavista_csv/f4_generator.py` (CLI: `generate-f4 --week 2026-W45`) |
| **Loader Module** | `src/buenavista_csv/f4_loader.py` (CLI: `load-f4 --week-dir ...`): books every row in Neon through `operation.reservar()` |
| **Output Location** | `output/f4_agency_files/<YYYY-Www>/<AGxx>_<YYYY-Www>.csv` or `.xlsx` + `_generation_summary.json` |
| **Catalog Agency Channels (`AG01`..`AG15`)** | `AG01` (`ViajaFacil`) .. `AG15` (`Viajes Amanecer`), `commission_pct = 10` |
| **Target Hotels (`28` properties)** | `HP-PT-001`..`003`, `HP-CO-001`..`010`, `HP-MX-001`..`006`, `HP-BR-001`..`006`, `HP-ES-001`..`003` |
| **Guests** | `guest_ref` `G-0000001`..`G-0060000`: the same pseudonymous guest catalog as F2 events and the F3 seed |
| **Accepted Currencies** | Hotel currency (`COP`, `MXN`, `BRL`, `EUR`) or **`USD`** |
| **Amounts** | Base rate of the F3 seed × resort factor × country season, per night and room (USD quotes use a fixed simulation rate; real conversion uses F7) |
| **Ingestion Frequency** | **Weekly** — 15 files/week; default 12 bookings/agency + 3 cross-channel duplicates = **183 rows/week** |

### Three Formats (Section 5.2: "agencias con formatos distintos")

| Layout | Agencies | Format | Differences |
| ----- | ----- | ----- | ----- |
| **A** | AG02, AG04, AG06, AG08, AG10, AG12, AG14 | CSV | English headers, `rate_type` `REFUNDABLE`/`NON_REFUNDABLE`, commission `0.1` |
| **B** | AG01, AG05, AG09, AG13 | CSV | Other headers and column order, `non_refundable` `Y`/`N`, commission in percent `10.0` |
| **C** | AG03, AG07, AG11, AG15 | Excel `.xlsx` | Spanish headers, real date cells, `tarifa` `reembolsable`/`no_reembolsable` |

```
# Layout A (AG02_2026-W45.csv)
agency_booking_id,guest_ref,guest_name,hotel_id,check_in,check_out,room_type,rooms,adults,children,rate_type,currency,total_amount,commission_rate
AG02-2026W45-0001,G-0020245,Pedro Pereira,HP-CO-003,2027-01-31,2027-02-02,SUITE,1,3,0,REFUNDABLE,COP,1258000,0.1

# Layout B (AG01_2026-W45.csv)
booking_ref,hotel_id,guest_code,guest_full_name,checkin_date,checkout_date,room_category,num_rooms,pax_adults,pax_children,non_refundable,amount,currency,commission_pct
AG01-2026W45-0002,HP-PT-001,G-0026178,Sofia Silva,2027-02-11,2027-02-17,DOUBLE,1,2,0,N,612.0,EUR,10.0

# Layout C (AG07_2026-W45.xlsx, sheet "reservas")
localizador | cod_huesped | nombre_huesped | hotel | fecha_llegada | fecha_salida | tipo_habitacion | habitaciones | adultos | ninos | tarifa | importe | moneda | comision
```

`buenavista_csv.f4_layouts` reads any of the three into one canonical row (the full mapping is in `schemas/f4_layouts_schema.json`):

`agency_booking_id, guest_ref, guest_name, hotel_id, check_in, check_out, room_type, rooms, adults, children, refundable, currency, total_amount, commission_rate`

The agency code comes from the file name, so it is never trusted from the content.

### Deliberate Data-Quality Defects (Section 5.3, US-14, US-26)

1. **Defect 3 — missing currency** (`--missing-currency-rate`, default `0.10`). The amount stays in the hotel currency, so the fix (use `catalog.hotels[hotel_id].currency` and flag `currency_inferred = true`) is correct, both in the loader and in Silver.
2. **Defect 4 — cross-channel duplicates** (`--duplicate-count`, RN-08). A real F2 booking is copied with the **same `guest_ref`**, hotel and room type, a slightly changed name (`swap_order`, `middle_initial`, `typo`, `drop_accent_like`) and check-in ±1 day. With `--database-url` (or `NEON_READ_URL`, role `etl_reader`) the F2 bookings are read from `operation.reservations` in Neon; without it, from `data/f2_reference/events.jsonl` (offline and tests). The lineage is in `_generation_summary.json`.

### Loading into Neon (single write path for inventory)

`f4_loader` books every row through `operation.reservar()` — the same function F2 and F3 use — so agency sales also respect the overbooking limit and are rejected when the hotel is full (RN-03, RN-04):

* Invalid rows (unknown hotel, bad dates, guests that do not fit...) are reported and never sent.
* Missing currency → hotel currency, `currency_inferred = true`.
* Billed in the hotel currency → the agency nightly rate is stored; billed in USD → `reservar()` prices from `operation.rates`.
* Idempotent: `(agency code, agency booking id)` is unique, so loading a week twice returns `already_exists`.
* Each row runs in its own transaction; the result of every row is written to `<week>/_load_report.json` (`confirmed`, `already_exists`, `rejected`, `invalid`, `error`).

Duplicates are booked too (agencies did sell them); RN-08 decides in Silver/Gold which one prevails (lowest commission).

## 4. Consumption Strategy, S3 Partitioning, and PySpark Transformation Contract

### Step 1: Pre-Ingestion Validation and S3 Bronze Landing (Airflow + Celery)

1. **Task 1 — `validate_file_sources` (`US-6`)**: Invokes `uv run -m buenavista_csv.validator` to validate F1 and F4 against `schemas/catalog.json` (`v1.1.0`), `schemas/f1_schema.json`, and `schemas/f4_layouts_schema.json`, writing `output/validation_reports/quality_validation_report.json` non-destructively.

2. **Task 2 — `upload_csv_sources_to_s3_bronze`**: Uploads raw files to Amazon S3 `Bronze` using deterministic keys (`CT-08`).

### Step 2: Business Rule Enforcement Matrix in Silver & Gold (Catalog v1.1.0)

| Rule / Requirement | Source(s) Affected | Transformation Logic Applied in PySpark (`Bronze` → `Silver` → `Gold`) | 
| ----- | ----- | ----- | 
| **`RN-01` & `RN-02`** (Net ADR & RevPAR in `COP`) | `F1`, `F4` (plus `F2`, `F3`) | Joins reservation `currency` (`COP`, `MXN`, `BRL`, `EUR`, `USD`) with `F7` exchange rates to convert `total_amount` into `reporting_currency = "COP"`, then applies `net_amount_cop = amount_cop * (1.0 - commission_pct / 100.0)` using `catalog.channels`. | 
| **`RN-03`** (Overbooking Limit) | `F1` (`HP-PT-001`, `HP-PT-002`) + `F3` | Computes rolling 90-day cancellation rate by `lead_time` bucket from `F1` (`is_canceled = 1`) and operational history to calculate `MIN(0.05 * rooms, (hist_cancel_rate * current_bookings) - 1)`, forcing `0` on high-category event dates (`CA-02`). | 
| **`RN-05` & `RN-06`** (Dynamic Pricing) | `F1`, `F2`, `F3`, `F4`, `F6` | Evaluates country-specific season (`catalog.seasons[country]`: `LOW=0.85`, `MID=1.0`, `HIGH=1.25`) × `pickup` multiplier (`14d` window, `±15%` threshold → `1.10x` or `0.92x`) × `event_multiplier` (`1.15x`), clamped to `[0.75x, 1.60x]` of base rate (`CA-04`). | 
| **`RN-08` & `CA-06`** (Cross-Channel Deduplication) | `F4` (`AG01..AG15`) vs. `F2` / `F3` | Matches `guest_ref` (or normalized guest name) + canonical `hotel_id` (`HP-XX-NNN`) + `check_in` within `±1 day`; retains the record with the lowest `commission_pct` (`Direct 0% < F4 Agency 10% < F2 OTA 18%`). | 
| **`RN-11`, `RNF-06` & `CA-10`** (PII Pseudonymization) | `F4` (`guest_name`) | Hashes `guest_name` with SHA-256 + KMS salt (`guest_id_hash`) and drops clear-text guest names before writing Silver Parquet tables. | 

## 5. Repository Structure, Quality Controls, and Execution Runbook (`buenavista-csv`)

### Repository Layout (`US-5`, `US-6` & `Catalog v1.1.0`)

```
buenavista-csv/
├── pyproject.toml                           # Standalone package config & CLI entrypoints
├── Dockerfile                               # Containerized batch validator/generator image
├── .env.example                             # Environment variables template (no secrets committed, CT-02)
├── README.md                                # Concise repository quickstart
├── schemas/
│   ├── catalog.json                         # Shared platform reference catalog (v1.1.0 — 28 hotels, 5 currencies)
│   ├── f1_schema.json                       # Canonical 32-column contract & HP-PT-001/002 mapping for F1
│   └── f4_layouts_schema.json               # Layouts A/B (CSV) and C (Excel) & AG01..AG15 contract for F4
├── data/
│   ├── f1_historical/
│   │   └── hotel_bookings.csv               # Kaggle Hotel Booking Demand dataset (119,390 rows)
│   └── f2_reference/
│       └── events.jsonl                     # Reference F2 events aligned with HP-XX-NNN hotel codes & currencies
├── src/
│   └── buenavista_csv/
│       ├── __init__.py
│       ├── catalog.py                       # Loader & season/currency/hotel helpers for catalog.json (v1.1.0)
│       ├── defects.py                       # Unified DefectRates dataclass (US-14 / US-26)
│       ├── f1_audit.py                      # F1 schema profiler, PT season mapper & IQR outlier auditor (US-11)
│       ├── f4_layouts.py                    # The three agency formats and the canonical reader
│       ├── f4_generator.py                  # 15-agency (AG01..AG15) weekly CSV/Excel generator (US-25 / US-26)
│       ├── f4_loader.py                     # Books each agency row in Neon via operation.reservar()
│       └── validator.py                     # Non-destructive catalog-aware quality gate for F1 & F4 (US-6)
├── output/
│   ├── f1_audit/
│   │   ├── audit_report.csv                 # Machine-readable column & IQR audit metrics
│   │   └── audit_summary.txt                # Executive text report of F1 anomalies & PT season profile
│   ├── f4_agency_files/
│   │   ├── 2026-W45/AGxx_2026-W45.csv|xlsx  # 15 weekly agency files (11 CSV + 4 Excel)
│   │   └── _generation_summary.json         # Traceability log of injected F2 duplicates & missing currencies
│   └── validation_reports/
│       └── quality_validation_report.json   # Structured US-6 validation output
├── tests/
│   ├── fixtures/
│   │   └── AG01_2026-W45.csv                # Known malformed test file for US-6 AC2 verification
│   └── test_buenavista_csv.py               # Automated test suite (6/6 tests passing)
└── docs/
    └── data-sources/                        # Modular Docusaurus pages ready to copy into doc-website/
        ├── _category_.json
        ├── index.md
        └── img/
            └── iqr_outlier_detection.png

```

### Local CLI Runbook

Run all commands from the root directory of `buenavista-csv`:

```
# 1. Synchronize environment with uv
uv sync --extra test

# 2. Run F1 Historical Dataset Audit, Catalog Mapping & IQR Outlier Profiler (US-11 / US-5)
uv run -m buenavista_csv.f1_audit

# 3. Generate F4 Weekly Traditional Agency CSVs with Catalog v1.1.0 Codes (US-25 / US-26)
uv run -m buenavista_csv.f4_generator --week 2026-W45 --seed 7 --bookings-per-agency 12 --duplicate-count 3 --missing-currency-rate 0.10

# 4. Execute Non-Destructive Batch Quality Controls across F1 and F4 (US-6)
uv run -m buenavista_csv.validator

# 5. Run Automated Verification Suite (6/6 tests including Catalog v1.1.0 & Malformed Fixtures)
uv run python -m unittest discover -s tests -v

```

### Non-Destructive Quality Gate Verification (`US-6 AC1 & AC2`)

When `buenavista_csv.validator` runs against `tests/fixtures/AG01_2026-W45.csv`, it validates every record against `schemas/catalog.json` (`v1.1.0`) and reports all 9 structural and catalog defects by exact file name, record ID, and field without modifying the input file:

```
{
  "file": "AG01_2026-W45.csv",
  "valid_schema": true,
  "layout": "A",
  "rows": 3,
  "defect_count": 9,
  "defects": [
    {
      "type": "MISSING_REQUIRED_VALUE",
      "file": "AG01_2026-W45.csv",
      "field": "guest_name",
      "record": "AG01-00002",
      "detail": "Missing guest name"
    },
    {
      "type": "INVALID_HOTEL_ID",
      "file": "AG01_2026-W45.csv",
      "field": "hotel_id",
      "record": "AG01-00002",
      "detail": "Hotel code 'hotel-999' not in catalog.json (expected HP-XX-NNN)"
    },
    {
      "type": "INVALID_ROOM_TYPE",
      "file": "AG01_2026-W45.csv",
      "field": "room_type",
      "record": "AG01-00002",
      "detail": "Invalid room_type 'PENTHOUSE'"
    },
    {
      "type": "INVALID_CURRENCY",
      "file": "AG01_2026-W45.csv",
      "field": "currency",
      "record": "AG01-00002",
      "detail": "Unsupported currency 'GBP' (allowed: ['BRL', 'COP', 'EUR', 'MXN', 'USD'])"
    },
    {
      "type": "INVALID_DATE_RANGE",
      "file": "AG01_2026-W45.csv",
      "field": "check_in,check_out",
      "record": "AG01-00002",
      "detail": "check_out (2026-11-12) <= check_in (2026-11-15) (Section 5.3 Defect 2)"
    },
    {
      "type": "INVALID_TYPE",
      "file": "AG01_2026-W45.csv",
      "field": "total_amount",
      "record": "AG01-00002",
      "detail": "Non-numeric amount 'not_a_number'"
    },
    {
      "type": "INVALID_COMMISSION_RATE",
      "file": "AG01_2026-W45.csv",
      "field": "commission_rate",
      "record": "AG01-00002",
      "detail": "Commission rate out of [0,1]: 1.5"
    },
    {
      "type": "INTRA_FILE_DUPLICATE",
      "file": "AG01_2026-W45.csv",
      "field": "agency_booking_id",
      "record": "AG01-00001",
      "detail": "Duplicate booking ID AG01-00001"
    },
    {
      "type": "MISSING_CURRENCY",
      "file": "AG01_2026-W45.csv",
      "field": "currency",
      "record": "AG01-00001",
      "detail": "Currency code is empty (Section 5.3 Defect 3)"
    }
  ]
}

```
