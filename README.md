# Real-Time SCD Type 2 Pipeline — dbt + Snowflake + AWS S3

A production-style data pipeline that ingests product catalog data from AWS S3, applies a **Medallion architecture** (Bronze → Silver → Snapshot → Gold), and tracks historical changes using **SCD Type 2** via dbt snapshots. Built as part of the Data Engineering Academy (DEA) portfolio.

---

## Architecture

```
┌─────────────┐     COPY Macro      ┌───────────────────────────────────────────────────────┐
│   AWS S3    │ ──────────────────► │                  Snowflake                            │
│  (CSV files)│                     │                                                       │
└─────────────┘                     │  ┌──────────┐   ┌──────────┐   ┌──────────────────┐   │
                                    │  │  BRONZE  │──►│  SILVER  │──►│    SNAPSHOT      │   │
      External Stage                │  │  (RAW)   │   │(Transform│   │  (SCD Type 2)    │   │
  DEA_REAL_TIME_SCD2.RAW            │  │          │   │  Layer)  │   │  dbt snapshots   │   │
                                    │  └──────────┘   └──────────┘   └────────┬─────────┘   │
                                    │                                          │            │
                                    │                                          ▼            │
                                    │                                   ┌──────────────┐    │
                                    │                                   │     GOLD     │    │
                                    │                                   │    (Views)   │    │
                                    │                                   │ Current State│    │
                                    │                                   │ Full History │    │
                                    │                                   └──────────────┘    │
                                    └───────────────────────────────────────────────────────┘
```

### Layer Reference

| Layer | Database | Schema | Object | Purpose |
|---|---|---|---|---|
| **Bronze** | `DEA_REAL_TIME_SCD2` | `RAW` | `WORK_PRODUCT_COPY` | Raw landing table — data copied from S3 as-is |
| **Silver** | `MINIPROJ3` | `SILVER` | `WORK_PRODUCT_TRANSFORM` | Typed, cleaned, audit-stamped transform |
| **Snapshot** | `MINIPROJ3` | `SNAPSHOTS` | `PRODUCT_SNAPSHOT` | Full SCD2 history via dbt snapshot |
| **Gold** | `MINIPROJ3` | `GOLD` | `DIM_PRODUCTS` | Current-state view (`DBT_VALID_TO IS NULL`) |
| **Gold** | `MINIPROJ3` | `GOLD` | `PRODUCT_VIEW` | Full versioned history view |

---

## Tech Stack

![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-FF9900?style=flat&logo=amazons3&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)

| Tool | Role |
|---|---|
| **dbt Core** | Transform orchestration, snapshot management, model materializations |
| **Snowflake** | Cloud data warehouse — storage, compute, and stage management |
| **AWS S3** | Source data storage (CSV files) |
| **Snowflake External Stage** | Secure S3 → Snowflake bridge via IAM role (no access keys) |
| **dbt Snapshots** | SCD Type 2 history tracking using `check` strategy |
| **Jinja Macros** | Parameterized COPY INTO logic for reusable ingestion |

---

## Project Structure

```
dbt-repo/
├── models/
│   └── project2/
│       ├── bronze/             # Source reference models (Bronze layer)
│       ├── silver_layer/       # Cleaning + type casting (Silver layer)
│       └── gold/               # Reporting views (Gold layer)
├── snapshots/
│   └── product_snapshot.sql    # SCD Type 2 snapshot — check strategy
├── macros/
│   └── macros_copy_csv.sql     # Parameterized COPY INTO macro
├── snowflake/
│   └── snowflake_setup.sql     # One-time Snowflake environment setup
├── dbt_project.yml             # Project config, vars, materializations
└── README.md
```

---

## Key Design Decisions

**Why SCD Type 2?**
Product attributes like `SELLING_PRICE` change over time. Without SCD2, overwrites would silently destroy history — making it impossible to answer questions like *"What was the price of this product when this order was placed?"* The snapshot layer preserves every version with `DBT_VALID_FROM` and `DBT_VALID_TO` timestamps.

**Why `check` strategy (not `timestamp`)?**
The source data does not guarantee that `UPDATE_DTS` increments on every change. Using `timestamp` strategy caused duplicate open snapshot records when the same timestamp appeared across batches. Switching to `check` strategy compares actual column values, which reliably detects real changes only.

**Why two Gold views?**
- `DIM_PRODUCTS` — current state only (`DBT_VALID_TO IS NULL`), optimized for fact table joins in daily reporting
- `PRODUCT_VIEW` — full versioned history, used for point-in-time analysis and auditing

**Why VARCHAR everywhere in Bronze?**
The raw layer preserves source data exactly as received. Type casting and validation happen in Silver, keeping Bronze as a faithful record of what arrived from S3.

---

## How to Run

### Prerequisites
- Snowflake account with `ACCOUNTADMIN` access for initial setup
- AWS S3 bucket with product CSV files
- dbt Core installed (`pip install dbt-snowflake`)
- `profiles.yml` configured with your Snowflake credentials

### 1. Snowflake Setup (one-time)
Run `snowflake/snowflake_setup.sql` in order, substituting your own:
- AWS IAM Role ARN
- S3 bucket name

### 2. Configure dbt Variables
Update `dbt_project.yml` vars to match your environment:

```yaml
vars:
  rawhist_db:  DEA_REAL_TIME_SCD2          # database for Bronze layer
  wrk_schema:  RAW                          # schema for Bronze table
  stage_name:  DEA_REAL_TIME_SCD2.RAW.DEA_REAL_TIME_SCD2_STAGE
  file_format: DEA_REAL_TIME_SCD2.RAW.MY_CSV_FORMAT
  purge_status: FALSE
```

### 3. Run the Pipeline

```bash
# Install dependencies
dbt deps

# Load data from S3 into Bronze (runs COPY macro)
dbt run --select bronze

# Transform Bronze → Silver
dbt run --select silver_layer

# Run SCD2 snapshot (captures changes)
dbt snapshot

# Build Gold views
dbt run --select gold
```

### 4. Verify Results

```sql
-- Check SCD2 history for a product
SELECT PRODUCT_ID, SELLING_PRICE, DBT_VALID_FROM, DBT_VALID_TO
FROM MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT
ORDER BY PRODUCT_ID, DBT_VALID_FROM;

-- Current state only
SELECT * FROM MINIPROJ3.GOLD.DIM_PRODUCTS;

-- Full history
SELECT * FROM MINIPROJ3.GOLD.PRODUCT_VIEW;
```

---

## Pipeline Reset (Dev/Testing)

To wipe all layers and re-run from scratch:

```sql
TRUNCATE TABLE MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT;
TRUNCATE TABLE DEA_REAL_TIME_SCD2.RAW.WORK_PRODUCT_COPY;
TRUNCATE TABLE MINIPROJ3.SILVER.WORK_PRODUCT_TRANSFORM;
```

---

## Author

**Talib Hussain**
Data Analyst → Data Engineer
[LinkedIn](https://linkedin.com/in/talhussain) · [GitHub](https://github.com/tbhatti211-wq)
