-- =============================================================================
-- DEA PROJECT 2 — Real-Time SCD Type 2 Pipeline
-- Snowflake Environment Setup Script
--
-- Purpose : One-time setup for the Snowflake infrastructure supporting the
--           dbt-powered SCD2 product pipeline.
-- Stack   : AWS S3 → Snowflake External Stage → dbt (Bronze/Silver/Snapshot/Gold)
-- Roles   : Run initial setup as ACCOUNTADMIN, then grant to pc_dbt_role for dbt
-- =============================================================================


-- =============================================================================
-- SECTION 1: ENVIRONMENT SETUP
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Use the default compute warehouse for setup tasks
USE WAREHOUSE COMPUTE_WH;

-- Create the raw/ingestion database (landing zone for S3 data)
CREATE OR REPLACE DATABASE DEA_REAL_TIME_SCD2;

USE DATABASE DEA_REAL_TIME_SCD2;

-- Create schemas:
--   BRONZE    → raw landing zone (data copied from S3 as-is)
--   TRANSFORMED → placeholder; actual transform layers live in MINIPROJ3 db via dbt
CREATE OR REPLACE SCHEMA BRONZE;
CREATE OR REPLACE SCHEMA TRANSFORMED;


-- =============================================================================
-- SECTION 2: S3 STORAGE INTEGRATION
-- Purpose: Allows Snowflake to securely read from S3 without hardcoded keys.
--          Uses an IAM role (trust relationship) instead of access keys.
-- =============================================================================

CREATE OR REPLACE STORAGE INTEGRATION DEA_REAL_TIME_SCD2_INT
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  -- Replace with your own AWS IAM role ARN — do not commit real ARN to version control
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/<YOUR_ROLE_NAME>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://dea-project2-bucket');

-- Verify integration was created and retrieve the Snowflake AWS principal
-- (needed to complete the IAM trust relationship on the AWS side)
SHOW INTEGRATIONS;
DESC INTEGRATION DEA_REAL_TIME_SCD2_INT;


-- =============================================================================
-- SECTION 3: EXTERNAL STAGE
-- Purpose: Named pointer to the S3 bucket. dbt macros reference this stage
--          by name via the `stage_name` variable in dbt_project.yml.
-- =============================================================================

CREATE OR REPLACE STAGE DEA_REAL_TIME_SCD2.RAW.DEA_REAL_TIME_SCD2_STAGE
  STORAGE_INTEGRATION = DEA_REAL_TIME_SCD2_INT
  URL                 = 's3://dea-project2-bucket';

-- Verify S3 files are visible through the stage (run after uploading CSV files)
ls @DEA_REAL_TIME_SCD2.RAW.DEA_REAL_TIME_SCD2_STAGE;


-- =============================================================================
-- SECTION 4: FILE FORMAT
-- Purpose: Tells Snowflake how to parse the CSV files from S3.
--          Created twice — once as ACCOUNTADMIN (for setup), once as pc_dbt_role
--          (so dbt can reference it at runtime without privilege escalation).
-- NOTE   : COLLATE 'en-ci' on the Bronze table ensures case-insensitive matching
--          on string columns (e.g. product lookups won't fail on "Widget" vs "widget")
-- =============================================================================

-- ⚠️  BUG NOTE: The original script had a SELECT statement embedded inside
--     the FILE FORMAT DDL block. That SELECT has been moved to Section 7
--     (Validation Queries) where it belongs.

CREATE OR REPLACE FILE FORMAT DEA_REAL_TIME_SCD2.RAW.MY_CSV_FORMAT
  TYPE                      = CSV
  FIELD_DELIMITER           = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER               = 1
  NULL_IF                   = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL       = true;


-- =============================================================================
-- SECTION 5: ROLE GRANTS
-- Purpose: Grant pc_dbt_role the minimum permissions needed to run dbt models.
--          dbt reads from RAW and writes to SILVER (in DEA_REAL_TIME_SCD2 db).
--          Snapshot and Gold layers are managed in the MINIPROJ3 database
--          and require separate grants there.
-- =============================================================================

-- RAW schema — dbt reads from here (Bronze source table)
GRANT USAGE  ON SCHEMA DEA_REAL_TIME_SCD2.RAW            TO ROLE pc_dbt_role;
GRANT SELECT ON ALL TABLES    IN SCHEMA DEA_REAL_TIME_SCD2.RAW TO ROLE pc_dbt_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DEA_REAL_TIME_SCD2.RAW TO ROLE pc_dbt_role;

-- SILVER schema — dbt creates/writes transform tables here
GRANT USAGE        ON SCHEMA DEA_REAL_TIME_SCD2.SILVER            TO ROLE pc_dbt_role;
GRANT CREATE TABLE ON SCHEMA DEA_REAL_TIME_SCD2.SILVER            TO ROLE pc_dbt_role;
GRANT SELECT ON ALL TABLES    IN SCHEMA DEA_REAL_TIME_SCD2.SILVER TO ROLE pc_dbt_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DEA_REAL_TIME_SCD2.SILVER TO ROLE pc_dbt_role;

-- Stage and file format access — needed for dbt COPY macro to load S3 data
GRANT USAGE ON ALL STAGES       IN DATABASE DEA_REAL_TIME_SCD2 TO ROLE pc_dbt_role;
GRANT USAGE ON FUTURE STAGES    IN DATABASE DEA_REAL_TIME_SCD2 TO ROLE pc_dbt_role;
GRANT USAGE ON ALL FILE FORMATS IN DATABASE DEA_REAL_TIME_SCD2 TO ROLE pc_dbt_role;
GRANT USAGE ON FUTURE FILE FORMATS IN DATABASE DEA_REAL_TIME_SCD2 TO ROLE pc_dbt_role;


-- =============================================================================
-- SECTION 6: DBT ROLE SETUP
-- Switch to pc_dbt_role to create objects dbt will own at runtime
-- =============================================================================

USE ROLE pc_dbt_role;

-- Recreate file format under pc_dbt_role so dbt can reference it without
-- needing ACCOUNTADMIN privileges during pipeline runs
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
  TYPE                         = CSV
  FIELD_DELIMITER              = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER                  = 1
  NULL_IF                      = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL          = true;

-- Bronze landing table — raw product data copied from S3
-- All columns VARCHAR to preserve raw values exactly as received;
-- type casting and cleaning happens in the Silver dbt model.
-- COLLATE 'en-ci' = case-insensitive collation for reliable string matching.
-- Audit columns (INSERT_DTS, UPDATE_DTS, SOURCE_FILE_NAME, SOURCE_FILE_ROW_NUMBER)
-- are populated by the dbt COPY macro, not the source data.
CREATE OR REPLACE TABLE WORK_PRODUCT_COPY (
   PRODUCT_ID             VARCHAR(255)   NOT NULL COLLATE 'en-ci'
  ,PRODUCT_NAME           VARCHAR(255)   NOT NULL COLLATE 'en-ci'
  ,CATEGORY               VARCHAR(255)           COLLATE 'en-ci'
  ,SELLING_PRICE          VARCHAR(50)            COLLATE 'en-ci'
  ,MODEL_NUMBER           VARCHAR(50)            COLLATE 'en-ci'
  ,ABOUT_PRODUCT          VARCHAR(5000)          COLLATE 'en-ci'
  ,PRODUCT_SPECIFICATION  VARCHAR(5000)          COLLATE 'en-ci'
  ,TECHNICAL_DETAILS      VARCHAR(50000)         COLLATE 'en-ci'
  ,SHIPPING_WEIGHT        VARCHAR(30)            COLLATE 'en-ci'
  ,PRODUCT_DIMENSIONS     VARCHAR(100)           COLLATE 'en-ci'
  ,INSERT_DTS             TIMESTAMP_NTZ(6) NOT NULL  -- set by COPY macro
  ,UPDATE_DTS             TIMESTAMP_NTZ(6) NOT NULL  -- set by COPY macro
  ,SOURCE_FILE_NAME       VARCHAR(255)     NOT NULL  -- S3 file that sourced this row
  ,SOURCE_FILE_ROW_NUMBER VARCHAR(255)     NOT NULL  -- row position within source file
);


-- =============================================================================
-- SECTION 7: PIPELINE RESET COMMANDS
-- Use these to wipe all layers and re-run the pipeline from scratch during
-- development/testing. Run in order to avoid FK or dependency issues.
-- ⚠️  Do NOT run in production without a backup plan.
-- =============================================================================

-- Step 1: Drop/truncate the snapshot table (dbt will recreate on next run)
TRUNCATE TABLE MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT;
-- DROP TABLE MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT;  -- use DROP if schema changed

-- Step 2: Clear Bronze raw landing table
TRUNCATE TABLE DEA_REAL_TIME_SCD2.RAW.WORK_PRODUCT_COPY;

-- Step 3: Clear Silver transform table
TRUNCATE TABLE MINIPROJ3.SILVER.WORK_PRODUCT_TRANSFORM;


-- =============================================================================
-- SECTION 8: VALIDATION QUERIES
-- Use these to verify data at each layer after a pipeline run.
-- =============================================================================

-- Check for duplicate PRODUCT_IDs in Bronze (should return 0 rows if clean)
SELECT PRODUCT_ID, COUNT(*) AS count
FROM DEA_REAL_TIME_SCD2.RAW.WORK_PRODUCT_COPY
GROUP BY PRODUCT_ID
HAVING COUNT(*) > 1;

-- Inspect Silver layer — verify timestamps are populated correctly
SELECT PRODUCT_ID, INSERT_DTS, UPDATE_DTS
FROM MINIPROJ3.SILVER.WORK_PRODUCT_TRANSFORM;

-- Inspect Snapshot layer — verify SCD2 open/closed records
-- DBT_VALID_TO IS NULL = current active record
-- DBT_VALID_TO IS NOT NULL = historical (closed) record
SELECT * FROM MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT;

-- Verify SCD2 history for a specific product — check open/close dates per version
SELECT
   PRODUCT_ID
  ,SELLING_PRICE
  ,UPDATE_DTS
  ,DBT_VALID_FROM
  ,DBT_VALID_TO
FROM MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT
ORDER BY PRODUCT_ID, UPDATE_DTS;

-- Check Gold view (full versioned history)
SELECT *
FROM MINIPROJ3.GOLD.PRODUCT_VIEW
WHERE PRODUCT_ID = '<YOUR_TEST_PRODUCT_ID>';  -- replace with a real ID for testing

-- Spot-check snapshot for a specific product
SELECT *
FROM MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT
WHERE PRODUCT_ID = '<YOUR_TEST_PRODUCT_ID>';

-- Row count sanity check on snapshot
SELECT COUNT(PRODUCT_ID) FROM MINIPROJ3.SNAPSHOTS.PRODUCT_SNAPSHOT;