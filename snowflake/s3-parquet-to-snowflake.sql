/* =============================================================================
   LISTING TABLE FROM PARQUET FILES IN S3 to SnowFlake
   ============================================================================= */

-- -----------------------------------------------------------------------------
-- STEP 1: Create Storage Integration for Parquet Bucket
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION s3_bootcamp_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<account_id>:role/<role_name>'  -- ← replace
    STORAGE_ALLOWED_LOCATIONS = ('s3://employee-assignmentsnowflake/');


DESC storage integration s3_bootcamp_integration

-- -----------------------------------------------------------------------------
-- STEP 2: Create a Stage Pointing to the Parquet Files
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STAGE listing_stage
    URL                 = 's3://employee-assignmentsnowflake/listing_data/'
    STORAGE_INTEGRATION = s3_bootcamp_integration
    FILE_FORMAT         = (TYPE = 'PARQUET');


-- -----------------------------------------------------------------------------
-- STEP 3: Preview Parquet File Contents Before Loading
-- Snowflake reads Parquet natively – no schema needed at this stage.
-- -----------------------------------------------------------------------------
SELECT *
FROM   @listing_stage
LIMIT  10;


-- -----------------------------------------------------------------------------
-- STEP 4: Create the Listing Table
-- Column names must match the Parquet schema for MATCH_BY_COLUMN_NAME to work.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE Listing (
    id                INT,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(200),
    gender            VARCHAR(50),
    ip_address        VARCHAR(50),
    birthdate         VARCHAR(50),
    salary            DECIMAL(12, 2),
    title             VARCHAR(200),
    comments          VARCHAR(500),
    cc                VARCHAR(50),
    country           VARCHAR(100),
    registration_dttm TIMESTAMP
);


-- -----------------------------------------------------------------------------
-- STEP 5: Create a Parquet File Format
-- Parquet is self-describing, so minimal configuration is needed.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = 'PARQUET';


-- -----------------------------------------------------------------------------
-- STEP 6: Load Parquet Data into Listing Table
-- MATCH_BY_COLUMN_NAME maps Parquet fields to table columns by name,
-- avoiding raw JSON/variant loading.
-- -----------------------------------------------------------------------------
COPY INTO Listing
FROM      @listing_stage
FILE_FORMAT          = parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


-- -----------------------------------------------------------------------------
-- STEP 7: Verify the Loaded Data
-- -----------------------------------------------------------------------------
SELECT * FROM Listing LIMIT 10;    -- Preview first 10 rows
SELECT COUNT(*) FROM Listing;      -- Confirm total row count
