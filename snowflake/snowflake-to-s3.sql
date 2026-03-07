/* ==================================================================================
   SNOWFLAKE + SQL + AWS (S3)

   This sql file displays the flow to load data from a table in Snowflake to S3 bucket.
   ================================================================================== */

-- STEP 1: Create the Employee Table

CREATE OR REPLACE TABLE Employee (
    EMPLOYEE_ID   INT,
    NAME          VARCHAR(100),
    SALARY        DECIMAL(10, 2),
    DEPARTMENT_ID INT,
    JOINING_DATE  DATE
);


-- STEP 2: Create a File Format for CSV Ingestion

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = ','
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE                   = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL          = TRUE;


-- STEP 3: Create an Internal Stage (Temporary Storage Location)

CREATE OR REPLACE STAGE employee_stage;

-- STEP 4: Insert Rows Directly

INSERT INTO Employee VALUES
    (100, 'Jennifer',  4400.10, 10, '2017-01-05'),
    (101, 'Michael',  13000.10, 10, '2018-08-24'),
    (102, 'Pat',       6000.10, 20, '2018-12-10'),
    (103, 'Den',      11000.20, 30, '2019-02-17'),
    (104, 'Alexander', 3100.20, 40, '2019-07-01'),
    (105, 'Shelli',    2900.20, 50, '2020-04-22'),
    (106, 'Sigal',     2800.30, 60, '2020-09-05'),
    (107, 'Guy',       2600.30, 70, '2021-05-25'),
    (108, 'Karen',     2500.30, 80, '2021-12-21');


-- STEP 5A: Preview the Staged File Before Loading

SELECT $1, $2, $3, $4, $5
FROM   @employee_stage/Employee.csv
       (FILE_FORMAT => csv_format)
LIMIT  10;


-- STEP 5B: Load Data from CSV Stage into Employee Table

COPY INTO Employee
FROM  @employee_stage/Employee.csv
FILE_FORMAT = csv_format
ON_ERROR    = 'CONTINUE';


-- -----------------------------------------------------------------------------
-- STEP 5C: Fallback – Inline File Format (if named format causes issues)
-- -----------------------------------------------------------------------------
COPY INTO Employee
FROM  @employee_stage/Employee.csv
FILE_FORMAT = (
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = ','
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';


-- -----------------------------------------------------------------------------
-- STEP 6: Verify the Loaded Data
-- -----------------------------------------------------------------------------
SELECT * FROM Employee;


/* -----------------------------------------------------------------------------
   TROUBLESHOOTING CHECKLIST
   Run the queries below if data did not load as expected.
----------------------------------------------------------------------------- */

-- Check 1: List files currently in the stage
-- LIST @employee_stage;

-- Check 2: Review COPY command history for errors (last 1 hour)
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--    TABLE_NAME => 'EMPLOYEE',
--    START_TIME => DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
-- ));

-- Check 3: Preview file contents from stage with limit
-- SELECT $1, $2, $3, $4, $5
-- FROM   @employee_stage/Employee.csv
--       (FILE_FORMAT => csv_format)
-- LIMIT  10;

-- Check 4: Reload cleanly – truncate first, then re-copy with error reporting
-- TRUNCATE TABLE Employee;

-- COPY INTO Employee
-- FROM      @employee_stage/Employee.csv
-- FILE_FORMAT        = csv_format
-- ON_ERROR           = 'CONTINUE'
-- RETURN_FAILED_ONLY = TRUE;


/* =============================================================================
-- EXPORT EMPLOYEE TABLE TO S3
   ============================================================================= */

-- METHOD B: Export Using Storage Integration (Recommended / More Secure)
-- Uses an IAM role instead of embedding keys in the query.

-- Step B1: Create the storage integration
CREATE STORAGE INTEGRATION s3_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<your_account_id>:role/<role_name>'  -- ← replace
    STORAGE_ALLOWED_LOCATIONS = ('s3://employee-snowflake/');

-- Step B2: Retrieve the Snowflake IAM values to configure the AWS trust policy
DESC STORAGE INTEGRATION s3_integration;

-- Note: Make sure to make changes to Trust relationship policy on S3 bucket.

-- Step B3: Create the external stage using the integration
CREATE OR REPLACE STAGE s3_output_stage
    URL                 = 's3://employee-snowflake/employee-output/'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT         = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Step B4: Export
COPY INTO @s3_output_stage
FROM     Employee
HEADER   = TRUE
OVERWRITE = TRUE;

-- Step B5: Verify
LIST @s3_output_stage;
