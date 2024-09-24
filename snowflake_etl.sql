-------------------------------------------------------------------------------------------
        -- Set up environment
-------------------------------------------------------------------------------------------

---> Create database
CREATE DATABASE IF NOT EXISTS nyc_airbnb;

---> Create warehouse
CREATE WAREHOUSE IF NOT EXISTS compute_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

---> Set role, warehouse, db
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE nyc_airbnb;

---> Namespace for raw data
CREATE SCHEMA IF NOT EXISTS raw;

---> Namespace for raw data
CREATE SCHEMA IF NOT EXISTS transformed;


-------------------------------------------------------------------------------------------
        -- Data Ingestion
-------------------------------------------------------------------------------------------

---> Create a storage integration
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_data_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::867344456549:role/snowflakerole'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://uniquecsvbucket/uploads/');

---> Create the Stage referencing the integration and the Blob location and CSV File Format
CREATE OR REPLACE STAGE public.s3_data_stage
  STORAGE_INTEGRATION = s3_data_integration
  URL = 's3://uniquecsvbucket/uploads/'
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

---> Create the raw airbnb table using raw schema
CREATE TABLE IF NOT EXISTS raw.nyc_airbnb_table (
  id NUMBER,
  name STRING,
  host_id NUMBER,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude FLOAT,
  longitude FLOAT,
  room_type STRING,
  price NUMBER,
  minimum_nights NUMBER,
  number_of_reviews NUMBER,
  last_review STRING,
  reviews_per_month FLOAT,
  calculated_host_listings_count NUMBER,
  availability_365 NUMBER
);

---> Create stream to track changes in raw data for incremental loads
CREATE OR REPLACE STREAM raw.nyc_airbnb_stream
ON TABLE raw.nyc_airbnb_table;


-------------------------------------------------------------------------------------------
                -- Data Transformation
-------------------------------------------------------------------------------------------

---> Create table for transformed data in separate schema
CREATE TABLE IF NOT EXISTS transformed.nyc_airbnb_table (
  id NUMBER,
  name STRING,
  host_id NUMBER,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude FLOAT,
  longitude FLOAT,
  room_type STRING,
  price NUMBER,
  minimum_nights NUMBER,
  number_of_reviews NUMBER,
  last_review STRING,
  reviews_per_month FLOAT,
  calculated_host_listings_count NUMBER,
  availability_365 NUMBER
);

---> Create stream  to track changes in transformed data for incremental loads
CREATE OR REPLACE STREAM transformed.nyc_airbnb_stream
ON TABLE transformed.nyc_airbnb_table;

---> Create Transform data procedure
CREATE OR REPLACE PROCEDURE public.data_transformation()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
   null_count INTEGER;
BEGIN
    INSERT INTO transformed.nyc_airbnb_table
    WITH min_last_review AS (
        SELECT MIN(TRY_TO_DATE(last_review, 'YYYY-MM-DD')) AS earliest_valid_date
        FROM raw.nyc_airbnb_table
    )
    SELECT 
        id,
        name,
        host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude,
        room_type,
        price,
        minimum_nights,
        number_of_reviews,
        -- Convert last_review to a valid date and fill missing values with the earliest valid date
        COALESCE(TRY_TO_DATE(last_review, 'YYYY-MM-DD'), (SELECT earliest_valid_date FROM min_last_review)) AS last_review,
        -- Handle missing values in reviews_per_month
        COALESCE(reviews_per_month, 0) AS reviews_per_month,
        calculated_host_listings_count,
        availability_365
    FROM raw.nyc_airbnb_table
    WHERE price > 0
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL;
    RETURN 'Data transformation has been completed successfully';
END;
$$;

---> Check whether transformed data contains invalid values
CREATE OR REPLACE PROCEDURE public.check_data_quality()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
   null_count INTEGER;
BEGIN
   -- Check for NULL values in critical columns
   SELECT COUNT(*)
   INTO :null_count
   FROM transformed.nyc_airbnb_table
   WHERE price IS NULL
      OR minimum_nights IS NULL
      OR availability_365 IS NULL;

   -- If nulls are found log it
   IF (null_count > 0) THEN
      RETURN 'Data quality check failed: NULL values found in critical columns.';
   ELSE
      RETURN 'Data quality check passed: No NULL values in critical columns.';
   END IF;
END;
$$;


-------------------------------------------------------------------------------------------
        -- Tasks
-------------------------------------------------------------------------------------------

---> Create a Task to automate the transformation process daily
CREATE OR REPLACE TASK public.daily_nyc_airbnb_etl
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON * * * * * UTC' -- run daily at midnight
AS
  CALL data_transformation();
  CALL check_data_quality();


---> Activate the task
ALTER TASK public.daily_nyc_airbnb_etl RESUME;


-------------------------------------------------------------------------------------------
        --  Error Handling and Monitoring
-------------------------------------------------------------------------------------------

---> Time Travel to recover from errors by querying historical data
CREATE OR REPLACE PROCEDURE public.time_travel()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO transformed.nyc_airbnb_table
  SELECT * FROM transformed.nyc_airbnb_table AT (OFFSET => -1);

  RETURN 'Data has been recovered successfully';
END;
$$;

-- Procedure for monitoring changes
CREATE OR REPLACE PROCEDURE public.monitor_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  raw_data_changes NUMBER;
  transformed_data_changes NUMBER;
BEGIN
  raw_data_changes := (SELECT COUNT(*) FROM raw.nyc_airbnb_stream);
  transformed_data_changes := (SELECT COUNT(*) FROM transformed.nyc_airbnb_stream);

  RETURN 'Number of changes in raw data: ' || TO_VARCHAR(raw_data_changes) || '. In transformed data: ' || TO_VARCHAR(transformed_data_changes);
END;
$$;