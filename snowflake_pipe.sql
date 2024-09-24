-------------------------------------------------------------------------------------------
        --  Pipeline
-------------------------------------------------------------------------------------------
---> Set up environment
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE nyc_airbnb;
  
---> Create pipe for continuous ingestion
CREATE OR REPLACE PIPE public.snowflake_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.nyc_airbnb_table
FROM @s3_data_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- View Snowpipe status
SHOW PIPES;

-- Manually refresh Snowpipe (if needed)
ALTER PIPE snowflake_pipe REFRESH;

-- Monitoring Snowpipe
SELECT SYSTEM$PIPE_STATUS('snowflake_pipe');