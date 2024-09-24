# Snowflake
This project implements an ETL (Extract, Transform, Load) pipeline for New York City Airbnb data using Snowflake.

## Preconditions

1. Snowflake account
   ### Note: Ukraine is not supported via registration, you need to use vpn
2. SnowSQL CLI installed on your local host
3. Access to AWS S3 cloud
4. NYC Airbnb dataset uploaded to your cloud storage

## How to run

1. Fetch repository
2. For the `snowflake_airbnb_etl_script.sql` file provide your own:
   - Cloud storage URL
   - Credentials for accessing the cloud storage

## Running the ETL

1. Log in to your Snowflake account via SnowSQL:
   ```
   snowsql -a your_account -u your_username
   ```
   OR
   You could use .snowsql config to provide all neccessary details to your connection
   and call it like
   ```
   snowsql -c *your_connection_name*
   ```
   
2. Execute the SQL script:
   ```
   snowsql -f snowflake_etl.sql
   ```
## Running the Pipe
1. Execute the SQL script:
   ```
   snowsql -f snowflake_pipe.sql
   ```
Snowflake pipe is set up to run automatically, but in case of delays in retrieving you can run it manually
```sql
ALTER PIPE snowflake_pipe REFRESH;
```

## Running Transformations

Transformations are scheduled to run automatically daily. For manual execution:

```sql
CALL data_transformation();
```

## Data quality

Can also be checked by manual execution
   ```sql
   CALL check_data_quality();
   ```
## Running Snowflake Tasks

The `daily_airbnb_etl` task is set to run automatically daily. For manual execution:

```sql
EXECUTE TASK daily_nyc_airbnb_etl;
```

## Monitoring the Pipeline

1. Check Snowpipe status:
   ```sql
   SELECT SYSTEM$PIPE_STATUS('nyc_airbnb_pipe');
   ```

2. Monitor data changes:
   ```sql
   CALL monitor_changes();
   ```
   
## Data Recovery

In case of errors/invalid state, you can recover data using Time Travel:

```sql
Call time_travel()
```
It will take snapshot from the previous hour
