-- SCD Type 1: Overwriting Records (No History)

-- Set Role and Create Warehouse
USE ROLE sysadmin;

CREATE WAREHOUSE IF NOT EXISTS compute_wh
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 120;

-- Create Database and Schema
CREATE DATABASE IF NOT EXISTS scd_demo;
USE DATABASE scd_demo;

CREATE SCHEMA IF NOT EXISTS scd1;
USE SCHEMA scd1;

-- Create the customer table (SCD Type 1)
CREATE OR REPLACE TABLE customer (
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create a stream to capture changes in the customer table (SCD Type 1)
CREATE OR REPLACE STREAM customer_table_changes ON TABLE customer;

-- Create raw customer table (Staging Table)
CREATE OR REPLACE TABLE customer_raw (
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR
);


-- Create a stage for external data (e.g., CSV data from S3)
CREATE OR REPLACE STAGE customer_ext_stage
    URL = 's3://scd-data-warehousing/'
    CREDENTIALS = (aws_key_id = '<access-key>', aws_secret_key = '<secret-key>')
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1);

SHOW STAGES;
LIST @customer_ext_stage;

-- Create a pipe to auto-ingest data into the raw table
CREATE OR REPLACE PIPE customer_s3_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO customer_raw
    FROM @customer_ext_stage
    FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1);


SHOW PIPES;

SELECT SYSTEM$PIPE_STATUS('customer_s3_pipe');


--==============================================


-- Merge operation to update customer data from raw data (SCD Type 1 logic)
/*

MERGE INTO customer c
USING (
SELECT customer_id, 
       MAX(first_name) AS first_name, 
       MAX(last_name) AS last_name, 
       MAX(email) AS email, 
       MAX(street) AS street, 
       MAX(city) AS city, 
       MAX(state) AS state, 
       MAX(country) AS country
FROM customer_raw
GROUP BY customer_id
) cr
ON c.customer_id = cr.customer_id
WHEN MATCHED AND (
    c.first_name  <> cr.first_name  OR
    c.last_name   <> cr.last_name   OR
    c.email       <> cr.email       OR
    c.street      <> cr.street      OR
    c.city        <> cr.city        OR
    c.state       <> cr.state       OR
    c.country     <> cr.country
) THEN UPDATE
SET 
    c.first_name  = cr.first_name,
    c.last_name   = cr.last_name,
    c.email       = cr.email,
    c.street      = cr.street,
    c.city        = cr.city,
    c.state       = cr.state,
    c.country     = cr.country,
    update_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (c.customer_id, c.first_name, c.last_name, c.email, c.street, c.city, c.state, c.country)
VALUES 
    (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);

*/


-- Create procedure for SCD Type 1
CREATE OR REPLACE PROCEDURE pdr_scd_1()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var cmd = `
        MERGE INTO customer c
        USING (
        SELECT customer_id, 
               MAX(first_name) AS first_name, 
               MAX(last_name) AS last_name, 
               MAX(email) AS email, 
               MAX(street) AS street, 
               MAX(city) AS city, 
               MAX(state) AS state, 
               MAX(country) AS country
        FROM customer_raw
        GROUP BY customer_id
        ) cr
        ON c.customer_id = cr.customer_id
        WHEN MATCHED AND (
            c.first_name  <> cr.first_name  OR
            c.last_name   <> cr.last_name   OR
            c.email       <> cr.email       OR
            c.street      <> cr.street      OR
            c.city        <> cr.city        OR
            c.state       <> cr.state       OR
            c.country     <> cr.country
        ) THEN UPDATE
        SET 
            c.first_name  = cr.first_name,
            c.last_name   = cr.last_name,
            c.email       = cr.email,
            c.street      = cr.street,
            c.city        = cr.city,
            c.state       = cr.state,
            c.country     = cr.country,
            update_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (c.customer_id, c.first_name, c.last_name, c.email, c.street, c.city, c.state, c.country)
        VALUES 
            (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);
    `;
    
    var cmd1 = "TRUNCATE TABLE SCD_DEMO.SCD1.customer_raw;";
    
    var sql  = snowflake.createStatement({sqlText: cmd});
    var sql1 = snowflake.createStatement({sqlText: cmd1});
    
    var result  = sql.execute();
    var result1 = sql1.execute();
    
    return cmd + '\n' + cmd1;
$$;

-- Call the procedure
CALL pdr_scd_1();



-- Set up TASKADMIN role
USE ROLE securityadmin;

CREATE OR REPLACE ROLE taskadmin;

-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to TASKADMIN
USE ROLE accountadmin;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE taskadmin;

-- Set the active role to SECURITYADMIN to show that this role can grant a role to another role
USE ROLE securityadmin;

GRANT ROLE taskadmin TO ROLE sysadmin;


-- Set the active role to sysadmin
USE ROLE sysadmin;

-- Create and schedule the task
CREATE OR REPLACE TASK task_scd_raw 
    WAREHOUSE = COMPUTE_WH 
    SCHEDULE = '1 MINUTE'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL pdr_scd_1();

/*
-- Or create the task that triggers on stream insertions
CREATE OR REPLACE TASK task_scd_raw
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING STREAM customer_table_changes'  -- Trigger task on stream updates
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
    WHEN SYSTEM$STREAM_HAS_DATA('customer_table_changes')  -- Ensure there is data in the stream
    AS
    -- Call the procedure to perform SCD Type 1 logic when new data is inserted
    CALL pdr_scd_1();   
*/

-- Show tasks
SHOW TASKS;


-- Check the next run time for scheduled tasks
SELECT 
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, scheduled_time) AS next_run,
    scheduled_time,
    CURRENT_TIMESTAMP,
    name,
    state
FROM 
    TABLE(information_schema.task_history()) 
WHERE 
    state = 'SCHEDULED'
ORDER BY 
    completed_time DESC;


-- Check Tables


--==============================================
SELECT COUNT(*) FROM customer;

SELECT COUNT(*) FROM customer_raw;

SELECT COUNT(*) FROM customer_table_changes;
--==============================================


-- Post-Merge Validation (Customer Table):
SELECT * FROM customer ORDER BY update_timestamp DESC LIMIT 100;

SELECT COUNT(*) FROM customer;


-- Query to check if a specific customer exists
SELECT 
    * 
FROM 
    customer 
WHERE 
    customer_id = 0;

-- Suspend the task (can be resumed later if needed)
ALTER TASK task_scd_raw SUSPEND;

-- Show tasks again to verify status
SHOW TASKS;