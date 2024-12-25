-- SCD Type 2: Keeping Historical Data (With Versioning - Type Boolean)

-- Set Role and Warehouse
USE ROLE sysadmin;

USE WAREHOUSE compute_wh;

-- Set Database and Create Schema
USE DATABASE scd_demo;

CREATE SCHEMA IF NOT EXISTS scd2;
USE SCHEMA scd2;

-- Create customer history table (SCD Type 2 with historical tracking)
CREATE OR REPLACE TABLE customer_history (
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    end_time TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    is_current BOOLEAN DEFAULT TRUE
);


-- "customer" TABLE and "customer_table_changes" STREAM already created in "scd1" SCHEMA


-- Create a view to capture customer change data (for historical tracking)
CREATE OR REPLACE VIEW v_customer_change_data AS
SELECT 
    customer_id, first_name, last_name, email, street, city, state, country, 
    start_time, end_time, is_current, 'I' AS dml_type
FROM (
    SELECT 
        customer_id, first_name, last_name, email, street, city, state, country, 
        update_timestamp AS start_time,
        LAG(update_timestamp) OVER (PARTITION BY customer_id 
        ORDER BY update_timestamp DESC) AS end_time_raw,
        CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE end_time_raw END AS end_time,
        CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM 
        scd1.customer_table_changes
    WHERE 
        metadata$action = 'INSERT'
        AND metadata$isupdate = 'FALSE'
)
UNION
SELECT 
    customer_id, first_name, last_name, email, street, city, state, country, 
    start_time, end_time, is_current, 'U' AS dml_type
FROM (
    SELECT 
        customer_id, first_name, last_name, email, street, city, state, country, 
        update_timestamp AS start_time,
        LAG(update_timestamp) OVER (PARTITION BY customer_id 
        ORDER BY update_timestamp DESC) AS end_time_raw,
        CASE WHEN end_time_raw IS NULL THEN '9999-12-31'::TIMESTAMP_NTZ ELSE end_time_raw END AS end_time,
        CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM 
        customer_history
    WHERE customer_id IN (SELECT DISTINCT customer_id FROM scd1.customer_table_changes
                        WHERE metadata$action = 'DELETE' AND metadata$isupdate = 'TRUE')
        AND is_current = TRUE
)
UNION
SELECT 
    ctc.customer_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    ch.start_time, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, NULL, 'D' AS dml_type
FROM 
    customer_history ch
INNER JOIN 
    scd1.customer_table_changes ctc
    ON ch.customer_id = ctc.customer_id
WHERE 
    ctc.metadata$action = 'DELETE'
    AND ctc.metadata$isupdate = 'FALSE'
    AND ch.is_current = TRUE;


-- Task to Merge changes into customer history table (SCD Type 2 logic)
CREATE OR REPLACE TASK task_scd_hist
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
MERGE INTO customer_history ch
USING v_customer_change_data ccd
    ON ch.customer_id = ccd.customer_id
    AND ch.start_time = ccd.start_time
WHEN MATCHED AND ccd.dml_type = 'U' THEN UPDATE
    SET ch.end_time = ccd.end_time,
        ch.is_current = FALSE
WHEN MATCHED AND ccd.dml_type = 'D' THEN UPDATE
    SET ch.end_time = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,  -- Update end_time to current timestamp
        ch.is_current = FALSE
WHEN NOT MATCHED AND ccd.dml_type = 'I' THEN INSERT 
        (customer_id, first_name, last_name, email, street, city, state, country, start_time, end_time, is_current)
    VALUES 
        (ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email, ccd.street, ccd.city, ccd.state, ccd.country, ccd.start_time, ccd.end_time, ccd.is_current);


/*
-- OR Automatically as data is updated:

CREATE OR REPLACE TASK task_scd_hist
    WAREHOUSE = COMPUTE_WH
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
    AFTER task_scd_raw  -- Ensures Task 2 runs immediately after Task 1
    AS
<SQL statement>
*/


-- Suspend the task when not in use
ALTER TASK task_scd_hist SUSPEND;


-- Query current and historical data

SELECT * FROM customer_history WHERE is_current = TRUE;

SELECT * FROM customer_history WHERE is_current = FALSE;


--======================================================================
-- Check CDC and SCD manually after inserting some data
--======================================================================

-- Show all streams
SHOW STREAMS;

-- View changes in the customer table
SELECT * 
FROM customer_table_changes;

-- Insert a new customer record
INSERT INTO customer 
VALUES (
    223136, 
    'Jessica', 
    'Arnold', 
    'tanner39@smith.com', 
    '595 Benjamin Forge Suite 124', 
    'Michaelstad', 
    'Connecticut', 
    'Cape Verde', 
    CURRENT_TIMESTAMP()
);

-- Update the first name and timestamp for customer ID 72
UPDATE customer 
SET 
    FIRST_NAME = 'Jessica', 
    update_timestamp = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ 
WHERE 
    customer_id = 72;

-- Delete customer record with ID 73
DELETE FROM customer 
WHERE 
    customer_id = 73;

-- View customer history for specific IDs
SELECT * 
FROM customer_history 
WHERE 
    customer_id IN (72, 73, 223136);

-- Check changes in the customer table after DML operations
SELECT * 
FROM customer_table_changes;

-- Verify the current state of customer records with specific IDs
SELECT * 
FROM customer 
WHERE 
    customer_id IN (72, 73, 223136);