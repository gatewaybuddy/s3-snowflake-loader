# Snowflake ELT Patterns Specification

## Overview

This document defines patterns for promoting data from STAGING tables to DEV/PROD environments in Snowflake. The S3 → Lambda loader intentionally keeps things simple: it just lands data into STAGING tables using `.T` (truncate/reload) and `.A` (append) conventions. All transformation, deduplication, and business logic happens downstream in Snowflake.

**Philosophy**: The loader is dumb, Snowflake is smart.

## Schema Structure

```
DATABASE
├── STAGING        # Raw landing zone (what the loader writes to)
├── DEV            # Transformed data for testing
└── PROD           # Production-ready analytics tables
    └── ANALYTICS  # Alternative name for production schema
```

### Table Naming Convention
- **STAGING**: Raw table names match S3 folder structure
- **DEV/PROD**: Same table names, but with transformations applied
- **_VW suffix**: Views that join/aggregate multiple tables
- **_HIST suffix**: Historical/audit tables

## Promotion Patterns

### 1. Truncate-Reload Pattern

**Use Case**: Small to medium tables (< 10M rows), simple transformations, full refresh acceptable.

**When to Use**:
- Daily batch loads
- Reference/lookup tables
- Master data with infrequent changes
- Tables where full scan is cheap

**Example: CUSTOMERS table**

```sql
-- STAGING → DEV promotion task
CREATE OR REPLACE TASK DEV_CUSTOMERS_PROMOTION
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 8 * * * America/New_York'  -- 8 AM daily
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMERS_STREAM')
AS
BEGIN
  -- Truncate and reload with business transformations
  TRUNCATE TABLE DEV.CUSTOMERS;
  
  INSERT INTO DEV.CUSTOMERS (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    PHONE,
    CREATED_DATE,
    LAST_UPDATED,
    IS_ACTIVE,
    CUSTOMER_TIER,
    DATA_HASH
  )
  SELECT 
    CUSTOMER_ID,
    UPPER(TRIM(CUSTOMER_NAME)) AS CUSTOMER_NAME,
    LOWER(TRIM(EMAIL)) AS EMAIL,
    REGEXP_REPLACE(PHONE, '[^0-9]', '') AS PHONE,
    TRY_CAST(CREATED_DATE AS DATE) AS CREATED_DATE,
    CURRENT_TIMESTAMP() AS LAST_UPDATED,
    COALESCE(IS_ACTIVE, TRUE) AS IS_ACTIVE,
    CASE 
      WHEN ANNUAL_SPEND >= 100000 THEN 'ENTERPRISE'
      WHEN ANNUAL_SPEND >= 10000 THEN 'BUSINESS'
      ELSE 'STANDARD'
    END AS CUSTOMER_TIER,
    HASH(*) AS DATA_HASH
  FROM STAGING.CUSTOMERS
  WHERE CUSTOMER_ID IS NOT NULL
    AND EMAIL IS NOT NULL;
    
  -- Log the promotion
  INSERT INTO ADMIN_DB._PIPELINE.PROMOTION_LOG 
    (TARGET_TABLE, ROWS_PROCESSED, PROMOTION_TIME)
  VALUES ('DEV.CUSTOMERS', @@ROWCOUNT, CURRENT_TIMESTAMP());
END;

-- Create stream to detect changes in staging
CREATE OR REPLACE STREAM CUSTOMERS_STREAM 
  ON TABLE STAGING.CUSTOMERS 
  APPEND_ONLY = TRUE;

-- Enable the task
ALTER TASK DEV_CUSTOMERS_PROMOTION RESUME;
```

**DEV → PROD promotion** (similar pattern with additional validations):

```sql
CREATE OR REPLACE TASK PROD_CUSTOMERS_PROMOTION
  WAREHOUSE = TRANSFORM_WH
  AFTER DEV_CUSTOMERS_PROMOTION  -- Chain after DEV promotion
WHEN (
  SELECT COUNT(*) > 0 
  FROM DEV.CUSTOMERS 
  WHERE LAST_UPDATED > CURRENT_TIMESTAMP() - INTERVAL '1 hour'
)
AS
BEGIN
  -- Data quality checks
  CALL ADMIN_DB._PIPELINE.VALIDATE_TABLE_QUALITY('DEV', 'CUSTOMERS');
  
  -- Promote to production
  CREATE OR REPLACE TABLE PROD.CUSTOMERS AS
  SELECT * FROM DEV.CUSTOMERS;
  
  -- Update metadata
  INSERT INTO ADMIN_DB._PIPELINE.PROMOTION_LOG 
    (TARGET_TABLE, ROWS_PROCESSED, PROMOTION_TIME)
  VALUES ('PROD.CUSTOMERS', @@ROWCOUNT, CURRENT_TIMESTAMP());
END;
```

### 2. MERGE INTO Pattern

**Use Case**: Large tables where full reload is expensive, incremental updates needed.

**When to Use**:
- Tables > 10M rows
- Frequent updates throughout the day
- Network/IO costs are significant
- Need to preserve historical records

**Example: TRANSACTIONS table**

```sql
-- STAGING → DEV incremental merge
CREATE OR REPLACE TASK DEV_TRANSACTIONS_MERGE
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 */4 * * * America/New_York'  -- Every 4 hours
WHEN SYSTEM$STREAM_HAS_DATA('TRANSACTIONS_STREAM')
AS
BEGIN
  -- Use MERGE to handle inserts/updates efficiently
  MERGE INTO DEV.TRANSACTIONS AS target
  USING (
    SELECT 
      TRANSACTION_ID,
      CUSTOMER_ID,
      TRANSACTION_DATE,
      AMOUNT,
      PAYMENT_METHOD,
      STATUS,
      CREATED_AT,
      HASH(*) AS DATA_HASH,
      METADATA$FILENAME AS SOURCE_FILE,
      METADATA$FILE_ROW_NUMBER AS SOURCE_ROW
    FROM STAGING.TRANSACTIONS
    WHERE TRANSACTION_ID IS NOT NULL
      AND CUSTOMER_ID IS NOT NULL
      AND AMOUNT IS NOT NULL
  ) AS source ON target.TRANSACTION_ID = source.TRANSACTION_ID
  
  WHEN MATCHED AND target.DATA_HASH != source.DATA_HASH THEN
    UPDATE SET
      CUSTOMER_ID = source.CUSTOMER_ID,
      TRANSACTION_DATE = source.TRANSACTION_DATE,
      AMOUNT = source.AMOUNT,
      PAYMENT_METHOD = source.PAYMENT_METHOD,
      STATUS = source.STATUS,
      UPDATED_AT = CURRENT_TIMESTAMP(),
      DATA_HASH = source.DATA_HASH,
      SOURCE_FILE = source.SOURCE_FILE,
      SOURCE_ROW = source.SOURCE_ROW
      
  WHEN NOT MATCHED THEN
    INSERT (
      TRANSACTION_ID, CUSTOMER_ID, TRANSACTION_DATE, AMOUNT, 
      PAYMENT_METHOD, STATUS, CREATED_AT, UPDATED_AT, DATA_HASH,
      SOURCE_FILE, SOURCE_ROW
    )
    VALUES (
      source.TRANSACTION_ID, source.CUSTOMER_ID, source.TRANSACTION_DATE,
      source.AMOUNT, source.PAYMENT_METHOD, source.STATUS,
      source.CREATED_AT, CURRENT_TIMESTAMP(), source.DATA_HASH,
      source.SOURCE_FILE, source.SOURCE_ROW
    );
    
  -- Log merge results
  INSERT INTO ADMIN_DB._PIPELINE.MERGE_LOG 
    (TARGET_TABLE, ROWS_INSERTED, ROWS_UPDATED, MERGE_TIME)
  VALUES ('DEV.TRANSACTIONS', 
          (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE $1 = 'INSERTED'),
          (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE $1 = 'UPDATED'),
          CURRENT_TIMESTAMP());
END;

-- Create stream for change detection
CREATE OR REPLACE STREAM TRANSACTIONS_STREAM 
  ON TABLE STAGING.TRANSACTIONS;
```

### 3. Insert-Only Append Pattern

**Use Case**: Event logs, audit trails, time-series data where records are never updated.

**When to Use**:
- Event/log data
- Time-series metrics
- Audit trails
- Immutable records

**Example: EVENTS table**

```sql
-- STAGING → DEV event log promotion
CREATE OR REPLACE TASK DEV_EVENTS_APPEND
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 */1 * * * America/New_York'  -- Every hour
WHEN SYSTEM$STREAM_HAS_DATA('EVENTS_STREAM')
AS
BEGIN
  -- Simple append with deduplication
  INSERT INTO DEV.EVENTS (
    EVENT_ID,
    EVENT_TYPE,
    USER_ID,
    SESSION_ID,
    EVENT_TIMESTAMP,
    EVENT_DATA,
    PROCESSED_AT,
    SOURCE_FILE,
    SOURCE_ROW
  )
  SELECT DISTINCT
    EVENT_ID,
    EVENT_TYPE,
    USER_ID,
    SESSION_ID,
    TRY_CAST(EVENT_TIMESTAMP AS TIMESTAMP_NTZ) AS EVENT_TIMESTAMP,
    PARSE_JSON(EVENT_DATA) AS EVENT_DATA,
    CURRENT_TIMESTAMP() AS PROCESSED_AT,
    METADATA$FILENAME AS SOURCE_FILE,
    METADATA$FILE_ROW_NUMBER AS SOURCE_ROW
  FROM STAGING.EVENTS
  WHERE EVENT_ID IS NOT NULL
    AND EVENT_TIMESTAMP IS NOT NULL
    -- Deduplication: only process events not already in DEV
    AND EVENT_ID NOT IN (
      SELECT EVENT_ID 
      FROM DEV.EVENTS 
      WHERE PROCESSED_AT > CURRENT_TIMESTAMP() - INTERVAL '7 days'
    );
    
  -- Partition management: drop old partitions if needed
  CALL ADMIN_DB._PIPELINE.MANAGE_EVENT_PARTITIONS('DEV.EVENTS', 90); -- Keep 90 days
END;

-- Stream for new events only
CREATE OR REPLACE STREAM EVENTS_STREAM 
  ON TABLE STAGING.EVENTS 
  APPEND_ONLY = TRUE;
```

### 4. SCD Type 2 Pattern (Slowly Changing Dimensions)

**Use Case**: Track historical changes to dimensional data while preserving history.

**When to Use**:
- Customer information that changes over time
- Product catalogs with price history
- Employee records
- Any dimension requiring historical accuracy

**Example: DEPARTMENTS table with history tracking**

```sql
-- STAGING → DEV SCD Type 2 implementation
CREATE OR REPLACE TASK DEV_DEPARTMENTS_SCD2
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- 6 AM daily
WHEN SYSTEM$STREAM_HAS_DATA('DEPARTMENTS_STREAM')
AS
DECLARE
  batch_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
  -- Step 1: Close out changed records
  UPDATE DEV.DEPARTMENTS_HIST 
  SET 
    EFFECTIVE_END_DATE = batch_timestamp,
    IS_CURRENT = FALSE
  WHERE DEPARTMENT_ID IN (
    SELECT DISTINCT s.DEPARTMENT_ID
    FROM STAGING.DEPARTMENTS s
    INNER JOIN DEV.DEPARTMENTS_HIST d 
      ON s.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.IS_CURRENT = TRUE
      AND HASH(s.DEPARTMENT_NAME, s.MANAGER_ID, s.BUDGET, s.LOCATION) != d.DATA_HASH
  );
  
  -- Step 2: Insert new versions for changed records
  INSERT INTO DEV.DEPARTMENTS_HIST (
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    MANAGER_ID,
    BUDGET,
    LOCATION,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_CURRENT,
    DATA_HASH,
    SOURCE_FILE,
    CREATED_AT
  )
  SELECT 
    s.DEPARTMENT_ID,
    s.DEPARTMENT_NAME,
    s.MANAGER_ID,
    s.BUDGET,
    s.LOCATION,
    batch_timestamp AS EFFECTIVE_START_DATE,
    '9999-12-31'::DATE AS EFFECTIVE_END_DATE,
    TRUE AS IS_CURRENT,
    HASH(s.DEPARTMENT_NAME, s.MANAGER_ID, s.BUDGET, s.LOCATION) AS DATA_HASH,
    METADATA$FILENAME AS SOURCE_FILE,
    CURRENT_TIMESTAMP() AS CREATED_AT
  FROM STAGING.DEPARTMENTS s
  WHERE s.DEPARTMENT_ID IN (
    -- Changed records
    SELECT DISTINCT s2.DEPARTMENT_ID
    FROM STAGING.DEPARTMENTS s2
    INNER JOIN DEV.DEPARTMENTS_HIST d 
      ON s2.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.IS_CURRENT = TRUE
      AND HASH(s2.DEPARTMENT_NAME, s2.MANAGER_ID, s2.BUDGET, s2.LOCATION) != d.DATA_HASH
  );
  
  -- Step 3: Insert completely new departments
  INSERT INTO DEV.DEPARTMENTS_HIST (
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    MANAGER_ID,
    BUDGET,
    LOCATION,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_CURRENT,
    DATA_HASH,
    SOURCE_FILE,
    CREATED_AT
  )
  SELECT 
    s.DEPARTMENT_ID,
    s.DEPARTMENT_NAME,
    s.MANAGER_ID,
    s.BUDGET,
    s.LOCATION,
    batch_timestamp AS EFFECTIVE_START_DATE,
    '9999-12-31'::DATE AS EFFECTIVE_END_DATE,
    TRUE AS IS_CURRENT,
    HASH(s.DEPARTMENT_NAME, s.MANAGER_ID, s.BUDGET, s.LOCATION) AS DATA_HASH,
    METADATA$FILENAME AS SOURCE_FILE,
    CURRENT_TIMESTAMP() AS CREATED_AT
  FROM STAGING.DEPARTMENTS s
  WHERE s.DEPARTMENT_ID NOT IN (
    SELECT DISTINCT DEPARTMENT_ID FROM DEV.DEPARTMENTS_HIST
  );
  
  -- Step 4: Create current view
  CREATE OR REPLACE VIEW DEV.DEPARTMENTS AS
  SELECT 
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    MANAGER_ID,
    BUDGET,
    LOCATION,
    EFFECTIVE_START_DATE,
    SOURCE_FILE,
    CREATED_AT
  FROM DEV.DEPARTMENTS_HIST 
  WHERE IS_CURRENT = TRUE;
END;

-- Create the historical table structure
CREATE TABLE IF NOT EXISTS DEV.DEPARTMENTS_HIST (
  DEPARTMENT_ID       VARCHAR(20)     NOT NULL,
  DEPARTMENT_NAME     VARCHAR(100)    NOT NULL,
  MANAGER_ID          VARCHAR(20),
  BUDGET              NUMBER(15,2),
  LOCATION            VARCHAR(100),
  EFFECTIVE_START_DATE DATE           NOT NULL,
  EFFECTIVE_END_DATE  DATE            NOT NULL,
  IS_CURRENT          BOOLEAN         NOT NULL DEFAULT TRUE,
  DATA_HASH           VARCHAR(64)     NOT NULL,
  SOURCE_FILE         VARCHAR(500),
  CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
  
  -- Performance optimization
  PRIMARY KEY (DEPARTMENT_ID, EFFECTIVE_START_DATE)
);

-- Stream for change detection
CREATE OR REPLACE STREAM DEPARTMENTS_STREAM 
  ON TABLE STAGING.DEPARTMENTS;
```

## Snowflake Features Leverage Guide

### 1. Streams (Change Data Capture)

Streams track changes to tables and enable event-driven processing:

```sql
-- Basic stream for all changes
CREATE STREAM my_table_stream ON TABLE STAGING.MY_TABLE;

-- Append-only stream (insert-only, more efficient for logs)
CREATE STREAM events_stream ON TABLE STAGING.EVENTS APPEND_ONLY = TRUE;

-- Check if stream has data (use in WHEN clauses)
SELECT SYSTEM$STREAM_HAS_DATA('my_table_stream');

-- Consume stream data
SELECT METADATA$ACTION, METADATA$ISUPDATE, * 
FROM my_table_stream
WHERE METADATA$ACTION = 'INSERT';
```

### 2. Tasks (Scheduled Processing)

Tasks provide serverless job scheduling:

```sql
-- Time-based schedule
CREATE TASK my_hourly_task
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour
AS
  CALL my_stored_procedure();

-- Event-driven task
CREATE TASK my_data_task
  WAREHOUSE = TRANSFORM_WH
  AFTER parent_task  -- Chain dependency
WHEN SYSTEM$STREAM_HAS_DATA('my_stream')
AS
  INSERT INTO target SELECT * FROM source_stream;

-- Start the task
ALTER TASK my_data_task RESUME;

-- Monitor task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'MY_DATA_TASK'
ORDER BY SCHEDULED_TIME DESC;
```

### 3. Stored Procedures (Complex Logic)

Encapsulate complex transformation logic:

```sql
CREATE OR REPLACE PROCEDURE VALIDATE_TABLE_QUALITY(
  schema_name VARCHAR, 
  table_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
DECLARE
  null_count NUMBER;
  duplicate_count NUMBER;
  result_msg VARCHAR;
BEGIN
  -- Check for excessive nulls
  EXECUTE IMMEDIATE 
    'SELECT COUNT(*) FROM ' || schema_name || '.' || table_name || 
    ' WHERE primary_key_column IS NULL'
  INTO null_count;
  
  -- Check for duplicates
  EXECUTE IMMEDIATE 
    'SELECT COUNT(*) - COUNT(DISTINCT primary_key_column) FROM ' || 
    schema_name || '.' || table_name
  INTO duplicate_count;
  
  -- Validation logic
  IF null_count > 0 THEN
    RAISE EXCEPTION 'Table validation failed: % null primary keys found', null_count;
  END IF;
  
  IF duplicate_count > 0 THEN
    RAISE EXCEPTION 'Table validation failed: % duplicate records found', duplicate_count;
  END IF;
  
  result_msg := 'Validation passed: ' || schema_name || '.' || table_name;
  INSERT INTO ADMIN_DB._PIPELINE.VALIDATION_LOG 
    (TABLE_NAME, VALIDATION_TIME, RESULT)
  VALUES (schema_name || '.' || table_name, CURRENT_TIMESTAMP(), result_msg);
  
  RETURN result_msg;
END;
```

### 4. QUALIFY + ROW_NUMBER for Deduplication

Efficient deduplication using window functions:

```sql
-- Remove duplicates, keeping most recent record
INSERT INTO DEV.ANIMALS
SELECT 
  ANIMAL_ID,
  ANIMAL_NAME,
  SPECIES,
  AGE,
  LAST_UPDATED,
  METADATA$FILENAME AS SOURCE_FILE
FROM STAGING.ANIMALS
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ANIMAL_ID 
  ORDER BY LAST_UPDATED DESC, METADATA$FILE_ROW_NUMBER DESC
) = 1;

-- Keep only records that changed since last load
INSERT INTO DEV.CUSTOMER_UPDATES
SELECT *
FROM STAGING.CUSTOMERS
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CUSTOMER_ID, HASH(*) 
  ORDER BY METADATA$FILE_ROW_NUMBER
) = 1
  AND HASH(*) NOT IN (
    SELECT DATA_HASH FROM DEV.CUSTOMERS WHERE CUSTOMER_ID = STAGING.CUSTOMERS.CUSTOMER_ID
  );
```

## Change Detection Strategies

### 1. HASH(*) Comparison

Most efficient for detecting actual data changes:

```sql
-- Add hash column during staging load
ALTER TABLE STAGING.CUSTOMERS ADD COLUMN DATA_HASH VARCHAR(64);

-- Update hash during transformation
UPDATE STAGING.CUSTOMERS 
SET DATA_HASH = HASH(CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, ADDRESS);

-- Detect changes
SELECT s.*
FROM STAGING.CUSTOMERS s
LEFT JOIN DEV.CUSTOMERS d ON s.CUSTOMER_ID = d.CUSTOMER_ID
WHERE d.CUSTOMER_ID IS NULL  -- New records
   OR s.DATA_HASH != d.DATA_HASH;  -- Changed records
```

### 2. Snowflake Streams

Built-in change data capture:

```sql
-- Create stream
CREATE STREAM customer_changes ON TABLE STAGING.CUSTOMERS;

-- Query changes
SELECT 
  METADATA$ACTION as action_type,  -- INSERT, DELETE, UPDATE
  METADATA$ISUPDATE as is_update,  -- TRUE for updates
  *
FROM customer_changes
WHERE METADATA$ACTION != 'DELETE';  -- Typically ignore deletes from staging
```

### 3. File-level Lineage

Track which source file each record came from:

```sql
-- During COPY INTO, capture file metadata
COPY INTO STAGING.TRANSACTIONS
FROM @my_stage/transactions/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Use metadata in transformations
INSERT INTO DEV.TRANSACTIONS
SELECT 
  *,
  METADATA$FILENAME as source_file,
  METADATA$FILE_ROW_NUMBER as source_row,
  CURRENT_TIMESTAMP() as processed_at
FROM STAGING.TRANSACTIONS
-- Only process new files
WHERE METADATA$FILENAME NOT IN (
  SELECT DISTINCT SOURCE_FILE 
  FROM DEV.TRANSACTIONS 
  WHERE PROCESSED_AT > CURRENT_TIMESTAMP() - INTERVAL '1 day'
);
```

## Decision Matrix: When to Use Each Pattern

| Data Characteristics | Volume | Change Frequency | Business Need | Recommended Pattern |
|---------------------|--------|------------------|---------------|-------------------|
| Reference/lookup tables | < 1M rows | Daily/weekly | Current state only | **Truncate-Reload** |
| Customer master data | 1M-50M rows | Multiple times/day | Current + some history | **MERGE INTO** |
| Transactional data | 10M+ rows | Real-time/frequent | Current + audit trail | **MERGE INTO** |
| Event/log data | Any | Continuous | Append-only, never update | **Insert-Only** |
| Dimensional data | < 10M rows | Infrequent | Full historical tracking | **SCD Type 2** |
| Configuration/settings | < 100K rows | Ad-hoc | Current state only | **Truncate-Reload** |
| Time-series metrics | 100M+ rows | Real-time | Append-only | **Insert-Only** |
| Product catalog | 1M-10M rows | Daily | Price/attr history | **SCD Type 2** |

### Pattern Selection Flowchart

```
Start
  │
  ▼
Are records ever updated after initial insert?
  │
  ├─ NO → Insert-Only Append Pattern
  │
  ▼
  YES
  │
  ▼
Do you need to track historical changes?
  │
  ├─ YES → SCD Type 2 Pattern
  │
  ▼
  NO (current state only)
  │
  ▼
Is the table size > 10M rows OR are updates very frequent?
  │
  ├─ YES → MERGE INTO Pattern
  │
  ▼
  NO
  │
  ▼
Truncate-Reload Pattern
```

## Performance Considerations

### 1. Warehouse Sizing

```sql
-- Create dedicated warehouse for ELT tasks
CREATE WAREHOUSE TRANSFORM_WH WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60  -- Suspend after 1 minute
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for ELT promotion tasks';

-- Use different sizes based on data volume
-- XSMALL: < 1M rows
-- SMALL:  1M-10M rows  
-- MEDIUM: 10M-100M rows
-- LARGE:  100M+ rows
```

### 2. Clustering Keys

```sql
-- Add clustering for large tables
ALTER TABLE DEV.TRANSACTIONS 
CLUSTER BY (TRANSACTION_DATE, CUSTOMER_ID);

-- Monitor clustering effectiveness
SELECT SYSTEM$CLUSTERING_INFORMATION('DEV.TRANSACTIONS');
```

### 3. Task Scheduling Optimization

```sql
-- Spread tasks across time to avoid warehouse contention
-- Small tables: Every 4-6 hours
-- Medium tables: 2-3 times per day
-- Large tables: Once daily during off-peak

-- Chain related tasks
CREATE TASK staging_to_dev_task ...;
CREATE TASK dev_to_prod_task AFTER staging_to_dev_task ...;
```

## Monitoring and Alerting

### Essential Monitoring Queries

```sql
-- Task execution history
SELECT 
  NAME,
  STATE,
  SCHEDULED_TIME,
  COMPLETED_TIME,
  RETURN_VALUE,
  ERROR_CODE,
  ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE SCHEDULED_TIME > CURRENT_TIMESTAMP() - INTERVAL '24 hours'
  AND STATE != 'SUCCEEDED'
ORDER BY SCHEDULED_TIME DESC;

-- Stream lag monitoring
SELECT 
  TABLE_NAME,
  STREAM_NAME,
  BYTES,
  ROWS
FROM INFORMATION_SCHEMA.STREAMS
WHERE BYTES > 0;  -- Streams with pending data

-- Data freshness check
SELECT 
  TABLE_SCHEMA,
  TABLE_NAME,
  LAST_ALTERED,
  ROW_COUNT,
  BYTES
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('DEV', 'PROD')
  AND LAST_ALTERED < CURRENT_TIMESTAMP() - INTERVAL '25 hours'  -- Expected daily refresh
ORDER BY LAST_ALTERED;
```

### Alerting Setup

```sql
-- Create procedure for alerts
CREATE OR REPLACE PROCEDURE SEND_ELT_ALERTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
DECLARE
  failed_tasks CURSOR FOR 
    SELECT NAME, ERROR_MESSAGE 
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
    WHERE SCHEDULED_TIME > CURRENT_TIMESTAMP() - INTERVAL '1 hour'
      AND STATE = 'FAILED';
BEGIN
  FOR task_record IN failed_tasks DO
    -- Send to external alerting system
    CALL SYSTEM$SEND_EMAIL(
      'ELT Alert: Task Failed',
      'Task ' || task_record.NAME || ' failed: ' || task_record.ERROR_MESSAGE
    );
  END FOR;
  RETURN 'Alerts processed';
END;

-- Schedule alert checking
CREATE TASK ELT_ALERT_TASK
  WAREHOUSE = ADMIN_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour
AS
  CALL SEND_ELT_ALERTS();
```

## Example Implementation: Complete ANIMALS Table Flow

Here's a complete example implementing all patterns for the ANIMALS table:

```sql
-- 1. Create the schemas and tables
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS DEV;
CREATE SCHEMA IF NOT EXISTS PROD;

-- 2. Staging table (populated by S3 loader)
CREATE TABLE IF NOT EXISTS STAGING.ANIMALS (
  ANIMAL_ID       VARCHAR(20)     NOT NULL,
  ANIMAL_NAME     VARCHAR(100),
  SPECIES         VARCHAR(50),
  AGE             NUMBER(3),
  WEIGHT_KG       NUMBER(6,2),
  LAST_UPDATED    TIMESTAMP_NTZ,
  IS_ACTIVE       BOOLEAN
);

-- 3. DEV table (business transformations applied)
CREATE TABLE IF NOT EXISTS DEV.ANIMALS (
  ANIMAL_ID       VARCHAR(20)     NOT NULL,
  ANIMAL_NAME     VARCHAR(100),
  SPECIES         VARCHAR(50),
  AGE             NUMBER(3),
  WEIGHT_KG       NUMBER(6,2),
  WEIGHT_LBS      NUMBER(7,2),     -- Derived field
  AGE_CATEGORY    VARCHAR(20),     -- Derived field
  LAST_UPDATED    TIMESTAMP_NTZ,
  IS_ACTIVE       BOOLEAN,
  DATA_HASH       VARCHAR(64),     -- For change detection
  SOURCE_FILE     VARCHAR(500),    -- Lineage
  PROCESSED_AT    TIMESTAMP_NTZ,
  
  PRIMARY KEY (ANIMAL_ID)
);

-- 4. Stream to detect changes
CREATE OR REPLACE STREAM ANIMALS_STREAM 
  ON TABLE STAGING.ANIMALS;

-- 5. Promotion task using MERGE pattern
CREATE OR REPLACE TASK DEV_ANIMALS_PROMOTION
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
WHEN SYSTEM$STREAM_HAS_DATA('ANIMALS_STREAM')
AS
BEGIN
  MERGE INTO DEV.ANIMALS AS target
  USING (
    SELECT 
      ANIMAL_ID,
      UPPER(TRIM(ANIMAL_NAME)) AS ANIMAL_NAME,
      UPPER(TRIM(SPECIES)) AS SPECIES,
      AGE,
      WEIGHT_KG,
      ROUND(WEIGHT_KG * 2.20462, 2) AS WEIGHT_LBS,  -- Convert to pounds
      CASE 
        WHEN AGE < 1 THEN 'BABY'
        WHEN AGE < 5 THEN 'YOUNG'
        WHEN AGE < 15 THEN 'ADULT'
        ELSE 'SENIOR'
      END AS AGE_CATEGORY,
      LAST_UPDATED,
      COALESCE(IS_ACTIVE, TRUE) AS IS_ACTIVE,
      HASH(ANIMAL_ID, ANIMAL_NAME, SPECIES, AGE, WEIGHT_KG, IS_ACTIVE) AS DATA_HASH,
      METADATA$FILENAME AS SOURCE_FILE,
      CURRENT_TIMESTAMP() AS PROCESSED_AT
    FROM STAGING.ANIMALS
    WHERE ANIMAL_ID IS NOT NULL
  ) AS source ON target.ANIMAL_ID = source.ANIMAL_ID
  
  WHEN MATCHED AND target.DATA_HASH != source.DATA_HASH THEN
    UPDATE SET
      ANIMAL_NAME = source.ANIMAL_NAME,
      SPECIES = source.SPECIES,
      AGE = source.AGE,
      WEIGHT_KG = source.WEIGHT_KG,
      WEIGHT_LBS = source.WEIGHT_LBS,
      AGE_CATEGORY = source.AGE_CATEGORY,
      LAST_UPDATED = source.LAST_UPDATED,
      IS_ACTIVE = source.IS_ACTIVE,
      DATA_HASH = source.DATA_HASH,
      SOURCE_FILE = source.SOURCE_FILE,
      PROCESSED_AT = source.PROCESSED_AT
      
  WHEN NOT MATCHED THEN
    INSERT (
      ANIMAL_ID, ANIMAL_NAME, SPECIES, AGE, WEIGHT_KG, WEIGHT_LBS,
      AGE_CATEGORY, LAST_UPDATED, IS_ACTIVE, DATA_HASH, SOURCE_FILE, PROCESSED_AT
    )
    VALUES (
      source.ANIMAL_ID, source.ANIMAL_NAME, source.SPECIES, source.AGE,
      source.WEIGHT_KG, source.WEIGHT_LBS, source.AGE_CATEGORY,
      source.LAST_UPDATED, source.IS_ACTIVE, source.DATA_HASH,
      source.SOURCE_FILE, source.PROCESSED_AT
    );
    
  -- Log the operation
  INSERT INTO ADMIN_DB._PIPELINE.PROMOTION_LOG 
    (TABLE_NAME, ROWS_PROCESSED, PROMOTION_TIME, OPERATION_TYPE)
  VALUES ('DEV.ANIMALS', 
          (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))),
          CURRENT_TIMESTAMP(), 
          'MERGE');
END;

-- 6. DEV → PROD promotion with validation
CREATE OR REPLACE TASK PROD_ANIMALS_PROMOTION
  WAREHOUSE = TRANSFORM_WH
  AFTER DEV_ANIMALS_PROMOTION
WHEN (
  SELECT COUNT(*) > 0 
  FROM DEV.ANIMALS 
  WHERE PROCESSED_AT > CURRENT_TIMESTAMP() - INTERVAL '8 hours'
)
AS
BEGIN
  -- Quality checks
  DECLARE
    row_count NUMBER;
    null_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO row_count FROM DEV.ANIMALS;
    SELECT COUNT(*) INTO null_count FROM DEV.ANIMALS WHERE ANIMAL_NAME IS NULL;
    
    IF row_count = 0 THEN
      RAISE EXCEPTION 'DEV.ANIMALS is empty - blocking promotion';
    END IF;
    
    IF null_count > row_count * 0.1 THEN  -- > 10% nulls
      RAISE EXCEPTION 'Too many null animal names: %', null_count;
    END IF;
  END;
  
  -- Promote to production
  CREATE OR REPLACE TABLE PROD.ANIMALS AS
  SELECT * FROM DEV.ANIMALS;
  
  -- Update promotion log
  INSERT INTO ADMIN_DB._PIPELINE.PROMOTION_LOG 
    (TABLE_NAME, ROWS_PROCESSED, PROMOTION_TIME, OPERATION_TYPE)
  VALUES ('PROD.ANIMALS', row_count, CURRENT_TIMESTAMP(), 'TRUNCATE_RELOAD');
END;

-- 7. Enable tasks
ALTER TASK DEV_ANIMALS_PROMOTION RESUME;
ALTER TASK PROD_ANIMALS_PROMOTION RESUME;

-- 8. Create monitoring view
CREATE OR REPLACE VIEW ADMIN_DB._PIPELINE.ANIMALS_STATUS AS
SELECT 
  'STAGING' as schema_name,
  COUNT(*) as row_count,
  MAX(LAST_UPDATED) as latest_update
FROM STAGING.ANIMALS
UNION ALL
SELECT 
  'DEV' as schema_name,
  COUNT(*) as row_count,
  MAX(PROCESSED_AT) as latest_update
FROM DEV.ANIMALS
UNION ALL
SELECT 
  'PROD' as schema_name,
  COUNT(*) as row_count,
  MAX(PROCESSED_AT) as latest_update
FROM PROD.ANIMALS;
```

## Best Practices Summary

1. **Start simple**: Use truncate-reload for small tables, optimize to MERGE later if needed
2. **Monitor data freshness**: Set up alerts when tables haven't updated in expected timeframes
3. **Use streams**: More efficient than timestamp-based change detection
4. **Hash for change detection**: `HASH(*)` is faster than column-by-column comparison
5. **Chain tasks**: Use AFTER clause to create dependencies
6. **Size warehouses appropriately**: Don't over-provision, use auto-suspend
7. **Add data lineage**: Track source files and processing timestamps
8. **Implement quality checks**: Validate data before promoting to PROD
9. **Log everything**: Track all operations for troubleshooting
10. **Test incrementally**: Start with DEV schema, then promote to PROD

This specification provides a robust foundation for implementing ELT patterns in Snowflake while maintaining the simplicity principle of the S3 loader architecture.