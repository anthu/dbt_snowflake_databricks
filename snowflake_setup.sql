-- Minimal Snowflake Setup for TPC-H Medallion Architecture
-- dbt will handle schema creation automatically

-- ===========================================
-- DATABASE SETUP
-- ===========================================

-- Create database for dbt model outputs
CREATE DATABASE IF NOT EXISTS TPCH_MEDALLION_DB
  COMMENT = 'TPC-H Medallion Architecture - dbt will create schemas automatically';

-- ===========================================
-- BASIC PERMISSIONS
-- ===========================================

-- Grant access to sample data (source for our models)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE PUBLIC;

-- Grant permissions on output database (dbt will create schemas)
GRANT USAGE ON DATABASE TPCH_MEDALLION_DB TO ROLE PUBLIC;
GRANT CREATE SCHEMA ON DATABASE TPCH_MEDALLION_DB TO ROLE PUBLIC;

-- Grant usage on default warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PUBLIC;

-- ===========================================
-- VERIFICATION
-- ===========================================

-- Verify access to source data
SELECT COUNT(*) as region_count FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Verify database creation
SHOW DATABASES LIKE 'TPCH_MEDALLION%';

-- ===========================================
-- USAGE NOTES
-- ===========================================

-- ✅ What this setup provides:
-- • Creates output database only
-- • dbt handles all schema creation automatically
-- • Uses SNOWFLAKE_SAMPLE_DATA for source data
-- • Uses default COMPUTE_WH warehouse