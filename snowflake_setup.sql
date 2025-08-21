-- Snowflake Setup Script for TPC-H Medallion Architecture
-- Run these commands in Snowflake to set up your environment
-- Based on Snowflake's dbt tutorial best practices

-- ===========================================
-- DATABASE AND SCHEMA SETUP
-- ===========================================

-- Create main database for TPC-H medallion architecture
CREATE DATABASE IF NOT EXISTS TPCH_MEDALLION_DB
  COMMENT = 'TPC-H Medallion Architecture - Bronze, Silver, Gold layers';

-- Create schemas for different layers and environments
CREATE SCHEMA IF NOT EXISTS TPCH_MEDALLION_DB.DEV
  COMMENT = 'Development environment for dbt models';

CREATE SCHEMA IF NOT EXISTS TPCH_MEDALLION_DB.PROD  
  COMMENT = 'Production environment for dbt models';

CREATE SCHEMA IF NOT EXISTS TPCH_MEDALLION_DB.BRONZE
  COMMENT = 'Bronze layer - raw data with minimal transformations';

CREATE SCHEMA IF NOT EXISTS TPCH_MEDALLION_DB.SILVER
  COMMENT = 'Silver layer - cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS TPCH_MEDALLION_DB.GOLD
  COMMENT = 'Gold layer - business-ready analytics and KPIs';

-- Create integrations schema for native dbt Projects on Snowflake (optional)
CREATE SCHEMA IF NOT EXISTS TPCH_MEDALLION_DB.INTEGRATIONS
  COMMENT = 'Integration objects for native dbt Projects on Snowflake';

-- ===========================================
-- WAREHOUSE SETUP
-- ===========================================

-- Create dedicated warehouse for TPC-H processing
-- Size based on your data volume and performance requirements
CREATE WAREHOUSE IF NOT EXISTS TPCH_MEDALLION_WH
  WITH 
  WAREHOUSE_SIZE = LARGE
  AUTO_SUSPEND = 300  -- 5 minutes
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for TPC-H medallion architecture dbt processing';

-- Alternative: Create separate warehouses for dev/prod
-- CREATE WAREHOUSE IF NOT EXISTS TPCH_MEDALLION_DEV_WH
--   WITH WAREHOUSE_SIZE = MEDIUM AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;
-- 
-- CREATE WAREHOUSE IF NOT EXISTS TPCH_MEDALLION_PROD_WH  
--   WITH WAREHOUSE_SIZE = LARGE AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

-- ===========================================
-- OPTIONAL: NATIVE DBT PROJECTS ON SNOWFLAKE SETUP
-- ===========================================

-- If you want to use Snowflake's native "dbt Projects on Snowflake" feature,
-- uncomment and configure the following objects:

-- -- API Integration for GitHub (replace with your GitHub URL)
-- CREATE OR REPLACE API INTEGRATION TPCH_DBT_GIT_API_INTEGRATION
--   API_PROVIDER = git_https_api
--   API_ALLOWED_PREFIXES = ('https://github.com/your-github-account')
--   ENABLED = TRUE;

-- -- Network rule for dbt dependencies
-- CREATE OR REPLACE NETWORK RULE DBT_NETWORK_RULE
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = (
--     'hub.getdbt.com',
--     'codeload.github.com'
--   );

-- -- External access integration for dbt packages
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBT_EXT_ACCESS
--   ALLOWED_NETWORK_RULES = (DBT_NETWORK_RULE)
--   ENABLED = TRUE;

-- -- Secret for private GitHub repository (if needed)
-- CREATE OR REPLACE SECRET TPCH_MEDALLION_DB.INTEGRATIONS.GITHUB_SECRET
--   TYPE = password
--   USERNAME = 'your-github-username'
--   PASSWORD = 'your-personal-access-token';

-- ===========================================
-- ROLE AND PERMISSIONS SETUP
-- ===========================================

-- Create dedicated role for dbt operations (optional but recommended)
CREATE ROLE IF NOT EXISTS DBT_TPCH_ROLE
  COMMENT = 'Role for dbt TPC-H medallion architecture operations';

-- Grant necessary permissions to the role
GRANT USAGE ON DATABASE TPCH_MEDALLION_DB TO ROLE DBT_TPCH_ROLE;
GRANT ALL ON SCHEMA TPCH_MEDALLION_DB.DEV TO ROLE DBT_TPCH_ROLE;
GRANT ALL ON SCHEMA TPCH_MEDALLION_DB.PROD TO ROLE DBT_TPCH_ROLE;
GRANT ALL ON SCHEMA TPCH_MEDALLION_DB.BRONZE TO ROLE DBT_TPCH_ROLE;
GRANT ALL ON SCHEMA TPCH_MEDALLION_DB.SILVER TO ROLE DBT_TPCH_ROLE;
GRANT ALL ON SCHEMA TPCH_MEDALLION_DB.GOLD TO ROLE DBT_TPCH_ROLE;
GRANT ALL ON SCHEMA TPCH_MEDALLION_DB.INTEGRATIONS TO ROLE DBT_TPCH_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE TPCH_MEDALLION_WH TO ROLE DBT_TPCH_ROLE;

-- Grant access to sample data (required for our TPC-H source)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE DBT_TPCH_ROLE;

-- Grant role to user (replace 'YOUR_USERNAME' with actual username)
-- GRANT ROLE DBT_TPCH_ROLE TO USER YOUR_USERNAME;

-- ===========================================
-- VERIFICATION QUERIES
-- ===========================================

-- Verify database and schema creation
SHOW DATABASES LIKE 'TPCH_MEDALLION%';
SHOW SCHEMAS IN DATABASE TPCH_MEDALLION_DB;

-- Verify warehouse creation
SHOW WAREHOUSES LIKE 'TPCH_MEDALLION%';

-- Test access to TPC-H sample data
SELECT COUNT(*) as region_count FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;
SELECT COUNT(*) as customer_count FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 1;

-- ===========================================
-- USAGE NOTES
-- ===========================================

-- 1. Update your profiles.yml with these database/warehouse names:
--    database: TPCH_MEDALLION_DB
--    warehouse: TPCH_MEDALLION_WH

-- 2. Choose appropriate warehouse sizes based on your data volume:
--    - TPCH_SF1: MEDIUM warehouse sufficient
--    - TPCH_SF10: LARGE warehouse recommended  
--    - TPCH_SF100+: XLARGE or larger

-- 3. For production, consider:
--    - Separate dev/prod warehouses
--    - Resource monitors for cost control
--    - Time-based auto-suspend policies
--    - Dedicated roles with minimal permissions

-- 4. For native "dbt Projects on Snowflake":
--    - Uncomment the integration objects above
--    - Create workspace in Snowsight
--    - Connect to your GitHub repository
--    - Use the workspace editor for dbt development
