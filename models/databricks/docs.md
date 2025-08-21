# TPC-H Medallion Architecture Documentation - Databricks Implementation

## Overview
This dbt project implements a medallion architecture using Databricks Unity Catalog and Delta Lake, providing a complete data pipeline from raw data to business-ready analytics. The project leverages Databricks' lakehouse architecture with Delta Lake for ACID transactions, schema evolution, and time travel capabilities.

## Architecture Layers

### Bronze Layer (Raw Data)
The Bronze layer contains raw data from TPC-H benchmark dataset with minimal transformations:

{% docs databricks_bronze_layer %}
**Purpose**: Store raw data from Unity Catalog with Delta Lake format and metadata tracking

**Characteristics**:
- 1:1 mapping from Unity Catalog source tables
- Delta Lake format for ACID transactions and performance
- Configurable catalog and schema via variables
- Metadata columns for audit trail and source tracking
- Basic data type preservation with Spark SQL optimizations
- No business logic applied

**Delta Lake Benefits**:
- ACID transactions ensure data consistency
- Schema evolution support for changing data structures
- Time travel for historical data analysis
- Automatic optimization and compaction
- Z-ordering for query performance

**Configuration**:
The bronze layer sources from Unity Catalog tables configurable via variables:
- `databricks_tpch_catalog`: Catalog name (default: 'samples')
- `databricks_tpch_schema`: Schema name (default: 'tpch')

**Tables**:
- bronze_region
- bronze_nation  
- bronze_customer
- bronze_supplier
- bronze_part
- bronze_partsupp
- bronze_orders
- bronze_lineitem
{% enddocs %}

### Silver Layer (Cleaned Data)
The Silver layer contains cleaned, validated, and enriched data:

{% docs databricks_silver_layer %}
**Purpose**: Provide clean, validated data with business logic applied using Spark SQL

**Characteristics**:
- Data quality validations and flags
- Standardized formatting using Spark SQL functions
- Derived business attributes and categorizations
- Foreign key relationships validated
- Delta Lake format for performance and reliability
- Spark SQL optimizations for date and string functions

**Key Transformations**:
- Name standardization (UPPER case)
- Address and phone formatting
- Business categorizations (customer tiers, price categories)
- Calculated fields (lead times, performance indicators)
- Cross-reference validations with optimized joins
- Date calculations using Spark SQL functions

**Spark SQL Adaptations**:
- `year()`, `quarter()`, `month()` instead of `extract()`
- `split()` function for string parsing
- `datediff()` with reversed parameter order
- `current_timestamp()` for timestamp generation
- Delta Lake specific optimizations
{% enddocs %}

### Gold Layer (Business Data)
The Gold layer contains aggregated, business-ready data for analytics:

{% docs databricks_gold_layer %}
**Purpose**: Deliver business-ready metrics and KPIs optimized for Databricks analytics

**Characteristics**:
- Pre-aggregated data for Photon acceleration
- Business metrics and KPIs for BI tools
- Time-series analysis with Spark SQL window functions
- Customer and supplier analytics
- Regional performance metrics with geographic analysis
- Delta Lake format for fast analytical queries

**Key Models**:
- Customer analytics and segmentation
- Sales performance metrics with time-series analysis
- Supplier performance tracking
- Regional trade analysis
- Monthly KPI dashboards with growth calculations

**Databricks Optimizations**:
- Delta Lake tables for fast analytical queries
- Spark SQL window functions for time-series analysis
- Optimized aggregations for large datasets
- Support for Photon acceleration
- Integration with Databricks SQL and BI tools
{% enddocs %}

## Data Sources and Integration

### Unity Catalog Integration
{% docs unity_catalog_integration %}
The Databricks implementation leverages Unity Catalog for:

**Data Governance**:
- Centralized metadata management
- Fine-grained access controls
- Data lineage tracking
- Schema evolution management

**Source Configuration**:
Sources are configured to use Unity Catalog format:
```yaml
catalog: "{{ var('databricks_tpch_catalog', 'samples') }}"
schema: "{{ var('databricks_tpch_schema', 'tpch') }}"
```

**Benefits**:
- Cross-workspace data sharing
- Unified governance across multiple workspaces
- Integration with Delta Sharing
- Automatic schema discovery and documentation
{% enddocs %}

### Delta Lake Optimizations
{% docs delta_lake_optimizations %}
All tables use Delta Lake format for:

**Performance**:
- Automatic data compaction
- Z-ordering for query optimization
- Liquid clustering (when available)
- Photon acceleration compatibility

**Reliability**:
- ACID transactions
- Schema enforcement and evolution
- Automatic data validation
- Time travel capabilities

**Configuration Examples**:
```sql
{{
  config(
    materialized='table',
    file_format='delta',
    partition_by=['order_year'],
    post_hook="OPTIMIZE {{ this }} ZORDER BY (customer_key)"
  )
}}
```
{% enddocs %}

## Spark SQL Adaptations

### Date Functions
{% docs spark_sql_date_functions %}
Key differences from Snowflake SQL:

**Extract Functions**:
- Snowflake: `extract(year from date)`
- Spark SQL: `year(date)`

**Date Calculations**:
- Snowflake: `datediff('day', start_date, end_date)`
- Spark SQL: `datediff(end_date, start_date)` (reversed parameters)

**Date Truncation**:
- Both platforms: `date_trunc('month', date)` (same syntax)

**Current Timestamp**:
- Snowflake: `current_timestamp`
- Spark SQL: `current_timestamp()`
{% enddocs %}

### String Functions
{% docs spark_sql_string_functions %}
String processing adaptations:

**String Splitting**:
- Snowflake: `split_part(string, delimiter, position)`
- Spark SQL: `split(string, delimiter)[position]` (0-indexed)

**Null Handling**:
- Both platforms support `nullif()` and `coalesce()`
- Spark SQL has additional null-safe operators (`<=>`)

**Case Statements**:
- Same syntax across both platforms
- Spark SQL supports additional conditional functions
{% enddocs %}

## Performance Optimization

### Query Optimization
{% docs databricks_query_optimization %}
Optimization techniques specific to Databricks:

**Delta Lake Optimizations**:
- Automatic file compaction
- Z-ordering on frequently filtered columns
- Liquid clustering for better data layout

**Spark SQL Optimizations**:
- Catalyst optimizer for query planning
- Adaptive query execution (AQE)
- Dynamic partition pruning
- Broadcast joins for small dimensions

**Photon Acceleration**:
- Vectorized execution engine
- Native support for Delta Lake format
- Automatic acceleration for analytical workloads
{% enddocs %}

### Materialization Strategies
{% docs databricks_materialization %}
Materialization approach for different layers:

**Bronze Layer**:
- Table materialization with Delta format
- Partitioning by date columns where appropriate
- Regular optimization schedules

**Silver Layer**:
- Table materialization with Delta format
- Z-ordering on key columns
- Incremental processing for large tables

**Gold Layer**:
- Table materialization for fast analytics
- Photon-optimized for BI workloads
- Pre-aggregated for dashboard performance
{% enddocs %}

## Monitoring and Observability

### Delta Lake Monitoring
{% docs delta_lake_monitoring %}
Monitoring capabilities specific to Delta Lake:

**Table History**:
- Track all changes to Delta tables
- Time travel for data recovery
- Version management and rollback

**Data Quality Monitoring**:
- Schema enforcement and evolution tracking
- Constraint violations monitoring
- Data freshness tracking

**Performance Monitoring**:
- Query performance metrics
- File statistics and optimization recommendations
- Photon acceleration usage
{% enddocs %}

## Migration and Deployment

### Development Workflow
{% docs databricks_development %}
Recommended development workflow:

**Environment Setup**:
1. Configure Unity Catalog access
2. Set up workspace permissions
3. Configure dbt profile for Databricks
4. Test connectivity and source access

**Development Process**:
1. Start with bronze layer development
2. Validate silver layer transformations
3. Build and test gold layer analytics
4. Implement data quality tests
5. Set up monitoring and alerting

**Production Deployment**:
- Use Databricks Jobs for scheduling
- Implement CI/CD with Databricks repos
- Monitor performance and costs
- Set up automatic optimization
{% enddocs %}
