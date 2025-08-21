# TPC-H Medallion Architecture Documentation

## Overview
This dbt project implements a medallion architecture using Snowflake's shared TPC-H benchmark dataset, providing a complete data pipeline from raw data to business-ready analytics. The project leverages Snowflake's SNOWFLAKE_SAMPLE_DATA database which provides TPC-H data at multiple scale factors.

## Architecture Layers

### Bronze Layer (Raw Data)
The Bronze layer contains raw data from the TPC-H benchmark dataset with minimal transformations:

{% docs bronze_layer %}
**Purpose**: Store raw data from Snowflake's TPC-H sample database with lineage and metadata tracking

**Characteristics**:
- 1:1 mapping from Snowflake's SNOWFLAKE_SAMPLE_DATA.TPCH_SF* tables
- Configurable scale factor via tpch_source_schema variable
- Metadata columns for audit trail and source tracking
- Basic data type preservation
- No business logic applied

**Scale Factor Selection**:
The bronze layer can source from any of Snowflake's TPC-H scale factors by updating the tpch_source_schema variable:
- tpch_sf1: ~6M lineitem rows (development)
- tpch_sf10: ~60M lineitem rows  
- tpch_sf100: ~600M lineitem rows
- tpch_sf1000: ~6B lineitem rows (production scale)

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

{% docs silver_layer %}
**Purpose**: Provide clean, validated data with business logic applied

**Characteristics**:
- Data quality validations
- Standardized formatting
- Derived business attributes
- Foreign key relationships validated
- Data quality flags for monitoring

**Key Transformations**:
- Name standardization (UPPER case)
- Address and phone formatting
- Business categorizations (customer tiers, price categories)
- Calculated fields (lead times, performance indicators)
- Cross-reference validations
{% enddocs %}

### Gold Layer (Business Data)
The Gold layer contains aggregated, business-ready data for analytics:

{% docs gold_layer %}
**Purpose**: Deliver business-ready metrics and KPIs

**Characteristics**:
- Pre-aggregated data for performance
- Business metrics and KPIs
- Time-series analysis
- Customer and supplier analytics
- Regional performance metrics

**Key Models**:
- Customer analytics and segmentation
- Sales performance metrics
- Supplier performance tracking
- Regional trade analysis
- Monthly KPI dashboards
{% enddocs %}

## Business Concepts

### Customer Segmentation
{% docs customer_segmentation %}
Customers are segmented based on multiple criteria:

**Value Segments**:
- VIP: >$1M total order value
- Premium: $500K-$1M total order value  
- Standard: $100K-$500K total order value
- Basic: <$100K total order value

**Frequency Segments**:
- Frequent: 10+ orders
- Regular: 5-9 orders
- Occasional: 2-4 orders
- One-time: 1 order

**Tier Classification** (based on account balance):
- High Value: $5K+
- Medium Value: $1K-$5K
- Low Value: $0-$1K
- Deficit: <$0
{% enddocs %}

### Supplier Performance
{% docs supplier_performance %}
Supplier performance is evaluated across multiple dimensions:

**Revenue Performance**:
- Top Performer: >$500K revenue generated
- High Performer: $200K-$500K revenue
- Medium Performer: $50K-$200K revenue  
- Low Performer: <$50K revenue

**Delivery Performance**:
- Excellent: 95%+ on-time delivery
- Good: 85-95% on-time delivery
- Fair: 70-85% on-time delivery
- Poor: <70% on-time delivery

**Key Metrics**:
- On-time delivery rate
- Average lead times
- Return rates
- Customer reach
- Part diversity
{% enddocs %}

### Trade Analysis
{% docs trade_analysis %}
Trade flows are analyzed between regions and nations:

**Trade Types**:
- Domestic: Customer and supplier in same region
- International: Customer and supplier in different regions

**Regional Metrics**:
- Trade volume and value
- Market penetration
- Customer concentration
- Supplier diversity
- Profit margins by region
{% enddocs %}

## Data Quality Framework

### Data Quality Dimensions
{% docs data_quality %}
Our data quality framework covers six key dimensions:

**Completeness**: No missing critical values
- Primary keys always present
- Required business fields populated
- Foreign key relationships valid

**Uniqueness**: No duplicate records
- Primary key constraints enforced
- Composite key validations
- Business key uniqueness

**Validity**: Data conforms to business rules
- Date ranges within expected bounds
- Numeric values within reasonable limits
- Category values from approved lists

**Consistency**: Data relationships are logical
- Ship dates after order dates
- Receipt dates after ship dates
- Price calculations mathematically correct

**Accuracy**: Data reflects real-world values
- Phone number formats
- Address standardization
- Name formatting consistency

**Timeliness**: Data is current and relevant
- TPC-H date ranges (1992-1998)
- No future dates in historical data
- Consistent time zone handling
{% enddocs %}

### Custom Tests
{% docs custom_tests %}
Beyond standard dbt tests, we implement custom business logic tests:

**Financial Validations**:
- Extended price = quantity × unit price
- Order totals match line item sums
- Discount calculations are accurate
- Tax calculations are reasonable

**Date Logic Tests**:
- Ship date ≥ order date
- Receipt date ≥ ship date  
- Commit date ≥ order date

**Business Rule Tests**:
- Discount rates: 0-100%
- Tax rates: 0-50%
- Profit margins: reasonable ranges
- Customer-nation consistency
{% enddocs %}

## Performance Considerations

### Materialization Strategy
{% docs materialization_strategy %}
We use different materialization strategies based on data characteristics:

**Tables**: For frequently accessed, relatively static data
- All Bronze layer models
- Dimension tables in Silver layer
- Gold layer aggregations

**Views**: For real-time calculations (not used in this project)

**Incremental**: For large, growing datasets (future enhancement)
- Could be applied to lineitem processing
- Date-based partitioning strategies

**Ephemeral**: For intermediate calculations
- Used in complex CTEs within models
{% enddocs %}

### Query Optimization
{% docs query_optimization %}
Optimization techniques employed:

**Indexing**: Snowflake automatic clustering
**Partitioning**: By date columns where appropriate  
**Aggregation**: Pre-calculated metrics in Gold layer
**Joins**: Optimized join order and conditions
**Filtering**: Early filtering in WHERE clauses
{% enddocs %}

## Monitoring and Observability

### Data Lineage
{% docs data_lineage %}
Complete data lineage tracking from source to gold:

**Source Tracking**: Seeds → Bronze models
**Transformation Tracking**: Bronze → Silver → Gold
**Dependency Mapping**: Automated via dbt
**Impact Analysis**: Understand downstream effects
{% enddocs %}

### Alerting and Monitoring
{% docs monitoring %}
Monitoring implemented at multiple levels:

**Test Failures**: Automated test execution
**Data Freshness**: SLA monitoring for data updates
**Performance**: Query execution time tracking
**Volume**: Row count and data size monitoring
**Quality Scores**: Aggregated quality metrics
{% enddocs %}
