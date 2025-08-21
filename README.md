# TPC-H Medallion Architecture with dbt (Multi-Platform)

This project implements a complete data pipeline using the TPC-H benchmark dataset, built with dbt (data build tool) and designed for multiple platforms. The architecture follows the medallion pattern with Bronze (raw), Silver (cleaned), and Gold (business) layers.

## 🏢 Platform Support

- **✅ Snowflake**: Complete implementation using Snowflake's shared TPC-H sample database
- **✅ Databricks**: Complete implementation using Unity Catalog and Delta Lake

The project is structured to support both platforms with platform-specific models and configurations.

### Current Status
- **Snowflake**: Ready for production use with full medallion architecture
- **Databricks**: Complete implementation with Delta Lake optimizations and Spark SQL

## 🏗️ Architecture Overview

### Medallion Architecture Layers

1. **Bronze Layer** 🥉
   - Raw data ingestion from Snowflake's TPC-H sample database
   - Minimal transformations
   - Data lineage tracking
   - Source system metadata

2. **Silver Layer** 🥈
   - Data cleaning and validation
   - Standardization and enrichment
   - Data quality flags
   - Business logic implementation
   - Foreign key relationships

3. **Gold Layer** 🥇
   - Business-ready aggregated data
   - KPIs and analytics
   - Executive dashboards
   - Performance metrics

## 📊 TPC-H Dataset

The TPC-H benchmark is a decision support benchmark that consists of a suite of business-oriented ad-hoc queries and concurrent data modifications. This project uses [Snowflake's shared TPC-H sample database](https://docs.snowflake.com/en/user-guide/sample-data-tpch) which provides multiple scale factors:

### Available Scale Factors
- **TPCH_SF1**: Several million rows (good for development)
- **TPCH_SF10**: 10x the base size
- **TPCH_SF100**: 100x the base size (several hundred million rows)
- **TPCH_SF1000**: 1000x the base size (several billion rows)

You can easily switch between scale factors by updating the `tpch_source_schema` variable in `dbt_project.yml`.

Our implementation includes:

### Core Tables
- **Region**: Geographic regions (5 regions)
- **Nation**: Countries within regions (25 nations)
- **Customer**: Customer master data with demographics
- **Supplier**: Supplier master data with geographic info
- **Part**: Product catalog with specifications
- **PartSupp**: Part-supplier relationships with pricing
- **Orders**: Order headers with customer info
- **LineItem**: Order line items (fact table)

## 🚀 Getting Started

### Prerequisites
- Snowflake account with appropriate permissions
- dbt-snowflake installed (for traditional approach)
- Python 3.8+

### Snowflake Deployment Options

This project supports **traditional dbt** usage (external dbt connecting to Snowflake). Snowflake also offers a newer approach:

#### **Option 1: Traditional dbt (This Project)**
- Run dbt from your local machine or CI/CD
- Use `profiles.yml` for connection configuration
- Full control over dbt environment and dependencies
- Supports cross-platform development (Snowflake + Databricks)

#### **Option 2: Native "dbt Projects on Snowflake" (Preview)**
- Run dbt directly within Snowflake workspaces
- Managed dbt environment within Snowflake
- Git integration through Snowflake workspaces
- See: [Snowflake's dbt tutorial](https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-getting-started-tutorial)

### Setup Instructions

1. **Clone and Setup**
   ```bash
   git clone <repository>
   cd dbt-snowflake-dbx
   ```

2. **Setup Snowflake Environment**
   - Run the Snowflake setup script: `snowflake_setup.sql`
   - This creates databases, schemas, warehouses, and permissions
   ```sql
   -- Creates: TPCH_MEDALLION_DB, TPCH_MEDALLION_WH, schemas (dev, prod, bronze, silver, gold)
   ```

3. **Configure Connection**
   - Copy `profiles.yml.template` to `~/.dbt/profiles.yml`  
   - Update with your Snowflake credentials
   ```yaml
   tpch_medallion_multiplatform:
     target: snowflake_dev
     outputs:
       snowflake_dev:
         type: snowflake
         account: your_account.region.snowflakecomputing.com
         user: your_username
         password: your_password
         role: ACCOUNTADMIN
         database: TPCH_MEDALLION_DB
         warehouse: TPCH_MEDALLION_WH
         schema: dev
   ```

4. **Install Dependencies**
   ```bash
   dbt deps
   ```

5. **Verify Access to Sample Data**
   Ensure your Snowflake account has access to the `SNOWFLAKE_SAMPLE_DATA` database. Most Snowflake accounts have this by default.

6. **Run Transformations**
   ```bash
   # Run all models
   dbt run
   
   # Run Snowflake models only
   dbt run --select tag:snowflake
   
   # Run by layer (Snowflake)
   dbt run --select tag:bronze,tag:snowflake
   dbt run --select tag:silver,tag:snowflake  
   dbt run --select tag:gold,tag:snowflake
   
   # Run specific platform and layer
   dbt run --select models.snowflake.bronze
   dbt run --select models.snowflake.silver
   dbt run --select models.snowflake.gold
   ```

7. **Test Data Quality**
   ```bash
   dbt test
   ```

8. **Generate Documentation**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## 📁 Project Structure

```
dbt-snowflake-dbx/
├── dbt_project.yml          # Main project configuration
├── packages.yml             # dbt package dependencies  
├── profiles.yml.template    # Connection configuration template
├── README.md                # This file
├── models/
│   ├── snowflake/           # Snowflake-specific implementation
│   │   ├── sources.yml      # Snowflake source definitions
│   │   ├── docs.md          # Snowflake documentation
│   │   ├── bronze/          # Raw data layer
│   │   │   ├── bronze_*.sql # Bronze models
│   │   │   └── schema.yml   # Tests and documentation
│   │   ├── silver/          # Cleaned data layer
│   │   │   ├── silver_*.sql # Silver models
│   │   │   └── schema.yml   # Tests and documentation
│   │   └── gold/            # Business layer
│   │       ├── gold_*.sql   # Gold models
│   │       └── schema.yml   # Tests and documentation
│   └── databricks/          # Databricks-specific implementation (future)
│       ├── README.md        # Databricks implementation plan
│       ├── bronze/          # Raw data layer (Delta Bronze)
│       ├── silver/          # Cleaned data layer (Delta Silver)
│       └── gold/            # Business layer (Delta Gold)
├── macros/                  # Custom macros and tests
│   └── data_quality_tests.sql
├── tests/                   # Custom data tests
├── analyses/                # Ad-hoc analyses
│   └── tpch_sample_queries.sql
└── snapshots/               # SCD implementations
```

## 🎯 Key Features

### Data Quality
- Comprehensive data validation tests
- Custom business logic tests
- Data freshness monitoring
- Orphan record detection
- Referential integrity checks

### Business Intelligence
- Customer analytics and segmentation
- Sales performance metrics
- Supplier performance tracking
- Regional trade analysis
- Monthly KPI dashboards

### Performance Optimizations
- Materialized tables for fast queries
- Proper indexing strategies
- Incremental model updates
- Partitioning by date

### Monitoring & Observability
- Data lineage documentation
- Model dependency graphs
- Test result tracking
- Performance metrics

## 📈 Business Use Cases

### Customer Analytics
- Customer lifetime value analysis
- Segmentation and targeting
- Purchase behavior patterns
- Geographic analysis

### Sales Performance
- Revenue trends and forecasting
- Product performance analysis
- Regional sales comparison
- Order fulfillment metrics

### Supply Chain Optimization
- Supplier performance evaluation
- Inventory level monitoring
- Lead time analysis
- Cost optimization

### Executive Dashboards
- Monthly KPI tracking
- Growth rate analysis
- Market penetration metrics
- Operational efficiency indicators

## 🧪 Testing Strategy

### Data Quality Tests
- **Uniqueness**: Primary key constraints
- **Completeness**: Not null validations
- **Validity**: Range and format checks
- **Consistency**: Business rule validation
- **Referential Integrity**: Foreign key relationships

### Custom Tests
- Price calculation accuracy
- Date logic validation
- Profit margin reasonableness
- Revenue reconciliation

## 🎯 Platform-Specific Development

### Working with Snowflake Models
```bash
# Run all Snowflake models
dbt run --select tag:snowflake

# Run specific Snowflake layer
dbt run --select models.snowflake.bronze
dbt run --select models.snowflake.silver
dbt run --select models.snowflake.gold

# Test Snowflake models
dbt test --select tag:snowflake

# Generate docs for Snowflake
dbt docs generate --select tag:snowflake
```

### Working with Databricks Models
```bash
# Run all Databricks models
dbt run --select tag:databricks

# Run specific Databricks layer
dbt run --select models.databricks.bronze
dbt run --select models.databricks.silver
dbt run --select models.databricks.gold

# Test Databricks models
dbt test --select tag:databricks

# Generate docs for Databricks
dbt docs generate --select tag:databricks
```

### Cross-Platform Development
- Each platform has its own directory structure under `models/`
- Platform-specific tags enable selective execution
- Shared macros and tests in root-level directories
- Independent deployment and testing workflows

## 🔧 Configuration

### Variables
- `tpch_source_schema`: TPC-H scale factor selection (tpch_sf1, tpch_sf10, tpch_sf100, tpch_sf1000)
- `start_date`/`end_date`: TPC-H benchmark date range (1992-1998)

### Switching Scale Factors
To use a different TPC-H dataset size, update the `tpch_source_schema` variable in `dbt_project.yml`:

```yaml
vars:
  tpch_source_schema: tpch_sf10  # Use 10x scale factor
```

Available options:
- `tpch_sf1`: ~6M rows in lineitem (good for development)
- `tpch_sf10`: ~60M rows in lineitem
- `tpch_sf100`: ~600M rows in lineitem
- `tpch_sf1000`: ~6B rows in lineitem (requires significant compute)

### Tags
- `bronze`, `silver`, `gold`: Layer identification
- `dimension`, `fact`: Table type classification
- `customer`, `sales`, `supplier`: Business domain

## 📚 Documentation

The project includes comprehensive documentation accessible via `dbt docs`:
- Model descriptions and column definitions
- Data lineage visualization
- Test coverage reports
- Business glossary

## 🚨 Troubleshooting

### Common Issues
1. **Connection Errors**: Verify Snowflake credentials and network access
2. **Model Failures**: Check data quality and referential integrity
3. **Test Failures**: Review business logic and data assumptions
4. **Performance Issues**: Optimize materialization strategies

### Debug Commands
```bash
# Debug connection
dbt debug

# Run specific model with logs
dbt run --select silver_customer --full-refresh

# Test specific model
dbt test --select silver_customer
```

## 🤝 Contributing

1. Follow the medallion architecture pattern
2. Add comprehensive tests for new models
3. Update documentation for any changes
4. Use consistent naming conventions
5. Tag models appropriately

## 📄 License

This project is for educational and demonstration purposes.
