#!/bin/bash

# TPC-H Medallion Architecture Setup Script (Multi-Platform)
# This script helps you get started with the dbt project

echo "🏗️  TPC-H Medallion Architecture Setup (Multi-Platform)"
echo "========================================================="
echo ""
echo "📋 Available Platforms:"
echo "  ✅ Snowflake - Using SNOWFLAKE_SAMPLE_DATA"  
echo "  ✅ Databricks - Using Unity Catalog & Delta Lake"
echo ""

# Platform selection
echo "🎯 Select your platform:"
echo "1) Snowflake"
echo "2) Databricks"
echo "3) Both platforms"
echo ""
read -p "Enter your choice (1/2/3): " PLATFORM_CHOICE

case $PLATFORM_CHOICE in
    1)
        SELECTED_PLATFORM="snowflake"
        PLATFORM_NAME="Snowflake"
        DBT_ADAPTER="dbt-snowflake"
        ;;
    2)
        SELECTED_PLATFORM="databricks"
        PLATFORM_NAME="Databricks"
        DBT_ADAPTER="dbt-databricks"
        ;;
    3)
        SELECTED_PLATFORM="both"
        PLATFORM_NAME="Both Platforms"
        DBT_ADAPTER="both"
        ;;
    *)
        echo "❌ Invalid choice. Exiting..."
        exit 1
        ;;
esac

echo ""
echo "🎯 Setting up $PLATFORM_NAME implementation..."
echo ""

# Check if dbt is installed
if ! command -v dbt &> /dev/null; then
    echo "❌ dbt is not installed. Please install the appropriate adapter:"
    if [ "$SELECTED_PLATFORM" = "snowflake" ]; then
        echo "   pip install dbt-snowflake"
    elif [ "$SELECTED_PLATFORM" = "databricks" ]; then
        echo "   pip install dbt-databricks"
    else
        echo "   pip install dbt-snowflake dbt-databricks"
    fi
    exit 1
fi

echo "✅ dbt is installed"

# Check if profiles.yml exists
if [ ! -f ~/.dbt/profiles.yml ]; then
    echo "⚠️  No profiles.yml found. Setting up configuration..."
    echo ""
    echo "📋 Configuration Options:"
    echo "   Option 1 (Recommended): Use environment variables"
    echo "     1. cp env.template .env"
    echo "     2. Edit .env with your credentials"
    echo "     3. cp profiles.yml ~/.dbt/profiles.yml"
    echo ""
    echo "   Option 2: Direct configuration"
    echo "     1. cp profiles.yml.template ~/.dbt/profiles.yml"
    echo "     2. Edit ~/.dbt/profiles.yml with your credentials"
    echo ""
    if [ "$SELECTED_PLATFORM" = "snowflake" ]; then
        echo "   Configure your Snowflake connection:"
        echo "   - Account: your_account.region.snowflakecomputing.com"
        echo "   - User, password, role, warehouse, database"
    elif [ "$SELECTED_PLATFORM" = "databricks" ]; then
        echo "   Configure your Databricks connection:"  
        echo "   - Host: your_workspace.cloud.databricks.com"
        echo "   - Token: Personal Access Token or Service Principal"
        echo "   - HTTP Path: SQL Warehouse or Cluster path"
    else
        echo "   Configure both Snowflake and Databricks connections"
    fi
    echo ""
    echo "   3. Run this script again"
    exit 1
fi

echo "✅ profiles.yml found"

# Test dbt connection
echo "🔍 Testing dbt connection..."
if dbt debug; then
    echo "✅ dbt connection successful"
else
    echo "❌ dbt connection failed. Please check your profiles.yml configuration"
    exit 1
fi

# Install dbt packages
echo "📦 Installing dbt packages..."
dbt deps

# Platform-specific data source verification
if [ "$SELECTED_PLATFORM" = "snowflake" ] || [ "$SELECTED_PLATFORM" = "both" ]; then
    echo "🔍 Verifying access to Snowflake TPC-H sample data..."
    if ! dbt source freshness --select source:tpch 2>/dev/null; then
        echo "⚠️  Warning: Could not verify access to Snowflake TPC-H sample data."
        echo "   Make sure your Snowflake account has access to SNOWFLAKE_SAMPLE_DATA database."
        echo "   You can also try a different scale factor by updating tpch_source_schema in dbt_project.yml"
    fi
fi

if [ "$SELECTED_PLATFORM" = "databricks" ] || [ "$SELECTED_PLATFORM" = "both" ]; then
    echo "🔍 Verifying access to Databricks Unity Catalog..."
    if ! dbt source freshness --select source:tpch_databricks 2>/dev/null; then
        echo "⚠️  Warning: Could not verify access to Databricks TPC-H data."
        echo "   Make sure your Unity Catalog has access to the configured TPC-H data."
        echo "   Update databricks_tpch_catalog and databricks_tpch_schema in dbt_project.yml as needed."
    fi
fi

# Function to run medallion architecture for a platform
run_platform() {
    local platform=$1
    local platform_name=$2
    
    echo ""
    echo "🚀 Building $platform_name Medallion Architecture..."
    echo "=================================================="
    
    # Run Bronze layer
    echo "🥉 Building Bronze layer ($platform_name)..."
    dbt run --select models.$platform.bronze
    
    # Run Silver layer
    echo "🥈 Building Silver layer ($platform_name)..."
    dbt run --select models.$platform.silver
    
    # Run Gold layer
    echo "🥇 Building Gold layer ($platform_name)..."
    dbt run --select models.$platform.gold
    
    # Run tests for this platform
    echo "🧪 Running data quality tests ($platform_name)..."
    dbt test --select tag:$platform
}

# Run the selected platform(s)
if [ "$SELECTED_PLATFORM" = "snowflake" ]; then
    run_platform "snowflake" "Snowflake"
elif [ "$SELECTED_PLATFORM" = "databricks" ]; then
    run_platform "databricks" "Databricks"
else
    run_platform "snowflake" "Snowflake"
    run_platform "databricks" "Databricks"
fi

# Generate documentation
echo ""
echo "📚 Generating documentation..."
if [ "$SELECTED_PLATFORM" = "both" ]; then
    dbt docs generate
else
    dbt docs generate --select tag:$SELECTED_PLATFORM
fi

# Final success message
echo ""
echo "🎉 Setup complete! Your $PLATFORM_NAME medallion architecture is ready."
echo ""
echo "📊 To explore your data:"
echo "   dbt docs serve    # View documentation and lineage"
echo ""

# Platform-specific commands
if [ "$SELECTED_PLATFORM" = "snowflake" ]; then
    echo "🔍 Useful Snowflake commands:"
    echo "   dbt run --select tag:snowflake           # Run all Snowflake models"
    echo "   dbt test --select tag:snowflake          # Test all Snowflake models"
    echo "   dbt run --select models.snowflake.silver # Run Snowflake Silver layer"
    echo ""
    echo "📈 Key Snowflake gold tables to query:"
    echo "   - gold_customer_analytics"
    echo "   - gold_sales_analytics"  
    echo "   - gold_supplier_performance"
    echo "   - gold_regional_analysis"
    echo "   - gold_monthly_kpis"
elif [ "$SELECTED_PLATFORM" = "databricks" ]; then
    echo "🔍 Useful Databricks commands:"
    echo "   dbt run --select tag:databricks           # Run all Databricks models"
    echo "   dbt test --select tag:databricks          # Test all Databricks models"
    echo "   dbt run --select models.databricks.silver # Run Databricks Silver layer"
    echo ""
    echo "📈 Key Databricks gold tables to query:"
    echo "   - gold_customer_analytics"
    echo "   - gold_sales_analytics"  
    echo "   - gold_supplier_performance"
    echo "   - gold_regional_analysis"
    echo "   - gold_monthly_kpis"
    echo ""
    echo "⚡ Delta Lake Benefits:"
    echo "   - ACID transactions for data consistency"
    echo "   - Time travel for historical analysis"
    echo "   - Automatic optimization and compaction"
    echo "   - Schema evolution support"
else
    echo "🔍 Useful cross-platform commands:"
    echo "   dbt run --select tag:snowflake    # Run all Snowflake models"
    echo "   dbt run --select tag:databricks   # Run all Databricks models"
    echo "   dbt test --select tag:snowflake   # Test Snowflake models"
    echo "   dbt test --select tag:databricks  # Test Databricks models"
    echo ""
    echo "📈 Compare results across platforms:"
    echo "   - Both platforms have identical business logic"
    echo "   - Same gold table schemas for consistent analysis"
    echo "   - Platform-specific optimizations for performance"
fi

echo ""
echo "🚀 Platform Status:"
if [ "$SELECTED_PLATFORM" = "snowflake" ]; then
    echo "   ✅ Snowflake: Ready for production use"
elif [ "$SELECTED_PLATFORM" = "databricks" ]; then
    echo "   ✅ Databricks: Ready with Delta Lake optimizations"
else
    echo "   ✅ Snowflake: Ready for production use"
    echo "   ✅ Databricks: Ready with Delta Lake optimizations"
    echo "   🔄 Cross-platform: Compare and validate results"
fi

echo ""
echo "🎯 Next Steps:"
if [ "$SELECTED_PLATFORM" = "snowflake" ]; then
    echo "   1. Query your Snowflake warehouse for business insights"
    echo "   2. Connect BI tools to your gold layer tables"
    echo "   3. Scale with different TPC-H scale factors (SF1-SF1000)"
elif [ "$SELECTED_PLATFORM" = "databricks" ]; then
    echo "   1. Query your Unity Catalog for business insights"
    echo "   2. Use Databricks SQL for interactive analytics"
    echo "   3. Leverage Photon for faster query performance"
else
    echo "   1. Compare query performance across platforms"
    echo "   2. Validate business logic consistency"
    echo "   3. Choose the best platform for your use case"
fi

echo ""
echo "Happy analyzing! 🎉"