-- Custom data quality tests for TPC-H medallion architecture

-- Test that lineitem extended price matches quantity * unit price calculation
{% macro test_lineitem_price_calculation(model, column_name) %}
  select *
  from {{ model }}
  where abs({{ column_name }} - (quantity * unit_price)) > 0.01
{% endmacro %}

-- Test that order total matches sum of line item final prices
{% macro test_order_total_reconciliation(model) %}
  with order_totals as (
    select 
      order_key,
      sum(final_price) as calculated_total
    from {{ ref('silver_lineitem') }}
    group by order_key
  ),
  order_comparison as (
    select 
      o.order_key,
      o.total_price as order_total,
      ot.calculated_total,
      abs(o.total_price - ot.calculated_total) as difference
    from {{ model }} o
    left join order_totals ot on o.order_key = ot.order_key
  )
  select *
  from order_comparison
  where difference > 1.00  -- Allow small rounding differences
{% endmacro %}

-- Test that ship date is not before order date
{% macro test_ship_date_logic(model) %}
  select *
  from {{ model }}
  where ship_date < order_date
{% endmacro %}

-- Test that receipt date is not before ship date
{% macro test_receipt_date_logic(model) %}
  select *
  from {{ model }}
  where receipt_date < ship_date
{% endmacro %}

-- Test that commit date is not before order date
{% macro test_commit_date_logic(model) %}
  select *
  from {{ model }}
  where commit_date < order_date
{% endmacro %}

-- Test for reasonable discount rates (0-100%)
{% macro test_discount_rate_range(model, column_name) %}
  select *
  from {{ model }}
  where {{ column_name }} < 0 or {{ column_name }} > 1
{% endmacro %}

-- Test for reasonable tax rates (0-50%)
{% macro test_tax_rate_range(model, column_name) %}
  select *
  from {{ model }}
  where {{ column_name }} < 0 or {{ column_name }} > 0.5
{% endmacro %}

-- Test that profit margin calculations are reasonable
{% macro test_profit_margin_logic(model) %}
  select *
  from {{ model }}
  where profit_margin_percent < -1 or profit_margin_percent > 1
{% endmacro %}

-- Test for data freshness - no orders older than TPC-H date range
{% macro test_tpch_date_range(model, date_column) %}
  select *
  from {{ model }}
  where {{ date_column }} < '1992-01-01' 
     or {{ date_column }} > '1998-12-31'
{% endmacro %}

-- Test for consistent customer-nation relationships
{% macro test_customer_nation_consistency(model) %}
  with customer_nations as (
    select 
      customer_key,
      count(distinct nation_name) as nation_count
    from {{ model }}
    group by customer_key
  )
  select *
  from customer_nations
  where nation_count > 1
{% endmacro %}

-- Test for orphaned records in fact tables
{% macro test_orphaned_records(model, foreign_key, parent_model, parent_key) %}
  select *
  from {{ model }} child
  left join {{ parent_model }} parent 
    on child.{{ foreign_key }} = parent.{{ parent_key }}
  where parent.{{ parent_key }} is null
    and child.{{ foreign_key }} is not null
{% endmacro %}

-- Test for duplicate part-supplier combinations
{% macro test_partsupp_uniqueness(model) %}
  select 
    part_key,
    supplier_key,
    count(*) as duplicate_count
  from {{ model }}
  group by part_key, supplier_key
  having count(*) > 1
{% endmacro %}

-- Test for reasonable quantity ranges
{% macro test_quantity_reasonableness(model, column_name) %}
  select *
  from {{ model }}
  where {{ column_name }} <= 0 or {{ column_name }} > 100
{% endmacro %}

-- Test that revenue calculations are consistent
{% macro test_revenue_consistency(model) %}
  select *
  from {{ model }}
  where abs(discounted_price - (extended_price * (1 - discount_rate))) > 0.01
     or abs(final_price - (discounted_price * (1 + tax_rate))) > 0.01
{% endmacro %}
