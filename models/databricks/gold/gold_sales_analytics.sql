{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold', 'business', 'sales', 'analytics', 'databricks']
  )
}}

select
    -- time dimensions
    l.order_date,
    l.order_year,
    l.order_quarter,
    l.order_month,
    l.ship_year,
    l.ship_quarter,
    l.ship_month,
    
    -- geography dimensions
    l.customer_region,
    l.customer_nation,
    l.supplier_region,
    l.supplier_nation,
    l.trade_type,
    
    -- product dimensions
    l.manufacturer,
    l.brand,
    l.part_type,
    l.part_price_category,
    
    -- order characteristics
    l.order_status,
    l.priority_group,
    l.line_value_category,
    l.quantity_category,
    
    -- core metrics
    count(*) as total_line_items,
    count(distinct l.order_key) as total_orders,
    count(distinct l.customer_key) as total_customers,
    count(distinct l.part_key) as total_parts,
    count(distinct l.supplier_key) as total_suppliers,
    
    -- quantity metrics
    sum(l.quantity) as total_quantity,
    avg(l.quantity) as avg_quantity_per_line,
    max(l.quantity) as max_quantity,
    
    -- financial metrics
    sum(l.extended_price) as gross_revenue,
    sum(l.discounted_price) as net_revenue,
    sum(l.final_price) as total_revenue_with_tax,
    sum(l.discount_amount) as total_discounts,
    sum(l.tax_amount) as total_tax,
    avg(l.unit_price) as avg_unit_price,
    avg(l.discount_rate) as avg_discount_rate,
    avg(l.tax_rate) as avg_tax_rate,
    
    -- profitability (would need cost data for true profit)
    case 
        when sum(l.quantity) = 0 then null
        else sum(l.final_price) / sum(l.quantity)
    end as revenue_per_unit,
    case 
        when sum(l.extended_price) = 0 then null
        else sum(l.discount_amount) / sum(l.extended_price)
    end as discount_rate_actual,
    
    -- performance metrics
    avg(l.order_to_ship_days) as avg_order_to_ship_days,
    avg(l.total_lead_time_days) as avg_total_lead_time,
    avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) as on_time_delivery_rate,
    
    -- return metrics
    sum(case when l.return_flag = 'R' then l.quantity else 0 end) as returned_quantity,
    sum(case when l.return_flag = 'R' then l.final_price else 0 end) as returned_revenue,
    avg(case when l.return_flag = 'R' then 1.0 else 0.0 end) as return_rate,
    
    -- metadata
    current_timestamp() as _gold_created_at

from {{ ref('silver_lineitem') }} l

group by 
    l.order_date,
    l.order_year,
    l.order_quarter,
    l.order_month,
    l.ship_year,
    l.ship_quarter,
    l.ship_month,
    l.customer_region,
    l.customer_nation,
    l.supplier_region,
    l.supplier_nation,
    l.trade_type,
    l.manufacturer,
    l.brand,
    l.part_type,
    l.part_price_category,
    l.order_status,
    l.priority_group,
    l.line_value_category,
    l.quantity_category
