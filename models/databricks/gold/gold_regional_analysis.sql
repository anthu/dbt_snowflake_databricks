{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold', 'business', 'regional', 'analysis', 'databricks']
  )
}}

select
    -- geographic dimensions
    coalesce(l.customer_region, 'UNKNOWN') as customer_region,
    coalesce(l.customer_nation, 'UNKNOWN') as customer_nation,
    coalesce(l.supplier_region, 'UNKNOWN') as supplier_region,
    coalesce(l.supplier_nation, 'UNKNOWN') as supplier_nation,
    l.trade_type,
    
    -- time dimension
    l.order_year,
    
    -- market presence
    count(distinct l.customer_key) as total_customers,
    count(distinct l.supplier_key) as total_suppliers,
    count(distinct l.part_key) as total_parts_traded,
    count(distinct l.order_key) as total_orders,
    count(*) as total_line_items,
    
    -- financial metrics
    sum(l.extended_price) as gross_revenue,
    sum(l.final_price) as net_revenue,
    sum(l.discount_amount) as total_discounts,
    sum(l.tax_amount) as total_taxes,
    avg(l.unit_price) as avg_unit_price,
    
    -- volume metrics
    sum(l.quantity) as total_quantity_traded,
    avg(l.quantity) as avg_quantity_per_line,
    
    -- trade flow analysis
    sum(case when l.customer_region = l.supplier_region then l.final_price else 0 end) as domestic_trade_value,
    sum(case when l.customer_region != l.supplier_region then l.final_price else 0 end) as international_trade_value,
    
    -- performance metrics
    avg(l.total_lead_time_days) as avg_lead_time,
    avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) as on_time_delivery_rate,
    avg(l.discount_rate) as avg_discount_rate,
    
    -- market characteristics
    count(distinct l.manufacturer) as manufacturers_present,
    count(distinct l.brand) as brands_present,
    
    -- customer segmentation
    sum(case when c.customer_tier = 'HIGH_VALUE' then l.final_price else 0 end) as high_value_customer_revenue,
    sum(case when c.customer_tier = 'MEDIUM_VALUE' then l.final_price else 0 end) as medium_value_customer_revenue,
    sum(case when c.customer_tier = 'LOW_VALUE' then l.final_price else 0 end) as low_value_customer_revenue,
    
    -- market segment analysis
    sum(case when c.market_segment = 'AUTOMOBILE' then l.final_price else 0 end) as automobile_segment_revenue,
    sum(case when c.market_segment = 'BUILDING' then l.final_price else 0 end) as building_segment_revenue,
    sum(case when c.market_segment = 'FURNITURE' then l.final_price else 0 end) as furniture_segment_revenue,
    sum(case when c.market_segment = 'HOUSEHOLD' then l.final_price else 0 end) as household_segment_revenue,
    sum(case when c.market_segment = 'MACHINERY' then l.final_price else 0 end) as machinery_segment_revenue,
    
    -- calculated metrics
    case 
        when count(distinct l.customer_key) = 0 then null
        else sum(l.final_price) / count(distinct l.customer_key)
    end as revenue_per_customer,
    case 
        when count(distinct l.order_key) = 0 then null
        else sum(l.final_price) / count(distinct l.order_key)
    end as revenue_per_order,
    case 
        when count(distinct l.customer_key) = 0 then null
        else sum(l.quantity) / count(distinct l.customer_key)
    end as quantity_per_customer,
    
    -- regional competitiveness
    avg(ps.supply_cost) as avg_supply_cost,
    avg(ps.profit_margin_percent) as avg_profit_margin,
    
    -- metadata
    current_timestamp() as _gold_created_at

from {{ ref('silver_lineitem') }} l
left join {{ ref('silver_customer') }} c 
    on l.customer_key = c.customer_key
left join {{ ref('silver_partsupp') }} ps 
    on l.part_key = ps.part_key 
    and l.supplier_key = ps.supplier_key

group by 
    coalesce(l.customer_region, 'UNKNOWN'),
    coalesce(l.customer_nation, 'UNKNOWN'),
    coalesce(l.supplier_region, 'UNKNOWN'),
    coalesce(l.supplier_nation, 'UNKNOWN'),
    l.trade_type,
    l.order_year
