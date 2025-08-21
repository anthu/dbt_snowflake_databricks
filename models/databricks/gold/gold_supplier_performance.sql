{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold', 'business', 'supplier', 'performance', 'databricks']
  )
}}

select
    s.supplier_key,
    s.supplier_name,
    s.supplier_tier,
    s.nation_name,
    s.region_name,
    s.account_balance,
    
    -- supply metrics
    count(distinct ps.part_key) as parts_supplied,
    sum(ps.available_quantity) as total_inventory,
    avg(ps.available_quantity) as avg_inventory_per_part,
    sum(ps.total_inventory_value) as total_inventory_value,
    avg(ps.supply_cost) as avg_supply_cost,
    
    -- profitability metrics
    avg(ps.profit_margin) as avg_profit_margin,
    avg(ps.profit_margin_percent) as avg_profit_margin_percent,
    sum(case when ps.margin_category = 'HIGH_MARGIN' then 1 else 0 end) as high_margin_parts,
    sum(case when ps.margin_category = 'MEDIUM_MARGIN' then 1 else 0 end) as medium_margin_parts,
    sum(case when ps.margin_category = 'LOW_MARGIN' then 1 else 0 end) as low_margin_parts,
    
    -- inventory risk analysis
    sum(case when ps.stock_level_category = 'CRITICAL_STOCK' then 1 else 0 end) as critical_stock_parts,
    sum(case when ps.stock_level_category = 'LOW_STOCK' then 1 else 0 end) as low_stock_parts,
    sum(case when ps.supply_risk = 'HIGH_RISK' then 1 else 0 end) as high_risk_parts,
    avg(case when ps.supply_risk = 'HIGH_RISK' then 1.0 else 0.0 end) as high_risk_ratio,
    
    -- sales performance (from lineitem)
    count(distinct l.order_key) as total_orders_fulfilled,
    sum(l.quantity) as total_quantity_sold,
    sum(l.final_price) as total_revenue_generated,
    avg(l.quantity) as avg_quantity_per_order,
    avg(l.final_price) as avg_revenue_per_order,
    
    -- delivery performance
    avg(l.order_to_ship_days) as avg_order_to_ship_days,
    avg(l.commit_to_ship_days) as avg_commit_to_ship_days,
    avg(l.ship_to_receipt_days) as avg_ship_to_receipt_days,
    avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) as on_time_delivery_rate,
    avg(case when l.delivery_category = 'ON_TIME' then 1.0 else 0.0 end) as on_time_commit_rate,
    
    -- customer reach
    count(distinct l.customer_key) as customers_served,
    count(distinct l.customer_region) as regions_served,
    count(distinct l.customer_nation) as nations_served,
    
    -- product diversity
    count(distinct l.manufacturer) as manufacturers_represented,
    count(distinct l.brand) as brands_represented,
    count(distinct l.part_type) as part_types_supplied,
    
    -- quality indicators (return analysis)
    sum(case when l.return_flag = 'R' then l.quantity else 0 end) as returned_quantity,
    sum(case when l.return_flag = 'R' then l.final_price else 0 end) as returned_revenue,
    avg(case when l.return_flag = 'R' then 1.0 else 0.0 end) as return_rate,
    
    -- business categorization
    case 
        when sum(l.final_price) >= 500000 then 'TOP_PERFORMER'
        when sum(l.final_price) >= 200000 then 'HIGH_PERFORMER'
        when sum(l.final_price) >= 50000 then 'MEDIUM_PERFORMER'
        else 'LOW_PERFORMER'
    end as revenue_performance_tier,
    
    case 
        when avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) >= 0.95 then 'EXCELLENT'
        when avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) >= 0.85 then 'GOOD'
        when avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) >= 0.70 then 'FAIR'
        else 'POOR'
    end as delivery_performance_tier,
    
    -- metadata
    current_timestamp() as _gold_created_at

from {{ ref('silver_supplier') }} s
left join {{ ref('silver_partsupp') }} ps 
    on s.supplier_key = ps.supplier_key
left join {{ ref('silver_lineitem') }} l 
    on s.supplier_key = l.supplier_key

group by 
    s.supplier_key,
    s.supplier_name,
    s.supplier_tier,
    s.nation_name,
    s.region_name,
    s.account_balance
