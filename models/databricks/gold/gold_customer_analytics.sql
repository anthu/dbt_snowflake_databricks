{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold', 'business', 'customer', 'analytics', 'databricks']
  )
}}

select
    c.customer_key,
    c.customer_name,
    c.customer_tier,
    c.market_segment,
    c.nation_name,
    c.region_name,
    c.account_balance,
    
    -- order metrics
    count(distinct o.order_key) as total_orders,
    count(distinct case when o.order_status = 'O' then o.order_key end) as open_orders,
    count(distinct case when o.order_status = 'F' then o.order_key end) as completed_orders,
    
    -- financial metrics
    sum(o.total_price) as total_order_value,
    avg(o.total_price) as avg_order_value,
    max(o.total_price) as largest_order_value,
    min(o.total_price) as smallest_order_value,
    
    -- detailed line item metrics
    sum(l.quantity) as total_quantity_ordered,
    sum(l.final_price) as total_revenue,
    sum(l.discount_amount) as total_discounts_given,
    sum(l.tax_amount) as total_tax_collected,
    avg(l.discount_rate) as avg_discount_rate,
    
    -- timing metrics
    min(o.order_date) as first_order_date,
    max(o.order_date) as latest_order_date,
    datediff(max(o.order_date), min(o.order_date)) as customer_lifespan_days,
    
    -- order frequency (adapted for Spark SQL)
    case 
        when datediff(max(o.order_date), min(o.order_date)) = 0 then count(distinct o.order_key)
        else count(distinct o.order_key) / nullif(
            floor(datediff(max(o.order_date), min(o.order_date)) / 365.25) + 1, 0
        )
    end as orders_per_year,
    
    -- priority analysis
    count(distinct case when o.priority_group = 'HIGH' then o.order_key end) as high_priority_orders,
    count(distinct case when o.priority_group = 'MEDIUM' then o.order_key end) as medium_priority_orders,
    count(distinct case when o.priority_group = 'LOW' then o.order_key end) as low_priority_orders,
    
    -- delivery performance
    avg(case when l.delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) as on_time_delivery_rate,
    avg(l.total_lead_time_days) as avg_total_lead_time,
    
    -- product diversity
    count(distinct l.part_key) as unique_parts_ordered,
    count(distinct l.supplier_key) as unique_suppliers_used,
    
    -- business categorization
    case 
        when sum(o.total_price) >= 1000000 then 'VIP'
        when sum(o.total_price) >= 500000 then 'PREMIUM'
        when sum(o.total_price) >= 100000 then 'STANDARD'
        else 'BASIC'
    end as customer_value_segment,
    
    case 
        when count(distinct o.order_key) >= 10 then 'FREQUENT'
        when count(distinct o.order_key) >= 5 then 'REGULAR'
        when count(distinct o.order_key) >= 2 then 'OCCASIONAL'
        else 'ONE_TIME'
    end as purchase_frequency_segment,
    
    -- metadata
    current_timestamp() as _gold_created_at

from {{ ref('silver_customer') }} c
left join {{ ref('silver_orders') }} o 
    on c.customer_key = o.customer_key
left join {{ ref('silver_lineitem') }} l 
    on o.order_key = l.order_key

group by 
    c.customer_key,
    c.customer_name,
    c.customer_tier,
    c.market_segment,
    c.nation_name,
    c.region_name,
    c.account_balance
