{{
  config(
    materialized='table',
    file_format='delta',
    tags=['gold', 'business', 'kpi', 'monthly', 'databricks']
  )
}}

with monthly_base as (
    select
        date_trunc('month', l.order_date) as report_month,
        year(l.order_date) as report_year,
        month(l.order_date) as report_month_num,
        l.*
    from {{ ref('silver_lineitem') }} l
    where l.order_date is not null
),

current_month as (
    select
        report_month,
        report_year,
        report_month_num,
        
        -- volume kpis
        count(*) as total_line_items,
        count(distinct order_key) as total_orders,
        count(distinct customer_key) as active_customers,
        count(distinct supplier_key) as active_suppliers,
        count(distinct part_key) as parts_sold,
        sum(quantity) as total_units_sold,
        
        -- revenue kpis
        sum(extended_price) as gross_revenue,
        sum(final_price) as net_revenue,
        sum(discount_amount) as total_discounts,
        sum(tax_amount) as total_taxes,
        avg(unit_price) as avg_unit_price,
        
        -- performance kpis
        avg(total_lead_time_days) as avg_lead_time,
        avg(case when delivery_performance = 'ON_TIME' then 1.0 else 0.0 end) as on_time_delivery_rate,
        avg(discount_rate) as avg_discount_rate,
        avg(case when return_flag = 'R' then 1.0 else 0.0 end) as return_rate,
        
        -- customer metrics
        case 
            when count(distinct customer_key) = 0 then null
            else sum(final_price) / count(distinct customer_key)
        end as revenue_per_customer,
        case 
            when count(distinct order_key) = 0 then null
            else sum(final_price) / count(distinct order_key)
        end as avg_order_value,
        case 
            when count(distinct order_key) = 0 then null
            else count(*) / count(distinct order_key)
        end as avg_lines_per_order,
        
        -- regional breakdown
        sum(case when trade_type = 'DOMESTIC' then final_price else 0 end) as domestic_revenue,
        sum(case when trade_type = 'INTERNATIONAL' then final_price else 0 end) as international_revenue,
        count(distinct case when trade_type = 'DOMESTIC' then customer_key end) as domestic_customers,
        count(distinct case when trade_type = 'INTERNATIONAL' then customer_key end) as international_customers,
        
        -- priority mix
        sum(case when priority_group = 'HIGH' then final_price else 0 end) as high_priority_revenue,
        sum(case when priority_group = 'MEDIUM' then final_price else 0 end) as medium_priority_revenue,
        sum(case when priority_group = 'LOW' then final_price else 0 end) as low_priority_revenue,
        
        -- product category performance
        count(distinct manufacturer) as active_manufacturers,
        count(distinct brand) as active_brands,
        sum(case when part_price_category = 'PREMIUM' then final_price else 0 end) as premium_product_revenue,
        sum(case when part_price_category = 'STANDARD' then final_price else 0 end) as standard_product_revenue,
        sum(case when part_price_category = 'ECONOMY' then final_price else 0 end) as economy_product_revenue,
        
        -- order size distribution
        count(distinct case when line_value_category = 'HIGH_VALUE' then order_key end) as high_value_orders,
        count(distinct case when line_value_category = 'MEDIUM_VALUE' then order_key end) as medium_value_orders,
        count(distinct case when line_value_category = 'LOW_VALUE' then order_key end) as low_value_orders
        
    from monthly_base
    group by report_month, report_year, report_month_num
),

previous_month as (
    select
        report_month,
        lag(net_revenue) over (order by report_month) as prev_month_revenue,
        lag(total_orders) over (order by report_month) as prev_month_orders,
        lag(active_customers) over (order by report_month) as prev_month_customers,
        lag(on_time_delivery_rate) over (order by report_month) as prev_month_delivery_rate
    from current_month
)

select
    cm.report_month,
    cm.report_year,
    cm.report_month_num,
    
    -- volume kpis
    cm.total_line_items,
    cm.total_orders,
    cm.active_customers,
    cm.active_suppliers,
    cm.parts_sold,
    cm.total_units_sold,
    
    -- revenue kpis
    cm.gross_revenue,
    cm.net_revenue,
    cm.total_discounts,
    cm.total_taxes,
    cm.avg_unit_price,
    
    -- performance kpis
    cm.avg_lead_time,
    cm.on_time_delivery_rate,
    cm.avg_discount_rate,
    cm.return_rate,
    
    -- customer metrics
    cm.revenue_per_customer,
    cm.avg_order_value,
    cm.avg_lines_per_order,
    
    -- regional metrics
    cm.domestic_revenue,
    cm.international_revenue,
    cm.domestic_customers,
    cm.international_customers,
    case 
        when cm.net_revenue = 0 then null
        else cm.domestic_revenue / cm.net_revenue
    end as domestic_revenue_pct,
    case 
        when cm.net_revenue = 0 then null
        else cm.international_revenue / cm.net_revenue
    end as international_revenue_pct,
    
    -- priority metrics
    cm.high_priority_revenue,
    cm.medium_priority_revenue,
    cm.low_priority_revenue,
    case 
        when cm.net_revenue = 0 then null
        else cm.high_priority_revenue / cm.net_revenue
    end as high_priority_revenue_pct,
    
    -- product metrics
    cm.active_manufacturers,
    cm.active_brands,
    cm.premium_product_revenue,
    cm.standard_product_revenue,
    cm.economy_product_revenue,
    
    -- order distribution
    cm.high_value_orders,
    cm.medium_value_orders,
    cm.low_value_orders,
    
    -- month-over-month growth
    case 
        when pm.prev_month_revenue > 0 then 
            (cm.net_revenue - pm.prev_month_revenue) / pm.prev_month_revenue * 100
        else null 
    end as revenue_growth_pct,
    
    case 
        when pm.prev_month_orders > 0 then 
            (cm.total_orders - pm.prev_month_orders) / pm.prev_month_orders * 100
        else null 
    end as order_growth_pct,
    
    case 
        when pm.prev_month_customers > 0 then 
            (cm.active_customers - pm.prev_month_customers) / pm.prev_month_customers * 100
        else null 
    end as customer_growth_pct,
    
    -- performance changes
    cm.on_time_delivery_rate - pm.prev_month_delivery_rate as delivery_rate_change,
    
    -- metadata
    current_timestamp() as _gold_created_at

from current_month cm
left join previous_month pm 
    on cm.report_month = pm.report_month

order by cm.report_month
