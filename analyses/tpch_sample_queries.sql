-- TPC-H Sample Queries (Snowflake)
-- These queries demonstrate how to use the Snowflake medallion architecture for TPC-H analytics
-- Based on the official TPC-H benchmark queries

-- Q1: Pricing Summary Report Query (adapted to use our silver layer)
-- This query provides a summary pricing report for all line items shipped
select
    return_flag,
    line_status,
    sum(quantity) as sum_qty,
    sum(extended_price) as sum_base_price,
    sum(discounted_price) as sum_disc_price,
    sum(final_price) as sum_charge,
    avg(quantity) as avg_qty,
    avg(extended_price) as avg_price,
    avg(discount_rate) as avg_disc,
    count(*) as count_order
from {{ ref('silver_lineitem') }}
where ship_date <= dateadd(day, -90, to_date('1998-12-01'))
group by return_flag, line_status
order by return_flag, line_status;

-- Customer Analysis: Top customers by revenue
select
    customer_name,
    nation_name,
    region_name,
    market_segment,
    customer_tier,
    total_order_value,
    total_orders,
    avg_order_value,
    on_time_delivery_rate
from {{ ref('gold_customer_analytics') }}
where total_order_value > 100000
order by total_order_value desc
limit 20;

-- Regional Trade Analysis: International vs Domestic
select
    customer_region,
    supplier_region,
    trade_type,
    total_customers,
    net_revenue,
    avg_lead_time,
    on_time_delivery_rate
from {{ ref('gold_regional_analysis') }}
where order_year = 1996
order by net_revenue desc;

-- Supplier Performance: Top performing suppliers
select
    supplier_name,
    nation_name as supplier_nation,
    parts_supplied,
    total_revenue_generated,
    customers_served,
    on_time_delivery_rate,
    return_rate,
    revenue_performance_tier,
    delivery_performance_tier
from {{ ref('gold_supplier_performance') }}
where total_revenue_generated > 50000
order by total_revenue_generated desc
limit 15;

-- Monthly Trends: Revenue and growth analysis
select
    report_month,
    net_revenue,
    total_orders,
    active_customers,
    revenue_growth_pct,
    order_growth_pct,
    on_time_delivery_rate,
    domestic_revenue_pct,
    international_revenue_pct
from {{ ref('gold_monthly_kpis') }}
order by report_month;

-- Product Analysis: Best selling parts by category
select
    manufacturer,
    brand,
    part_type,
    price_category,
    count(distinct l.part_key) as unique_parts,
    sum(l.quantity) as total_quantity_sold,
    sum(l.final_price) as total_revenue,
    avg(l.unit_price) as avg_unit_price,
    avg(l.discount_rate) as avg_discount
from {{ ref('silver_lineitem') }} l
join {{ ref('silver_part') }} p on l.part_key = p.part_key
where l.order_year = 1996
group by manufacturer, brand, part_type, price_category
having sum(l.final_price) > 10000
order by total_revenue desc;
