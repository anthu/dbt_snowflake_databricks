{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'fact', 'orders', 'databricks']
  )
}}

select
    o.o_orderkey as order_key,
    o.o_custkey as customer_key,
    c.customer_name,
    c.nation_name as customer_nation,
    c.region_name as customer_region,
    c.market_segment as customer_segment,
    upper(trim(o.o_orderstatus)) as order_status,
    o.o_totalprice as total_price,
    o.o_orderdate as order_date,
    upper(trim(o.o_orderpriority)) as order_priority,
    trim(o.o_clerk) as clerk,
    o.o_shippriority as ship_priority,
    trim(o.o_comment) as order_comment,
    
    -- derived date fields (adapted for Spark SQL)
    year(o.o_orderdate) as order_year,
    quarter(o.o_orderdate) as order_quarter,
    month(o.o_orderdate) as order_month,
    dayofweek(o.o_orderdate) as order_day_of_week,
    date_trunc('month', o.o_orderdate) as order_month_start,
    date_trunc('quarter', o.o_orderdate) as order_quarter_start,
    date_trunc('year', o.o_orderdate) as order_year_start,
    
    -- derived business fields
    case 
        when o.o_totalprice >= 200000 then 'LARGE'
        when o.o_totalprice >= 100000 then 'MEDIUM'
        when o.o_totalprice >= 50000 then 'SMALL'
        else 'MICRO'
    end as order_size_category,
    
    case 
        when upper(trim(o.o_orderpriority)) in ('1-URGENT', '2-HIGH') then 'HIGH'
        when upper(trim(o.o_orderpriority)) = '3-MEDIUM' then 'MEDIUM'
        else 'LOW'
    end as priority_group,
    
    -- order age calculation (Spark SQL datediff has different parameter order)
    datediff(current_date(), o.o_orderdate) as order_age_days,
    
    -- data quality flags
    case 
        when o.o_orderkey is null then true
        else false 
    end as is_missing_key,
    
    case 
        when o.o_custkey is null then true
        else false 
    end as is_missing_customer,
    
    case 
        when o.o_totalprice is null or o.o_totalprice <= 0 then true
        else false 
    end as is_invalid_price,
    
    case 
        when o.o_orderdate is null then true
        else false 
    end as is_missing_date,
    
    case 
        when c.customer_key is null then true
        else false 
    end as is_orphaned_customer,
    
    -- metadata
    o._dbt_created_at,
    o._scale_factor,
    o._source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_orders') }} o
left join {{ ref('silver_customer') }} c 
    on o.o_custkey = c.customer_key

-- basic data quality filter
where o.o_orderkey is not null
  and o.o_totalprice > 0
  and o.o_orderdate is not null
