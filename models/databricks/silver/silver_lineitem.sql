{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'fact', 'lineitem', 'databricks']
  )
}}

select
    l.l_orderkey as order_key,
    l.l_partkey as part_key,
    l.l_suppkey as supplier_key,
    l.l_linenumber as line_number,
    
    -- foreign key lookups
    o.customer_key,
    o.customer_name,
    o.customer_nation,
    o.customer_region,
    o.order_date,
    o.order_status,
    p.part_name,
    p.manufacturer,
    p.brand,
    p.part_type,
    p.price_category as part_price_category,
    s.supplier_name,
    s.nation_name as supplier_nation,
    s.region_name as supplier_region,
    
    -- measures
    l.l_quantity as quantity,
    l.l_extendedprice as extended_price,
    l.l_discount as discount_rate,
    l.l_tax as tax_rate,
    
    -- derived financial calculations
    l.l_extendedprice * (1 - l.l_discount) as discounted_price,
    l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax) as final_price,
    l.l_extendedprice * l.l_discount as discount_amount,
    l.l_extendedprice * (1 - l.l_discount) * l.l_tax as tax_amount,
    case 
        when l.l_quantity = 0 then null
        else l.l_extendedprice / l.l_quantity
    end as unit_price,
    
    -- status and logistics
    upper(trim(l.l_returnflag)) as return_flag,
    upper(trim(l.l_linestatus)) as line_status,
    l.l_shipdate as ship_date,
    l.l_commitdate as commit_date,
    l.l_receiptdate as receipt_date,
    upper(trim(l.l_shipinstruct)) as ship_instructions,
    upper(trim(l.l_shipmode)) as ship_mode,
    trim(l.l_comment) as line_comment,
    
    -- derived date fields (adapted for Spark SQL)
    year(l.l_shipdate) as ship_year,
    quarter(l.l_shipdate) as ship_quarter,
    month(l.l_shipdate) as ship_month,
    
    -- lead time calculations (Spark SQL datediff has different parameter order)
    datediff(l.l_shipdate, o.order_date) as order_to_ship_days,
    datediff(l.l_shipdate, l.l_commitdate) as commit_to_ship_days,
    datediff(l.l_receiptdate, l.l_shipdate) as ship_to_receipt_days,
    datediff(l.l_receiptdate, o.order_date) as total_lead_time_days,
    
    -- performance indicators
    case 
        when l.l_shipdate <= l.l_commitdate then 'ON_TIME'
        else 'LATE'
    end as delivery_performance,
    
    case 
        when datediff(l.l_shipdate, l.l_commitdate) <= 0 then 'ON_TIME'
        when datediff(l.l_shipdate, l.l_commitdate) <= 7 then 'SLIGHTLY_LATE'
        else 'VERY_LATE'
    end as delivery_category,
    
    -- business categorizations
    case 
        when l.l_quantity >= 50 then 'HIGH_VOLUME'
        when l.l_quantity >= 25 then 'MEDIUM_VOLUME'
        else 'LOW_VOLUME'
    end as quantity_category,
    
    case 
        when l.l_extendedprice >= 50000 then 'HIGH_VALUE'
        when l.l_extendedprice >= 25000 then 'MEDIUM_VALUE'
        else 'LOW_VALUE'
    end as line_value_category,
    
    -- cross-regional analysis
    case 
        when o.customer_region = s.supplier_region then 'DOMESTIC'
        else 'INTERNATIONAL'
    end as trade_type,
    
    -- data quality flags
    case 
        when l.l_orderkey is null then true
        else false 
    end as is_missing_order_key,
    
    case 
        when l.l_quantity is null or l.l_quantity <= 0 then true
        else false 
    end as is_invalid_quantity,
    
    case 
        when l.l_extendedprice is null or l.l_extendedprice <= 0 then true
        else false 
    end as is_invalid_price,
    
    case 
        when l.l_shipdate > l.l_receiptdate then true
        else false 
    end as is_invalid_ship_sequence,
    
    case 
        when o.order_key is null then true
        else false 
    end as is_orphaned_order,
    
    case 
        when p.part_key is null then true
        else false 
    end as is_orphaned_part,
    
    case 
        when s.supplier_key is null then true
        else false 
    end as is_orphaned_supplier,
    
    -- metadata
    l._dbt_created_at,
    l._scale_factor,
    l._source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_lineitem') }} l
left join {{ ref('silver_orders') }} o 
    on l.l_orderkey = o.order_key
left join {{ ref('silver_part') }} p 
    on l.l_partkey = p.part_key
left join {{ ref('silver_supplier') }} s 
    on l.l_suppkey = s.supplier_key

-- basic data quality filter
where l.l_orderkey is not null
  and l.l_quantity > 0
  and l.l_extendedprice > 0
  and l.l_shipdate <= l.l_receiptdate  -- ensure logical date sequence
