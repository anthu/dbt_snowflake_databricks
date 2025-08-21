{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'fact', 'partsupp', 'databricks']
  )
}}

select
    ps.ps_partkey as part_key,
    ps.ps_suppkey as supplier_key,
    
    -- foreign key lookups
    p.part_name,
    p.manufacturer,
    p.brand,
    p.part_type,
    p.retail_price as part_retail_price,
    s.supplier_name,
    s.nation_name as supplier_nation,
    s.region_name as supplier_region,
    
    -- measures
    ps.ps_availqty as available_quantity,
    ps.ps_supplycost as supply_cost,
    trim(ps.ps_comment) as partsupp_comment,
    
    -- derived calculations
    ps.ps_availqty * ps.ps_supplycost as total_inventory_value,
    p.retail_price - ps.ps_supplycost as profit_margin,
    case 
        when p.retail_price = 0 then null
        else (p.retail_price - ps.ps_supplycost) / p.retail_price
    end as profit_margin_percent,
    
    -- business categorizations
    case 
        when ps.ps_availqty >= 5000 then 'HIGH_STOCK'
        when ps.ps_availqty >= 1000 then 'MEDIUM_STOCK'
        when ps.ps_availqty >= 100 then 'LOW_STOCK'
        else 'CRITICAL_STOCK'
    end as stock_level_category,
    
    case 
        when ps.ps_supplycost >= 500 then 'EXPENSIVE'
        when ps.ps_supplycost >= 200 then 'MODERATE'
        else 'CHEAP'
    end as cost_category,
    
    case 
        when (p.retail_price - ps.ps_supplycost) / nullif(p.retail_price, 0) >= 0.5 then 'HIGH_MARGIN'
        when (p.retail_price - ps.ps_supplycost) / nullif(p.retail_price, 0) >= 0.2 then 'MEDIUM_MARGIN'
        else 'LOW_MARGIN'
    end as margin_category,
    
    -- supply chain risk indicators
    case 
        when ps.ps_availqty < 100 then 'HIGH_RISK'
        when ps.ps_availqty < 500 then 'MEDIUM_RISK'
        else 'LOW_RISK'
    end as supply_risk,
    
    -- data quality flags
    case 
        when ps.ps_partkey is null then true
        else false 
    end as is_missing_part_key,
    
    case 
        when ps.ps_suppkey is null then true
        else false 
    end as is_missing_supplier_key,
    
    case 
        when ps.ps_availqty is null or ps.ps_availqty < 0 then true
        else false 
    end as is_invalid_quantity,
    
    case 
        when ps.ps_supplycost is null or ps.ps_supplycost <= 0 then true
        else false 
    end as is_invalid_cost,
    
    case 
        when p.part_key is null then true
        else false 
    end as is_orphaned_part,
    
    case 
        when s.supplier_key is null then true
        else false 
    end as is_orphaned_supplier,
    
    -- metadata
    ps._dbt_created_at,
    ps._scale_factor,
    ps._source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_partsupp') }} ps
left join {{ ref('silver_part') }} p 
    on ps.ps_partkey = p.part_key
left join {{ ref('silver_supplier') }} s 
    on ps.ps_suppkey = s.supplier_key

-- basic data quality filter
where ps.ps_partkey is not null
  and ps.ps_suppkey is not null
  and ps.ps_availqty >= 0
  and ps.ps_supplycost > 0
