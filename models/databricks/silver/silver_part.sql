{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'dimension', 'part', 'databricks']
  )
}}

select
    p_partkey as part_key,
    trim(p_name) as part_name,
    upper(trim(p_mfgr)) as manufacturer,
    upper(trim(p_brand)) as brand,
    upper(trim(p_type)) as part_type,
    p_size as part_size,
    upper(trim(p_container)) as container,
    p_retailprice as retail_price,
    trim(p_comment) as part_comment,
    
    -- derived fields
    case 
        when p_retailprice >= 1500 then 'PREMIUM'
        when p_retailprice >= 1000 then 'STANDARD'
        else 'ECONOMY'
    end as price_category,
    
    case 
        when p_size >= 30 then 'LARGE'
        when p_size >= 15 then 'MEDIUM'
        else 'SMALL'
    end as size_category,
    
    -- extract type components using split function (Spark SQL)
    split(upper(trim(p_type)), ' ')[0] as type_category,
    split(upper(trim(p_type)), ' ')[1] as type_modifier,
    split(upper(trim(p_type)), ' ')[2] as type_material,
    
    -- data quality flags
    case 
        when p_partkey is null then true
        else false 
    end as is_missing_key,
    
    case 
        when trim(p_name) = '' or p_name is null then true
        else false 
    end as is_missing_name,
    
    case 
        when p_retailprice is null or p_retailprice <= 0 then true
        else false 
    end as is_invalid_price,
    
    case 
        when p_size is null or p_size <= 0 then true
        else false 
    end as is_invalid_size,
    
    -- metadata
    _dbt_created_at,
    _scale_factor,
    _source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_part') }}

-- basic data quality filter
where p_partkey is not null
  and p_retailprice > 0
  and p_size > 0
