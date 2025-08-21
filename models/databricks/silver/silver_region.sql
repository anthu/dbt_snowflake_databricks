{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'dimension', 'region', 'databricks']
  )
}}

select
    r_regionkey as region_key,
    upper(trim(r_name)) as region_name,
    trim(r_comment) as region_comment,
    
    -- data quality flags
    case 
        when r_regionkey is null then true
        else false 
    end as is_missing_key,
    
    case 
        when trim(r_name) = '' or r_name is null then true
        else false 
    end as is_missing_name,
    
    -- metadata
    _dbt_created_at,
    _scale_factor,
    _source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_region') }}

-- basic data quality filter
where r_regionkey is not null
