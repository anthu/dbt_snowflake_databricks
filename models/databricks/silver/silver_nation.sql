{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'dimension', 'nation', 'databricks']
  )
}}

select
    n.n_nationkey as nation_key,
    upper(trim(n.n_name)) as nation_name,
    n.n_regionkey as region_key,
    r.region_name,
    trim(n.n_comment) as nation_comment,
    
    -- data quality flags
    case 
        when n.n_nationkey is null then true
        else false 
    end as is_missing_key,
    
    case 
        when trim(n.n_name) = '' or n.n_name is null then true
        else false 
    end as is_missing_name,
    
    case 
        when n.n_regionkey is null then true
        else false 
    end as is_missing_region,
    
    case 
        when r.region_key is null then true
        else false 
    end as is_orphaned_region,
    
    -- metadata
    n._dbt_created_at,
    n._scale_factor,
    n._source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_nation') }} n
left join {{ ref('silver_region') }} r 
    on n.n_regionkey = r.region_key

-- basic data quality filter
where n.n_nationkey is not null
