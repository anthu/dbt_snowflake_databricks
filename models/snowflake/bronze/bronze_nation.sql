{{
  config(
    materialized='table',
    tags=['bronze', 'raw', 'nation']
  )
}}

select
    n_nationkey,
    n_name,
    n_regionkey,
    n_comment,
    
    -- metadata columns
    current_timestamp as _dbt_created_at,
    '{{ var("tpch_source_schema") }}' as _scale_factor,
    'snowflake_sample_data' as _source_system

from {{ source('tpch', 'nation') }}
