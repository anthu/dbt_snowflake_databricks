{{
  config(
    materialized='table',
    tags=['bronze', 'raw', 'supplier']
  )
}}

select
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment,
    
    -- metadata columns
    current_timestamp as _dbt_created_at,
    '{{ var("tpch_source_schema") }}' as _scale_factor,
    'snowflake_sample_data' as _source_system

from {{ source('tpch', 'supplier') }}
