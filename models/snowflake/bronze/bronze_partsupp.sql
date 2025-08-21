{{
  config(
    materialized='table',
    tags=['bronze', 'raw', 'partsupp']
  )
}}

select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment,
    
    -- metadata columns
    current_timestamp as _dbt_created_at,
    '{{ var("tpch_source_schema") }}' as _scale_factor,
    'snowflake_sample_data' as _source_system

from {{ source('tpch', 'partsupp') }}
