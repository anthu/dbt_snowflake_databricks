{{
  config(
    materialized='table',
    tags=['bronze', 'raw', 'orders']
  )
}}

select
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    
    -- metadata columns
    current_timestamp as _dbt_created_at,
    '{{ var("tpch_source_schema") }}' as _scale_factor,
    'snowflake_sample_data' as _source_system

from {{ source('tpch', 'orders') }}
