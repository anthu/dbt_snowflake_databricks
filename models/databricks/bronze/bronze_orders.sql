{{
  config(
    materialized='table',
    file_format='delta',
    tags=['bronze', 'raw', 'orders', 'databricks']
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
    current_timestamp() as _dbt_created_at,
    '{{ var("databricks_tpch_schema", "tpch") }}' as _scale_factor,
    'databricks_unity_catalog' as _source_system

from {{ source('tpch_databricks', 'orders') }}
