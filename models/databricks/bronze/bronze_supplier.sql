{{
  config(
    materialized='table',
    file_format='delta',
    tags=['bronze', 'raw', 'supplier', 'databricks']
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
    current_timestamp() as _dbt_created_at,
    '{{ var("databricks_tpch_schema", "tpch") }}' as _scale_factor,
    'databricks_unity_catalog' as _source_system

from {{ source('tpch_databricks', 'supplier') }}
