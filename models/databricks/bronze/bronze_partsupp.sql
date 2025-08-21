{{
  config(
    materialized='table',
    file_format='delta',
    tags=['bronze', 'raw', 'partsupp', 'databricks']
  )
}}

select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment,
    
    -- metadata columns
    current_timestamp() as _dbt_created_at,
    '{{ var("databricks_tpch_schema", "tpch") }}' as _scale_factor,
    'databricks_unity_catalog' as _source_system

from {{ source('tpch_databricks', 'partsupp') }}
