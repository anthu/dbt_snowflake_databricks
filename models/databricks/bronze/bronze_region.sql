{{
  config(
    materialized='table',
    file_format='delta',
    tags=['bronze', 'raw', 'region', 'databricks']
  )
}}

select
    r_regionkey,
    r_name,
    r_comment,
    
    -- metadata columns
    current_timestamp() as _dbt_created_at,
    '{{ var("databricks_tpch_schema", "tpch") }}' as _scale_factor,
    'databricks_unity_catalog' as _source_system

from {{ source('tpch_databricks', 'region') }}
