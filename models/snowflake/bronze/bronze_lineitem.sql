{{
  config(
    materialized='table',
    tags=['bronze', 'raw', 'lineitem']
  )
}}

select
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment,
    
    -- metadata columns
    current_timestamp as _dbt_created_at,
    '{{ var("tpch_source_schema") }}' as _scale_factor,
    'snowflake_sample_data' as _source_system

from {{ source('tpch', 'lineitem') }}
