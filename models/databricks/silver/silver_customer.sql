{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'dimension', 'customer', 'databricks']
  )
}}

select
    c.c_custkey as customer_key,
    trim(c.c_name) as customer_name,
    trim(c.c_address) as customer_address,
    c.c_nationkey as nation_key,
    n.nation_name,
    n.region_name,
    trim(c.c_phone) as customer_phone,
    c.c_acctbal as account_balance,
    upper(trim(c.c_mktsegment)) as market_segment,
    trim(c.c_comment) as customer_comment,
    
    -- derived fields
    case 
        when c.c_acctbal > 0 then 'POSITIVE'
        when c.c_acctbal = 0 then 'ZERO'
        else 'NEGATIVE'
    end as balance_category,
    
    case 
        when c.c_acctbal >= 5000 then 'HIGH_VALUE'
        when c.c_acctbal >= 1000 then 'MEDIUM_VALUE'
        when c.c_acctbal >= 0 then 'LOW_VALUE'
        else 'DEFICIT'
    end as customer_tier,
    
    -- data quality flags
    case 
        when c.c_custkey is null then true
        else false 
    end as is_missing_key,
    
    case 
        when trim(c.c_name) = '' or c.c_name is null then true
        else false 
    end as is_missing_name,
    
    case 
        when c.c_acctbal is null then true
        else false 
    end as is_missing_balance,
    
    case 
        when n.nation_key is null then true
        else false 
    end as is_orphaned_nation,
    
    -- metadata
    c._dbt_created_at,
    c._scale_factor,
    c._source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_customer') }} c
left join {{ ref('silver_nation') }} n 
    on c.c_nationkey = n.nation_key

-- basic data quality filter
where c.c_custkey is not null
