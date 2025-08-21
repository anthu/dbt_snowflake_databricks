{{
  config(
    materialized='table',
    file_format='delta',
    tags=['silver', 'cleaned', 'dimension', 'supplier', 'databricks']
  )
}}

select
    s.s_suppkey as supplier_key,
    trim(s.s_name) as supplier_name,
    trim(s.s_address) as supplier_address,
    s.s_nationkey as nation_key,
    n.nation_name,
    n.region_name,
    trim(s.s_phone) as supplier_phone,
    s.s_acctbal as account_balance,
    trim(s.s_comment) as supplier_comment,
    
    -- derived fields
    case 
        when s.s_acctbal > 0 then 'POSITIVE'
        when s.s_acctbal = 0 then 'ZERO'
        else 'NEGATIVE'
    end as balance_category,
    
    case 
        when s.s_acctbal >= 5000 then 'HIGH_VALUE'
        when s.s_acctbal >= 1000 then 'MEDIUM_VALUE'
        when s.s_acctbal >= 0 then 'LOW_VALUE'
        else 'DEFICIT'
    end as supplier_tier,
    
    -- data quality flags
    case 
        when s.s_suppkey is null then true
        else false 
    end as is_missing_key,
    
    case 
        when trim(s.s_name) = '' or s.s_name is null then true
        else false 
    end as is_missing_name,
    
    case 
        when s.s_acctbal is null then true
        else false 
    end as is_missing_balance,
    
    case 
        when n.nation_key is null then true
        else false 
    end as is_orphaned_nation,
    
    -- metadata
    s._dbt_created_at,
    s._scale_factor,
    s._source_system,
    current_timestamp() as _silver_created_at

from {{ ref('bronze_supplier') }} s
left join {{ ref('silver_nation') }} n 
    on s.s_nationkey = n.nation_key

-- basic data quality filter
where s.s_suppkey is not null
