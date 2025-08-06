{{ config(materialized='table') }}

-- 1) stage FudgeMart products
with fudgemart as (
  select
    product_id   as productid,
    product_name as productname,
    'fudgemart'  as source_system
  from {{ source('fudgemart_v3', 'fm_products') }}
),

-- 2) stage FudgeFlix plans
fudgeflix as (
  select
    plan_id   as productid,
    plan_name as productname,
    'fudgeflix' as source_system
  from {{ source('fudgeflix_v3', 'ff_plans') }}
),

-- 3) union all into one staging set
stg_products as (
  select * from fudgemart
  union all
  select * from fudgeflix
)

-- 4) build the true dimension with a unique surrogate key
select
  {{ dbt_utils.generate_surrogate_key(
       ['stg_products.source_system', 'stg_products.productid']
     )
  }} as productkey,

  stg_products.productid,
  stg_products.productname,
  stg_products.source_system

from stg_products
