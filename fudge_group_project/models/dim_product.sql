{{ config(materialized='table') }}

with fudgemart as (
  select
    product_id   as productid,
    product_name as productname,
    'fudgemart'  as source_system
  from {{ source('fudgemart_v3', 'fm_products') }}
),

fudgeflix as (
  select
    plan_id     as productid,
    plan_name   as productname,
    'fudgeflix' as source_system
  from {{ source('fudgeflix_v3', 'ff_plans') }}
),

stg_products as (
  select * from fudgemart
  union all
  select * from fudgeflix
)

select
  -- string key: "fudgemart_ABC" or "fudgeflix_XYZ"
  concat(source_system, '_', productid) as productkey,

  productid,
  productname,
  source_system

from stg_products
