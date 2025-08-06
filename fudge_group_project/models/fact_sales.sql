{{ config(materialized='table') }}

-- 1) FudgeMart sales: one row per order‐detail
with fudgemart as (
  select
    concat('fudgemart_', to_varchar(o.order_id), '_', to_varchar(od.product_id)) as orderid,
    cast(to_char(o.order_date, 'YYYYMMDD') as int)                    as datekey,
    concat('fudgemart_', to_varchar(o.customer_id))                   as customerkey,
    concat('fudgemart_', to_varchar(od.product_id))                   as productkey,
    od.order_qty                                                      as quantity,
    od.order_qty * p.product_retail_price                             as revenue,
    'fudgemart'                                                      as division
  from {{ source('fudgemart_v3', 'fm_orders') }} o
  join {{ source('fudgemart_v3', 'fm_order_details') }} od
    on o.order_id = od.order_id
  join {{ source('fudgemart_v3', 'fm_products') }} p
    on od.product_id = p.product_id
),

-- 2) FudgeFlix billing: one row per billing record
fudgeflix as (
  select
    concat(
      'fudgeflix_', 
      to_varchar(ab.ab_account_id), '_',
      to_varchar(ab.ab_plan_id), '_',
      to_char(ab.ab_date, 'YYYYMMDD')
    )                                                                 as orderid,
    cast(to_char(ab.ab_date, 'YYYYMMDD') as int)                        as datekey,
    concat('fudgeflix_', to_varchar(ab.ab_account_id))                  as customerkey,
    concat('fudgeflix_', to_varchar(ab.ab_plan_id))                     as productkey,
    1                                                                   as quantity,
    ab.ab_billed_amount                                                 as revenue,
    'fudgeflix'                                                         as division
  from {{ source('fudgeflix_v3', 'ff_account_billing') }} ab
)

-- 3) Union both systems into one fact
select * from fudgemart
union all
select * from fudgeflix
