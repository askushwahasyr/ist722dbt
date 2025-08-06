{{ config(materialized='table') }}

with fudgemart as (
  select
    customer_id        as customerid,
    customer_email     as customeremail,
    customer_firstname as customerfirstname,
    customer_lastname  as customerlastname,
    customer_address   as customeraddress,
    customer_zip       as customerzip,
    'fudgemart'        as source_system
  from {{ source('fudgemart_v3', 'fm_customers') }}
),

fudgeflix as (
  select
    account_id        as customerid,
    account_email     as customeremail,
    account_firstname as customerfirstname,
    account_lastname  as customerlastname,
    account_address   as customeraddress,
    account_zipcode   as customerzip,
    'fudgeflix'       as source_system
  from {{ source('fudgeflix_v3', 'ff_accounts') }}
),

stg_customers as (
  select * from fudgemart
  union all
  select * from fudgeflix
)

select
  concat(source_system, '_', customerid) as customerkey,

  customerid,
  customeremail,
  customerfirstname,
  customerlastname,
  customeraddress,
  customerzip,
  source_system

from stg_customers
