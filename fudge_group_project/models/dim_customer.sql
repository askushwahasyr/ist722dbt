with stg_customers as (

    select
        account_id::varchar as customerid,
        account_email as customeremail,
        account_firstname as customerfirstname,
        account_lastname as customerlastname,
        account_address as customeraddress,
        z.zip_city as customercity,
        z.zip_state as customerstate,
        account_zipcode as customerzip,
        null as customerphone,
        null as customerfax,
        account_opened_on,
        'fudgeflix' as source_system
    from {{ source('fudgeflix_v3', 'ff_accounts') }} a
    left join {{ source('fudgeflix_v3', 'ff_zipcodes') }} z
        on a.account_zipcode = z.zip_code

    union all

    select
        customer_id::varchar as customerid,
        customer_email,
        customer_firstname,
        customer_lastname,
        customer_address,
        customer_city,
        customer_state,
        customer_zip,
        customer_phone,
        customer_fax,
        null as account_opened_on,
        'fudgemart' as source_system
    from {{ source('fudgemart_v3', 'fm_customers') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['customerid', 'source_system']) }} as customerkey,
    *
from stg_customers
