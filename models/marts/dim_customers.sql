with 
    staging as (
        select *
        from {{ ref('stg_erp__customers') }}
    )
        
    , transformed as (
        select
            row_number() over (order by customer_id) as customer_sk -- auto-incremental surrogate key
            , customer_id
            , country
            , city
            , fax
            , postal_code   
            , customer_adress
            , region
            , contact_name
            , phone
            , company_name
            , contact_title
        from staging
    )

select *
from transformed