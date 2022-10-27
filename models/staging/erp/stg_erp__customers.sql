with 
    source_data as (
        select
            customer_id
            , country
            , city
            , fax
            , postal_code
            , 'address' as customer_adress
            , region
            , contact_name
            , phone
            , company_name
            , contact_title
        from {{ source('erp','customers') }}
    )

select *
from source_data