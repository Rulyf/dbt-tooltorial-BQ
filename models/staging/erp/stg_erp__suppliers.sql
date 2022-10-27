with 
    source_data as (
        select
            supplier_id
            , company_name
            , contact_name
            , contact_title
            , 'address' as company_adress
            , city
            , region
            , postal_code
            , country
            , phone
            , fax
            , homepage
        from {{ source('erp','suppliers') }}
    )

select *
from source_data