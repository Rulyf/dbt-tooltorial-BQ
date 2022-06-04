with source_data as (
    select
        employee_id
        , first_name
        , last_name
        , country
        , city
        , postal_code
        , hire_date
        , 'extension' as employee_extension
        , 'address' as employee_adress
        , birth_date
        , region
        , photo_path
        , home_phone
        , reports_to
        , title
        , title_of_courtesy
        , notes
    from {{ source('northwind_erp','employees') }}
)

select *
from source_data