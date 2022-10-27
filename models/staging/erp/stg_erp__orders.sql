with 
    source_data as (
        select
            order_id
            , employee_id
            , customer_id
            , ship_via as shipper_id
            , order_date
            , ship_region
            , shipped_date
            , required_date
            , ship_country
            , ship_city
            , ship_name
            , ship_address
            , ship_postal_code
            , freight
        from {{ source('erp','orders') }}
    )

select *
from source_data