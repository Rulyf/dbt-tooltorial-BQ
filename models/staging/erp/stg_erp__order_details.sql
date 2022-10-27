with source_data as (
    select
        order_id
        , product_id
        , discount
        , unit_price
        , quantity
    from {{ source('erp','order_details') }}
)

select *
from source_data