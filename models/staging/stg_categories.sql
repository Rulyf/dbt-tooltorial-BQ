with source_data as (
    select
        category_id				
        , category_name				
        , 'description'	as category_description			
        , picture				
    from {{ source('northwind_erp', 'categories') }}
)

select *
from source_data