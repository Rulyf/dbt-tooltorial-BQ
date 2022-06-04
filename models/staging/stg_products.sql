with source_data as (
    select
        product_id
        , supplier_id				
        , category_id				
        , product_name								
        , quantity_per_unit as qty_per_unit			
        , unit_price			
        , units_in_stock				
        , units_on_order				
        , cast(reorder_level as int) as reorder_level				
        , case
            when discontinued = 0 then 'No'  
            else 'Yes'
        end as is_discontinued		
    from {{ source('northwind_erp', 'products') }}
)

select *
from source_data