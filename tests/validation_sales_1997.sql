with 
    sales_1997 as (
        select sum(gross_revenue) as gross_revenue
        from {{ ref('fct_order_details') }}
        where order_date between '1997-01-01' and '1997-12-31'
    )

select *
from sales_1997
where gross_revenue not between 658388 and 658389