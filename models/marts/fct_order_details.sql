with customers as (
    select
        customer_sk
        , customer_id
    from {{ref('dim_customers')}}
)

, employees as (
    select
        employee_sk
        , employee_id
    from {{ref('dim_employees')}}
)

, products as (
    select
        product_sk
        , product_id
    from {{ref('dim_products')}}
)

, orders_with_sk as (
    select
        orders.order_id
        , employees.employee_sk as employee_fk
        , customers.customer_sk as customer_fk
        , orders.order_date
        , orders.ship_region
        , orders.shipped_date
        , orders.ship_country
        , orders.ship_name
        , orders.ship_postal_code
        , orders.ship_city
        , orders.freight
        , orders.ship_address
        , orders.required_date
    from {{ref('stg_orders')}} orders
    left join employees on orders.employee_id = employees.employee_id
    left join customers on orders.customer_id = customers.customer_id
)

, orders_detail_with_sk as (
    select
        order_dtl.order_id
        , products.product_sk as product_fk
        , order_dtl.discount
        , order_dtl.unit_price
        , order_dtl.quantity
    from {{ref('stg_order_details')}} order_dtl
    left join products on order_dtl.product_id = products.product_id
)

/* We then join orders and orders detail to get the final fact table*/
, final as (
    select
        order_dtl.order_id
        , orders.employee_fk
        , orders.customer_fk
        , orders.order_date
        , orders.ship_region
        , orders.shipped_date
        , orders.ship_country
        , orders.ship_name
        , orders.ship_postal_code
        , orders.ship_city
        , orders.freight
        , orders.ship_address
        , orders.required_date
        , order_dtl.product_fk
        , order_dtl.discount
        , order_dtl.unit_price
        , order_dtl.quantity
        , order_dtl.unit_price * order_dtl.quantity as gross_total
    from orders_with_sk as orders
    left join orders_detail_with_sk as order_dtl on orders.order_id = order_dtl.order_id
)

select * 
from final