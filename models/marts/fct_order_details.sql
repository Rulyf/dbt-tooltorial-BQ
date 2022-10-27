with 
    customers as (
        select
            customer_sk
            , customer_id
        from {{ ref('dim_customers') }}
    )

    , employees as (
        select
            employee_sk
            , employee_id
        from {{ ref('dim_employees') }}
    )

    , products as (
        select
            product_sk
            , product_id
        from {{ ref('dim_products') }}
    )

    , orders_joined as (
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
        from {{ ref('stg_erp__orders') }} as orders
        left join employees on orders.employee_id = employees.employee_id
        left join customers on orders.customer_id = customers.customer_id
    )

    , orders_detail_joined as (
        select
            order_details.order_id
            , products.product_sk as product_fk
            , order_details.discount
            , order_details.unit_price
            , order_details.quantity
        from {{ ref('stg_erp__order_details') }} as order_details
        left join products on order_details.product_id = products.product_id
    )

    /* We then join orders and orders detail to get the final fact table*/
    , orders_items_fan_out as (
        select
            concat(orders.order_id, orders_detail.product_fk) as sk_order_details
            , orders.order_id
            , orders.employee_fk
            , orders.customer_fk
            , orders_detail.product_fk
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
            , orders_detail.discount
            , orders_detail.unit_price
            , orders_detail.quantity
            , orders_detail.unit_price * orders_detail.quantity as gross_revenue
            , orders_detail.unit_price * (1 - orders_detail.discount) * orders_detail.quantity as liquid_revenue
        from orders_joined as orders
        left join orders_detail_joined as orders_detail on orders.order_id = orders_detail.order_id
    )

select * 
from orders_items_fan_out