with orders as (
    select *
    from {{ ref('orders') }}
),

monthly as (
    select
        date_trunc('month', ordered_at) as order_month,
        count(*) as total_orders,
        count(delivery_time_from_order) as delivered_orders,
        avg(delivery_time_from_order) as avg_delivery_time_from_order,
        avg(delivery_time_from_collection) as avg_delivery_time_from_collection
    from orders
    group by 1
)

select *
from monthly
