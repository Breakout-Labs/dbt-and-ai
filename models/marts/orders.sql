with orders as (
    select
        *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select
        *
    from deliveries
    where delivery_status = 'delivered'
),

joined as (
    select
        order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        orders.store_id,
        datediff('minute', orders.ordered_at, deliveries_filtered.delivered_at) as delivery_time_from_order,
        datediff('minute', deliveries_filtered.picked_up_at, deliveries_filtered.delivered_at) as delivery_time_from_collection
    from orders
    left join deliveries_filtered using (order_id)
)

select
  *
from joined