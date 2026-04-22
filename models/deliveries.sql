with deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

orders as (
    select
        *
    from {{ ref('stg_ecomm__orders') }}
),

joined as (
    select
        deliveries.delivery_id,
        deliveries.order_id,
        orders.customer_id,
        orders.store_id,
        orders.ordered_at,
        deliveries.picked_up_at,
        deliveries.delivered_at,
        deliveries.delivery_status,
        orders.order_status,
        orders.total_amount,
        datediff('minute', orders.ordered_at, deliveries.delivered_at) as delivery_time_from_order,
        datediff('minute', deliveries.picked_up_at, deliveries.delivered_at) as delivery_time_from_collection,
        deliveries._synced_at
    from deliveries
    inner join orders using (order_id)
),

fulfillment_ratio as (
    select
        store_id,
        count(case when delivery_status = 'delivered' then 1 end) / count(*) as fulfillment_ratio
    from joined
    group by store_id
)

select
    joined.*,
    fr.fulfillment_ratio
from joined
left join fulfillment_ratio fr on joined.store_id = fr.store_id