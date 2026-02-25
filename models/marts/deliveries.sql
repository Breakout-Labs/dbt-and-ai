with
    deliveries as (
        select order_id, delivery_id, delivery_status, delivered_at
        from {{ ref("stg_ecomm__deliveries") }}
    ),

    orders as (select order_id, customer_id from {{ ref("stg_ecomm__orders") }}),

    joined as (
        select
            orders.customer_id,
            deliveries.delivery_id,
            deliveries.delivery_status,
            deliveries.delivered_at
        from deliveries
        inner join orders using (order_id)
    ),

    aggregated as (
        select
            customer_id,
            count(delivery_id) as total_deliveries,
            count(
                case when delivery_status = 'delivered' then delivery_id end
            ) as successful_deliveries,
            count(
                case when delivery_status = 'cancelled' then delivery_id end
            ) as failed_deliveries,
            count(
                case
                    when delivery_status not in ('delivered', 'cancelled')
                    then delivery_id
                end
            ) as other_status_deliveries,
            max(delivered_at) as last_delivery_at
        from joined
        group by 1
    )

select
    *,
    (div0null(successful_deliveries, total_deliveries) * 100)::int as fulfillment_ratio
-- (successful_deliveries::float / nullif(total_deliveries, 0)) * 100 as
-- fulfillment_ratio
from aggregated
