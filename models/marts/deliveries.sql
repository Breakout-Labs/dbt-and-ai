with orders as (
    select
        orders.order_id,
        orders.customer_id
    from {{ ref('stg_ecomm__orders') }} as orders
),

deliveries as (
    select
        deliveries.delivery_id,
        deliveries.order_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from {{ ref('stg_ecomm__deliveries') }} as deliveries
),

joined as (
    select
        orders.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders on deliveries.order_id = orders.order_id
),

aggregated as (
    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(case when joined.delivery_status = 'delivered' then cast(joined.delivered_at as date) end) as last_delivery_date,
        (count(case when joined.delivery_status = 'delivered' then 1 end) * 100.0 / count(joined.delivery_id)) as fulfillment_ratio
    from joined
    group by 1
)

select
    aggregated.customer_id,
    aggregated.total_deliveries,
    aggregated.successful_deliveries,
    aggregated.failed_deliveries,
    aggregated.other_status_deliveries,
    aggregated.last_delivery_date,
    aggregated.fulfillment_ratio
from aggregated