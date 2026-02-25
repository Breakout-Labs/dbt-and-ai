
with orders as (
    select
        orders.order_id,
        orders.customer_id
    from {{ ref('stg_ecomm__orders') }} as orders
),

deliveries as (
    select
        deliveries.order_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from {{ ref('stg_ecomm__deliveries') }} as deliveries
),

joined as (
    select
        orders.customer_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders on deliveries.order_id = orders.order_id
),

final as (
    select
        joined.customer_id,
        count(*) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) / nullif(count(*), 0) as fulfillment_ratio,
        max(joined.delivered_at)::date as last_delivery_date,
    from joined
    group by 1
)

select
    *
from final