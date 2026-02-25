
with orders as (
    select
        order_id,
        customer_id
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select
        delivery_id,
        order_id,
        delivered_at,
        delivery_status
    from {{ ref('stg_ecomm__deliveries') }}
),

joined as (
    select
        orders.customer_id,
        deliveries.delivered_at,
        deliveries.delivery_status
    from deliveries
    inner join orders using (order_id)
),

final as (
    select
        joined.customer_id,
        count(*) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(joined.delivered_at)::date as last_delivery_date,
        (count(case when joined.delivery_status = 'delivered' then 1 end) * 100.0 / count(*)) as fulfillment_ratio
    from joined
    group by 1
)

select
    *
from final