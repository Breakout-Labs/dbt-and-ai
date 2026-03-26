with deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

orders as (
    select
        order_id,
        customer_id
    from {{ ref('stg_ecomm__orders') }}
),

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
        count(*) as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        round(div0(
            count(case when delivery_status = 'delivered' then 1 end),
            count(*)
        ) * 100, 2) as fulfillment_ratio,
        max(delivered_at)::date as last_delivery_date
    from joined
    group by 1
)

select
    *
from aggregated