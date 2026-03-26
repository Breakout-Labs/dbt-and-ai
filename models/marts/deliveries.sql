
with customers as (
    select
        customer_id
    from {{ ref('stg_ecomm__customers') }}
),

orders as (
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
        customers.customer_id,
        deliveries.delivery_id,
        deliveries.delivered_at,
        deliveries.delivery_status
    from customers
    inner join orders on customers.customer_id = orders.customer_id
    inner join deliveries on orders.order_id = deliveries.order_id
),

aggregated as (
    select
        customer_id,
        count(*) as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date
    from joined
    group by 1
),

fulfillment_ratio as (
    select
        customer_id,
        successful_deliveries / nullif(total_deliveries, 0) as fulfillment_ratio
    from aggregated
)

select
    aggregated.customer_id,
    total_deliveries,
    successful_deliveries,
    failed_deliveries,
    other_status_deliveries,
    last_delivery_date,
    fulfillment_ratio.fulfillment_ratio
from aggregated
inner join fulfillment_ratio on aggregated.customer_id = fulfillment_ratio.customer_id
order by aggregated.customer_id