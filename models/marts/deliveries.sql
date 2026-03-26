with customers as (
    select
        customers.customer_id
    from {{ ref('stg_ecomm__customers') }} as customers
),

orders as (
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
        customers.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from customers
    left join orders on (
        customers.customer_id = orders.customer_id
    )
    left join deliveries on (
        orders.order_id = deliveries.order_id
    )
),

aggregated as (
    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,  -- was count(*)
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(joined.delivered_at)::date as last_delivery_date
    from joined
    group by 1
)

select
    aggregated.*,
    (successful_deliveries::float / nullif(total_deliveries, 0)) as fulfillment_ratio
from aggregated
order by aggregated.customer_id