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

customers as (
    select
        customer_id
    from {{ ref('stg_ecomm__customers') }}
),

joined as (
    select
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at,
        orders.customer_id
    from deliveries
    inner join orders on deliveries.order_id = orders.order_id
),

aggregated as (
    select
        customers.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        max(joined.delivered_at)::date as last_delivery_date,
        coalesce(count(case when joined.delivery_status = 'delivered' then 1 end) / nullif(count(joined.delivery_id), 0), 0) as fulfillment_ratio
    from customers
    left join joined on customers.customer_id = joined.customer_id
    group by 1
)

select
    *
from aggregated
order by aggregated.customer_id