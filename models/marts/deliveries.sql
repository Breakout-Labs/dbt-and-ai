with customers as (
    select
        stg_ecomm__customers.customer_id
    from {{ ref('stg_ecomm__customers') }}
),

orders as (
    select
        stg_ecomm__orders.order_id,
        stg_ecomm__orders.customer_id
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select
        stg_ecomm__deliveries.delivery_id,
        stg_ecomm__deliveries.order_id,
        stg_ecomm__deliveries.delivery_status,
        stg_ecomm__deliveries.delivered_at
    from {{ ref('stg_ecomm__deliveries') }}
),

joined as (
    select
        customers.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from customers
    inner join orders using (customer_id)
    inner join deliveries using (order_id)
),

aggregated as (
    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(date(joined.delivered_at)) as last_delivery_date,
        count(case when joined.delivery_status = 'delivered' then 1 end) / nullif(count(joined.delivery_id), 0) as fulfillment_ratio
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
order by aggregated.customer_id