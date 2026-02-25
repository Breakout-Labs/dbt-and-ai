with orders as (
    select
        *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

customers as (
    select
        *
    from {{ ref('stg_ecomm__customers') }}
),

joined as (
    select
        orders.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders using (order_id)
    inner join customers using (customer_id)
),

aggregated as (
    select
        customer_id,
        count(*) as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(delivered_at) as last_delivery_date
    from joined
    group by 1
),

fulfillment as (
    select
        customer_id,
        cast((successful_deliveries / nullif(total_deliveries, 0)) * 100 as integer) as fulfillment_ratio
    from aggregated
)

select
    aggregated.*,
    fulfillment.fulfillment_ratio
from aggregated
inner join fulfillment using (customer_id)
order by customer_id