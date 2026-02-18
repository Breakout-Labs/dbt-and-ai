with deliveries as (
    select
        order_id,
        delivery_status,
        delivered_at
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
        count(case when delivery_status = 'failed' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'failed') then 1 end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date
    from joined
    group by 1
)

select
    customer_id,
    total_deliveries,
    successful_deliveries,
    failed_deliveries,
    other_status_deliveries,
    last_delivery_date
with deliveries as (
    select
        order_id,
        delivery_status,
        delivered_at
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
        count(case when delivery_status = 'failed' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'failed') then 1 end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date,
        count(case when delivery_status = 'delivered' then 1 end) / nullif(count(*), 0) as fulfillment_ratio
    from joined
    group by 1
)

select
    customer_id,
    total_deliveries,
    successful_deliveries,
    failed_deliveries,
    other_status_deliveries,
    last_delivery_date,
    fulfillment_ratio
from aggregated