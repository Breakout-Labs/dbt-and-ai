
with customers as (

    select *
    from {{ ref('stg_ecomm__customers') }}

),

orders as (

    select *
    from {{ ref('stg_ecomm__orders') }}

),

deliveries as (

    select *
    from {{ ref('stg_ecomm__deliveries') }}

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
        count(delivery_id) as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date,
        count(case when delivery_status = 'delivered' then 1 end) / nullif(count(delivery_id), 0) as fulfillment_ratio
    from joined
    group by 1

)

select *
from aggregated
order by customer_id