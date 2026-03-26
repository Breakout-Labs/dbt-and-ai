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
        customers.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from customers
    inner join orders
        on customers.customer_id = orders.customer_id
    inner join deliveries
        on orders.order_id = deliveries.order_id

),

aggregated as (

    select
        customer_id,
        count(delivery_id) as total_deliveries,
        count(case when delivery_status = 'delivered' then delivery_id end) as successful_deliveries,
        count(case when delivery_status = 'cancelled' then delivery_id end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then delivery_id end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date,
        count(case when delivery_status = 'delivered' then delivery_id end) / nullif(count(delivery_id), 0) as fulfillment_ratio
    from joined
    group by 1

)

select *
from aggregated
order by customer_id