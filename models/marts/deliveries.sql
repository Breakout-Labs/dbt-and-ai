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

final as (

    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(joined.delivered_at) as last_delivery_date,
        count(case when joined.delivery_status = 'delivered' then 1 end) / nullif(count(joined.delivery_id), 0) as fulfillment_ratio
    from joined
    group by 1
    order by 1

)

select *
from final