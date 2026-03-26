with orders as (
    select
        order_id,
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
        orders.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders on deliveries.order_id = orders.order_id
),
 
final as (
    select
        customer_id,
        count(*) as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when delivery_status = 'cancelled' then 1 end) as cancelled_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date,
        count(case when delivery_status = 'delivered' then 1 end) * 1.0 / count(*) as fulfillment_ratio
    from joined
    group by 1
)
 
select
    *
from final