with orders as (
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
        orders.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders using (order_id)
),

aggregated as (
    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'failed' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'failed') then 1 end) as other_status_deliveries,
        max(joined.delivered_at)::date as last_delivery_date
    from joined
    group by 1
)

select
    aggregated.customer_id as customer_id,
    aggregated.total_deliveries as total_deliveries,
    aggregated.successful_deliveries as successful_deliveries,
    aggregated.failed_deliveries as failed_deliveries,
    aggregated.other_status_deliveries as other_status_deliveries,
    aggregated.last_delivery_date as last_delivery_date,
    case when aggregated.total_deliveries > 0 then 
        aggregated.successful_deliveries / aggregated.total_deliveries::float
    else 
        null 
    end as fulfillment_ratio
from aggregated