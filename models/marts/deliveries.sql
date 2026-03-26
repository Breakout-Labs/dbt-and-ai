
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

orders_with_deliveries as (

    select
        orders.customer_id,
        deliveries.delivery_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from orders
    left join deliveries using (order_id)

),

delivery_metrics as (

    select
        customer_id,
        count(delivery_id)                                                                          as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end)                                  as successful_deliveries,
        count(case when delivery_status = 'cancelled' then 1 end)                                  as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'cancelled') then 1 end)              as other_status_deliveries,
        max(delivered_at)                                                                           as last_delivery_date
    from orders_with_deliveries
    group by 1

),

fulfillment_ratio as (

    select
        customer_id,
        case 
            when total_deliveries > 0 then successful_deliveries::float / total_deliveries
            else 0
        end as fulfillment_ratio
    from delivery_metrics

),

final as (

    select
        customers.customer_id,
        coalesce(delivery_metrics.total_deliveries, 0)          as total_deliveries,
        coalesce(delivery_metrics.successful_deliveries, 0)     as successful_deliveries,
        coalesce(delivery_metrics.failed_deliveries, 0)         as failed_deliveries,
        coalesce(delivery_metrics.other_status_deliveries, 0)   as other_status_deliveries,
        delivery_metrics.last_delivery_date,
        coalesce(fulfillment_ratio.fulfillment_ratio, 0)        as fulfillment_ratio
    from customers
    left join delivery_metrics using (customer_id)
    left join fulfillment_ratio using (customer_id)

)

select
    *
from final
order by customer_id