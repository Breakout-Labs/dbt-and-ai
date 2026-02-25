with
    deliveries as (select * from {{ ref("stg_ecomm__deliveries") }}),

    orders as (select * from {{ ref("orders") }}),

    deliveries_with_customer as (
        select
            orders.customer_id,
            deliveries.delivery_id,
            deliveries.delivery_status,
            deliveries.delivered_at
        from deliveries
        inner join orders on (deliveries.order_id = orders.order_id)
    ),

    customer_delivery_metrics as (
        select
            customer_id,
            count(*) as total_deliveries,
            count(
                case when delivery_status = 'delivered' then 1 end
            ) as successful_deliveries,
            count(
                case when delivery_status = 'cancelled' then 1 end
            ) as failed_deliveries,
            count(
                case when delivery_status not in ('delivered', 'cancelled') then 1 end
            ) as other_status_deliveries,
            max(delivered_at::date) as last_delivery_date
        from deliveries_with_customer
        group by 1
    )

select
    customer_id,
    total_deliveries,
    successful_deliveries,
    failed_deliveries,
    other_status_deliveries,
    last_delivery_date,
    div0(successful_deliveries, total_deliveries) as fulfillment_ratio
from customer_delivery_metrics
