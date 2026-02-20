with customers as (
    select
        *
    from {{ ref('stg_ecomm__customers') }}
),

orders as (
    select
        *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

orders_with_customers as (
    select
        orders.order_id,
        orders.customer_id
    from orders
    inner join customers using (customer_id)
),

delivery_metrics as (
    select
        orders_with_customers.customer_id,
        count(*) as total_deliveries,
        count(case when deliveries.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when deliveries.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when deliveries.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(deliveries.delivered_at) as last_delivery_date
    from deliveries
    inner join orders_with_customers using (order_id)
    group by 1
),

fulfillment_ratio as (
    select
        customer_id,
        case 
            when total_deliveries = 0 then 0
            else successful_deliveries::float / total_deliveries
        end as fulfillment_ratio
    from delivery_metrics
)

select
    customers.customer_id,
    coalesce(delivery_metrics.total_deliveries, 0) as total_deliveries,
    coalesce(delivery_metrics.successful_deliveries, 0) as successful_deliveries,
    coalesce(delivery_metrics.failed_deliveries, 0) as failed_deliveries,
    coalesce(delivery_metrics.other_status_deliveries, 0) as other_status_deliveries,
    delivery_metrics.last_delivery_date,
    coalesce(fulfillment_ratio.fulfillment_ratio, 0) as fulfillment_ratio
from customers
left join delivery_metrics using (customer_id)
left join fulfillment_ratio using (customer_id)
order by customers.customer_id