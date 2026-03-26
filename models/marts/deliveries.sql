with

deliveries as (
    select * from {{ ref('stg_ecomm__deliveries') }}
),

orders as (
    select * from {{ ref('stg_ecomm__orders') }}
),

customers as (
    select * from {{ ref('stg_ecomm__customers') }}
),

deliveries_by_order as (
    select
        deliveries.order_id,
        count(deliveries.delivery_id) as total_deliveries,
        count(
            case when deliveries.delivery_status = 'delivered' then 1 end
        ) as successful_deliveries,
        count(
            case when deliveries.delivery_status = 'cancelled' then 1 end
        ) as failed_deliveries,
        count(
            case
                when deliveries.delivery_status not in ('delivered', 'cancelled') then 1
            end
        ) as other_status_deliveries,
        max(deliveries.delivered_at) as last_delivery_at
    from deliveries
    group by 1
),

deliveries_by_customer as (
    select
        orders.customer_id,
        sum(deliveries_by_order.total_deliveries) as total_deliveries,
        sum(deliveries_by_order.successful_deliveries) as successful_deliveries,
        sum(deliveries_by_order.failed_deliveries) as failed_deliveries,
        sum(deliveries_by_order.other_status_deliveries) as other_status_deliveries,
        max(deliveries_by_order.last_delivery_at) as last_delivery_at
    from deliveries_by_order
    inner join orders
        on deliveries_by_order.order_id = orders.order_id
    group by 1
),

final as (
    select
        customers.customer_id,
        deliveries_by_customer.total_deliveries,
        deliveries_by_customer.successful_deliveries,
        deliveries_by_customer.failed_deliveries,
        deliveries_by_customer.other_status_deliveries,
        cast(deliveries_by_customer.last_delivery_at as date) as last_delivery_date,
        case when deliveries_by_customer.total_deliveries > 0 then 
            deliveries_by_customer.successful_deliveries::float / deliveries_by_customer.total_deliveries
        else null end as fulfillment_ratio
    from customers
    left join deliveries_by_customer
        on customers.customer_id = deliveries_by_customer.customer_id
)

select * from final
order by final.customer_id