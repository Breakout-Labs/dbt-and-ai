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
    inner join orders using (customer_id)
    inner join deliveries using (order_id)

),

aggregated as (

    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'cancelled') then 1 end) as other_status_deliveries,
        max(joined.delivered_at)::date as last_delivery_date
    from joined
    group by 1

),

fulfillment_ratio as (

    select
        customer_id,
        successful_deliveries / nullif(total_deliveries, 0) as fulfillment_ratio
    from aggregated

)

select
    aggregated.*,
    fulfillment_ratio.fulfillment_ratio
from aggregated
left join fulfillment_ratio using (customer_id)
order by aggregated.customer_id