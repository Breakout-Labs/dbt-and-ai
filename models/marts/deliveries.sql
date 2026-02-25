
with orders as (
 
    select * from {{ ref('stg_ecomm__orders') }}
 
),
 
deliveries as (
 
    select * from {{ ref('stg_ecomm__deliveries') }}
 
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
 
aggregated as (
 
    select
        joined.customer_id,
        count(joined.delivery_id) as total_deliveries,
        count(case when joined.delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when joined.delivery_status = 'failed' then 1 end) as failed_deliveries,
        count(case when joined.delivery_status not in ('delivered', 'failed') then 1 end) as other_status_deliveries,
        max(joined.delivered_at)::date as last_delivery_date,
        (count(case when joined.delivery_status = 'delivered' then 1 end) * 100.0 / count(joined.delivery_id)) as fulfillment_ratio
    from joined
    group by 1
 
)
 
select * from aggregated