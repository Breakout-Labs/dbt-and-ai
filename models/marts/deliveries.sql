with deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),


joined as (
    select
        orders.customer_id,
        deliveries.delivery_status,
        deliveries.delivered_at
    from deliveries
    inner join orders using (order_id)
),

aggregated as (
    select
        customer_id,
        count(*) as total_deliveries,
        count(case when delivery_status = 'delivered' then 1 end) as successful_deliveries,
        count(case when delivery_status = 'failed' then 1 end) as failed_deliveries,
        count(case when delivery_status not in ('delivered', 'failed') then 1 end) as other_status_deliveries,
        max(delivered_at)::date as last_delivery_date,
        count(case when delivery_status = 'delivered' then 1 end) * 100.0 / count(*) as percentage_fulfilled
    from joined
    group by 1
)


select
    *
from aggregated