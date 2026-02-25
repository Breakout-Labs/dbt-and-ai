with deliveries as (

    select *
    from {{ ref('stg_deliveries') }}

),

final as (

    select
        deliveries.customer_id,
        count(deliveries.delivery_id) as total_deliveries,
        count(case when deliveries.status = 'delivered' then 1 end) as successful_deliveries,
        count(case when deliveries.status = 'cancelled' then 1 end) as failed_deliveries,
        count(case when deliveries.status not in ('success', 'failed') then 1 end) as other_status_deliveries,
        max(deliveries.delivery_date) as last_delivery_date
    from deliveries
    group by 1
    order by 1

)

select *
from final