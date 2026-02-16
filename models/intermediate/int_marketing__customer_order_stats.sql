select
    customer_id,
    round(avg(total_amount),2) as avg_order_amount,
    count(*) as order_count
from {{ ref('stg_ecomm__orders') }}
where ordered_at > current_date - 180 -- Include orders from last 6 months
group by 1