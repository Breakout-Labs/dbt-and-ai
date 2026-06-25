with orders as (
    select * from {{ ref('stg_ecomm__orders') }}
),

delivered_orders as (
    select * from orders
    where order_status = 'delivered'
),

monthly_revenue as (
    select
        date_trunc('month', ordered_at)::date as revenue_month,
        count(order_id)                        as order_count,
        sum(total_amount)                      as total_revenue
    from delivered_orders
    group by 1
),

final as (
    select * from monthly_revenue
)

select * from final
