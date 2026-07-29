with

orders_monthly as (

    select
        store_id,
        date_trunc('month', ordered_at)::date as order_month,
        count(*) as order_count,
        sum(total_amount) as total_revenue,
        round(avg(total_amount), 2) as avg_order_amount
    from {{ ref('stg_ecomm__orders') }}
    group by 1, 2

),

orders_for_delivery as (

    select
        order_id,
        store_id,
        date_trunc('month', ordered_at)::date as order_month,
        ordered_at
    from {{ ref('stg_ecomm__orders') }}

),

deliveries as (

    select
        order_id,
        delivered_at
    from {{ ref('stg_ecomm__deliveries') }}
    where delivery_status = 'delivered'

),

delivery_times as (

    select
        orders_for_delivery.store_id,
        orders_for_delivery.order_month,
        datediff('minute', orders_for_delivery.ordered_at, deliveries.delivered_at) as delivery_time_from_order
    from orders_for_delivery
    inner join deliveries using (order_id)

),

delivery_times_monthly as (

    select
        store_id,
        order_month,
        round(avg(delivery_time_from_order), 2) as avg_delivery_time_from_order
    from delivery_times
    group by 1, 2

),

store_performance as (

    select
        orders_monthly.store_id,
        orders_monthly.order_month,
        orders_monthly.order_count,
        orders_monthly.total_revenue,
        orders_monthly.avg_order_amount,
        delivery_times_monthly.avg_delivery_time_from_order
    from orders_monthly
    left join delivery_times_monthly
        on orders_monthly.store_id = delivery_times_monthly.store_id
        and orders_monthly.order_month = delivery_times_monthly.order_month

),

month_over_month as (

    select
        current_month.store_id,
        current_month.order_month,
        current_month.order_count,
        current_month.total_revenue,
        current_month.avg_order_amount,
        current_month.avg_delivery_time_from_order,
        prior_month.order_count as prior_month_order_count,
        prior_month.total_revenue as prior_month_revenue,
        round(
            (current_month.total_revenue - prior_month.total_revenue) * 100.0
                / nullif(prior_month.total_revenue, 0),
            2
        ) as revenue_mom_change_pct
    from store_performance as current_month
    left join store_performance as prior_month
        on current_month.store_id = prior_month.store_id
        and current_month.order_month = dateadd('month', 1, prior_month.order_month)

)

select * from month_over_month
