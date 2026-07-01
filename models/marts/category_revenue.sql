with

order_lines as (
    select *
    from {{ ref('stg_ecomm__order_lines') }}
),

orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
),

products as (
    select *
    from {{ ref('stg_ecomm__products') }}
),

joined as (
    select
        date_trunc('month', orders.ordered_at) as order_month,
        products.product_category,
        orders.order_id,
        order_lines.quantity * order_lines.unit_price as line_revenue
    from order_lines
    inner join orders using (order_id)
    inner join products using (product_id)
),

monthly_category_summary as (
    select
        order_month,
        product_category,
        sum(line_revenue) as total_revenue,
        count(distinct order_id) as number_of_orders,
        sum(line_revenue) / count(distinct order_id) as average_order_value
    from joined
    group by 1, 2
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_month', 'product_category']) }} as category_month_id,
        order_month,
        product_category,
        total_revenue,
        number_of_orders,
        average_order_value
    from monthly_category_summary
)

select * from final
