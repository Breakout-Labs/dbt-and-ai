
{{
    config(
        materialized='semantic_view'
    )
}}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders, capturing key details about each transaction. Each record represents a single order and includes information about the customer, store, order status, financial totals, and delivery timing metrics.'
    )
    facts (
        orders.total_amount as total_amount comment='The total monetary amount associated with an order.'
    )
    dimensions (
        orders.customer_id as customer_id comment='The unique identifier assigned to each customer.',
        orders.delivery_time_from_collection as delivery_time_from_collection comment='The number of minutes between collection and delivery for an order.',
        orders.delivery_time_from_order as delivery_time_from_order comment='The estimated or actual delivery time measured in minutes from when an order was placed.',
        orders.order_id as order_id comment='Unique identifier assigned to each order.',
        orders.order_status as order_status comment='The current status of an order.',
        orders.store_id as store_id comment='The unique identifier for the store associated with an order.',
        orders.ordered_at as ordered_at comment='The date and time when an order was placed.'
    )
    metrics (
        orders.orders_delivered as count(case when order_status = 'delivered' then 1 end) comment='Counts the number of orders with a status of ''delivered''. Use when questions ask about ''delivered orders'', ''how many orders were delivered'', ''delivery count'', or ''fulfilled orders''. Helps measure fulfillment performance, track delivery volumes, and evaluate operational efficiency in completing orders.',
        orders.revenue as sum(total_amount) with synonyms=('sales')
    )
    comment='sales analysis'
    ai_sql_generation 'The fiscal year starts in october'
    with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["15","63","4"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["5","75"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["150","35","25"]},{"name":"ORDER_ID","sample_values":["d59ebdb1-d27b-42e7-bb76-abb86878d2d0","782ecdb9-4809-4c6f-a257-da0429f55b42","907f2207-77dd-425d-bbcf-933c9f5f3656"]},{"name":"ORDER_STATUS","sample_values":["ordered","pending","delivered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["40.01","35.35","56.65"]}],"metrics":[{"name":"orders_delivered"},{"name":"revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-11-02T00:00:00.000+0000","2025-06-03T00:00:00.000+0000","2025-04-25T00:00:00.000+0000"]}]}]}');