
{{
    config(
        materialized='semantic_view'
    )
}}

tables (
    {{ ref('customers') }} primary key (customer_id) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.',
    {{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.'
)
relationships (
    orders_to_customers as orders(customer_id) references customers(customer_id)
)
facts (
    orders.total_amount as total_amount comment='The total amount of the order.'
)
dimensions (
    customers.customer_id as customer_id comment='Unique identifier for each customer.',
    customers.email as email comment='Email addresses for customers.',
    customers.first_name as first_name comment='The first name of the customer.',
    customers.last_name as last_name comment='Customer last names.',
    orders.customer_id as customer_id comment='Unique identifier for the customer who placed the order.',
    orders.delivery_time_from_collection as delivery_time_from_collection comment='The time duration between collection and delivery measured in a numeric unit.',
    orders.delivery_time_from_order as delivery_time_from_order comment='The number of time units between when an order was placed and when it was delivered.',
    orders.order_id as order_id comment='Unique identifier for each order.',
    orders.order_status as order_status comment='The current status of the order in its fulfillment lifecycle.',
    orders.store_id as store_id comment='The identifier for the store where the order was placed.',
    orders.ordered_at as ordered_at comment='The date and time when the order was placed.'
)
metrics (
    orders.revenue as sum(total_amount) with synonyms=('sales','total revenue','total sales') comment='Total revenue from orders'
)
comment='Semantic Model Orders Customers'
ai_sql_generation 'The fiscal year starts in October.'
with extension (
    ca='{
        "tables": [
            {
                "name": "customers",
                "dimensions": [
                    {"name": "customer_id", "sample_values": ["7", "36", "37"]},
                    {"name": "email", "sample_values": ["amalea.treat@gmail.com", "cyndi.root@gmail.com", "sienna.lopez@yahoo.com"]},
                    {"name": "first_name", "sample_values": ["Cyndi", "Priya", "Sienna"]},
                    {"name": "last_name", "sample_values": ["Lancetter", "Lindström", "Bransgrove"]}
                ]
            },
            {
                "name": "orders",
                "dimensions": [
                    {"name": "customer_id", "sample_values": ["75", "33", "15"]},
                    {"name": "delivery_time_from_collection", "sample_values": ["100", "75"]},
                    {"name": "delivery_time_from_order", "sample_values": ["35", "75"]},
                    {"name": "order_id", "sample_values": ["69548b40-a9da-44e7-9ebe-c0638ab5d571", "ab805b41-bb2a-4ef2-b3e9-72b4ceef185a", "c96d978b-2029-4edb-8288-b78c2e96b706"]},
                    {"name": "order_status", "sample_values": ["delivered", "pending", "ordered"]},
                    {"name": "store_id", "sample_values": ["1"]}
                ],
                "facts": [
                    {"name": "total_amount", "sample_values": ["27.22", "43.07", "56.58"]}
                ],
                "metrics": [
                    {"name": "revenue"}
                ],
                "time_dimensions": [
                    {"name": "ordered_at", "sample_values": ["2025-11-11T00:00:00.000+0000", "2025-10-02T00:00:00.000+0000", "2025-09-18T00:00:00.000+0000"]}
                ]
            }
        ],
        "relationships": [
            {
                "name": "orders_to_customers",
                "relationship_type": "many_to_one",
                "join_type": "inner"
            }
        ],
        "verified_queries": [
            {
                "name": "\\"0;1\\"",
                "sql": "SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id",
                "question": "What are the complete order details combined with customer information?",
                "verified_at": 1771428168,
                "verified_by": "Semantic Model Generator"
            }
        ]
    }'
)