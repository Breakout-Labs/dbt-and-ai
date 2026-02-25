{{ config(materialized="semantic_view") }}

tables(
    {{ ref("orders") }} comment
    = 'The table contains records of customer orders placed at various store locations. Each record represents a single order and includes details about the customer, order timing and status, financial totals, and delivery timeframes.',
    {{ ref("customers") }} primary key(customer_id) comment
    = 'The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.'
)
relationships(
    orders_to_customers as orders(customer_id) references customers(customer_id)
)
facts(orders.total_amount as total_amount comment = 'The total amount of the order.')
dimensions(
    orders.customer_id as customer_id comment
    = 'The unique identifier for the customer who placed the order.',
    orders.delivery_time_from_collection as delivery_time_from_collection comment
    = 'The time elapsed between collection and delivery measured in a numeric unit.',
    orders.delivery_time_from_order as delivery_time_from_order comment
    = 'The number of time units elapsed between order placement and delivery.',
    orders.order_id as order_id comment
    = 'Unique identifier for each order in the system.',
    orders.order_status as order_status comment
    = 'The current status of the order in its fulfillment lifecycle.',
    orders.store_id as store_id comment
    = 'The identifier for the store where the order was placed.',
    orders.ordered_at as ordered_at comment
    = 'The timestamp when the order was placed.',
    customers.customer_id as customer_id comment
    = 'Unique identifier for each customer in the system.',
    customers.email as email comment = 'Customer email addresses.',
    customers.first_name as first_name comment = 'The first name of the customer.',
    customers.last_name as last_name comment = 'Customer last names.'
)
metrics(
    orders.revenue as sum(total_amount)
    with synonyms = ('sales') comment = 'Revenue is the sum of total_amount.'
)
ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October. Fiscal Year 2026 starts in October 2025.'
with
    extension(
        ca
        = '{
    "tables": [
        {
            "name": "ORDERS",
            "dimensions": [
                {
                    "name": "CUSTOMER_ID",
                    "sample_values": ["4", "15", "63"]
                },
                {
                    "name": "DELIVERY_TIME_FROM_COLLECTION",
                    "sample_values": ["75", "50"]
                },
                {
                    "name": "DELIVERY_TIME_FROM_ORDER",
                    "sample_values": ["150", "60"]
                },
                {
                    "name": "ORDER_ID",
                    "sample_values": [
                        "434eb6d0-6e91-4362-bc44-e298d747f776",
                        "b1869e79-dd18-495b-96c8-73223a420f73",
                        "8689fd20-122c-48e5-bce7-850e9372e527"
                    ]
                },
                {
                    "name": "ORDER_STATUS",
                    "sample_values": ["pending", "delivered", "ordered"]
                },
                {
                    "name": "STORE_ID",
                    "sample_values": ["1"]
                }
            ],
            "facts": [
                {
                    "name": "TOTAL_AMOUNT",
                    "sample_values": ["18.26", "19.8", "40.8"]
                }
            ],
            "metrics": [
                {
                    "name": "revenue"
                }
            ],
            "time_dimensions": [
                {
                    "name": "fiscal_year"
                },
                {
                    "name": "ORDERED_AT",
                    "sample_values": [
                        "2025-04-04T00:00:00.000+0000",
                        "2025-06-03T00:00:00.000+0000",
                        "2025-09-18T00:00:00.000+0000"
                    ]
                }
            ]
        },
        {
            "name": "CUSTOMERS",
            "dimensions": [
                {
                    "name": "CUSTOMER_ID",
                    "sample_values": ["67", "38", "70"]
                },
                {
                    "name": "EMAIL",
                    "sample_values": [
                        "cyndi.root@gmail.com",
                        "iggy.scuse@outlook.com",
                        "pat.pryell@yahoo.com"
                    ]
                },
                {
                    "name": "FIRST_NAME",
                    "sample_values": ["Kris", "Luis", "Cyndi"]
                },
                {
                    "name": "LAST_NAME",
                    "sample_values": ["Marklund", "Peete", "Root"]
                }
            ]
        }
    ],
    "relationships": [
        {
            "name": "ORDERS_TO_CUSTOMERS",
            "relationship_type": "many_to_one",
            "join_type": "inner"
        }
    ],
    "verified_queries": [
        {
            "name": "0;1",
            "sql": "SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id",
            "question": "What are the complete order details combined with customer information?",
            "verified_at": 1772025821,
            "verified_by": "Semantic Model Generator"
        }
    ]
}'
    )
