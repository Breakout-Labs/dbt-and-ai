
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
        orders.total_amount as total_amount comment='The total monetary amount of the order.'
    )
    dimensions (
        customers.customer_id as customer_id comment='Unique identifier for each customer in the system.',
        customers.email as email comment='Customer email addresses.',
        customers.first_name as first_name comment='The first name of the customer.',
        customers.last_name as last_name comment='Customer last names.',
        orders.customer_id as customer_id comment='The unique identifier for the customer who placed the order.',
        orders.delivery_time_from_collection as delivery_time_from_collection comment='The time duration between collection and delivery measured in a numeric unit.',
        orders.delivery_time_from_order as delivery_time_from_order comment='The number of time units between when an order was placed and when it was delivered.',
        orders.order_id as order_id comment='Unique identifier for each order in the system.',
        orders.order_status as order_status comment='The current status of the order in its fulfillment lifecycle.',
        orders.store_id as store_id comment='The unique identifier for the store where the order was placed.',
        orders.ordered_at as ordered_at comment='The date and time when the order was placed.'
    )
    metrics (
        orders.revenue as sum(total_amount) with synonyms=('sales') comment='Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October'
    )
    comment='sales_analysis ne semantic view for lab12'
    ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October.'
    ai_question_categorization 'When we refer to a year, we mean our fiscal year which starts in October.'
with extension (
    ca='{
        "tables": [
            {
                "name": "customers",
                "dimensions": [
                    {
                        "name": "customer_id",
                        "sample_values": ["72", "49", "4"]
                    },
                    {
                        "name": "email",
                        "sample_values": ["cyndi.root@gmail.com", "erl.digginson@gmail.com", "godiva.eidler@icloud.com"]
                    },
                    {
                        "name": "first_name",
                        "sample_values": ["Lenna", "Gwennie", "Amalea"]
                    },
                    {
                        "name": "last_name",
                        "sample_values": ["Treat", "Lopez", "Reyes"]
                    }
                ]
            },
            {
                "name": "orders",
                "dimensions": [
                    {
                        "name": "customer_id",
                        "sample_values": ["12", "15", "63"]
                    },
                    {
                        "name": "delivery_time_from_collection",
                        "sample_values": ["25", "5", "75"]
                    },
                    {
                        "name": "delivery_time_from_order",
                        "sample_values": ["35", "150"]
                    },
                    {
                        "name": "order_id",
                        "sample_values": ["a71c4f73-0164-4f7b-805b-ceb965498f9b", "2f95d7f1-1eed-4266-95c8-2f744a926326", "7666a165-4389-4b9e-ac1e-ebd46371dd80"]
                    },
                    {
                        "name": "order_status",
                        "sample_values": ["ordered", "delivered", "pending"]
                    },
                    {
                        "name": "store_id",
                        "sample_values": ["1"]
                    }
                ],
                "facts": [
                    {
                        "name": "total_amount",
                        "sample_values": ["5.41", "45.51", "37.16"]
                    }
                ],
                "metrics": [
                    {
                        "name": "revenue"
                    }
                ],
                "time_dimensions": [
                    {
                        "name": "ordered_at",
                        "sample_values": ["2025-10-06T00:00:00.000+0000", "2025-05-28T00:00:00.000+0000", "2025-05-12T00:00:00.000+0000"]
                    }
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
                "verified_at": 1772032256,
                "verified_by": "Semantic Model Generator"
            }
        ]
    }'
)