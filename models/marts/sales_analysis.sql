{{ config(materialized='semantic_view') }}
    tables (
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.',
        {{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total amount of the order.'
    )
    dimensions (
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
        CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.',
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier for the customer who placed the order.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time duration between collection and delivery measured in a numeric unit.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units between when an order was placed and when it was delivered.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
        ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The timestamp when the order was placed.'
    )
    metrics (
        ORDERS.REVENUE as sum(total_amount) with synonyms=('sales')
    )
    comment='This semantic views provides sales analysis'
    ai_question_categorization '
A fiscal year starting in October means FY2026 runs from October 2025 through September 2026'
    with extension (
        CA='{
            "tables": [
                {
                    "name": "CUSTOMERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["48", "67", "71"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": ["cyndi.root@gmail.com", "amalea.treat@gmail.com", "xerxes.colchett53@gmail.com"]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Cyndi", "Amalea", "Sienna"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Achromov", "Weeden", "Treat"]
                        }
                    ]
                },
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["4", "14", "42"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["90", "75"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["150", "35"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": ["04128e9e-09bb-4bf1-90ba-4f4e5b9fffa6", "0b93f5ef-6734-4c63-8523-08226b1034bb", "bc5e99ce-9c40-4b1d-b22e-d16643239800"]
                        },
                        {
                            "name": "ORDER_STATUS",
                            "sample_values": ["delivered", "ordered", "pending"]
                        },
                        {
                            "name": "STORE_ID",
                            "sample_values": ["1"]
                        }
                    ],
                    "facts": [
                        {
                            "name": "TOTAL_AMOUNT",
                            "sample_values": ["30.61", "37.46", "22.7"]
                        }
                    ],
                    "metrics": [
                        {
                            "name": "REVENUE"
                        }
                    ],
                    "time_dimensions": [
                        {
                            "name": "ORDERED_AT",
                            "sample_values": ["2025-06-03T00:00:00.000+0000", "2025-08-12T00:00:00.000+0000", "2025-04-05T00:00:00.000+0000"]
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
                    "name": "\\"0;1\\"",
                    "sql": "SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id",
                    "question": "What are the complete order details combined with customer information?",
                    "verified_at": 1772032503,
                    "verified_by": "Semantic Model Generator"
                }
            ]
        }'
    );