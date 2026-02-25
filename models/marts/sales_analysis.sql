
{{ config(materialized='semantic_view') }}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders placed at various store locations. Each record represents a single order and includes details about the customer, order timing and status, financial totals, and delivery timeframes.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers with their contact information and order history metrics. Each record represents an individual customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total amount of the order.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier for the customer who placed the order.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time duration between collection and delivery measured in a numeric unit.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units elapsed between order placement and delivery.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
        ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
        CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.'
    )
    metrics (
        ORDERS.REVENUE as SUM(TOTAL_AMOUNT) with synonyms=('Sales') comment='Revenue is the sum of 
total_amount. Some team members also call it sales'
    )
    comment='SALES_ANALYSIS'
    ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October.'
    with extension (
        CA='{
            "tables": [
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["15", "63", "4"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["75", "5"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["150", "135"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": [
                                "782ecdb9-4809-4c6f-a257-da0429f55b42",
                                "4bfdf66e-da57-4891-b5ed-05474b10f6e9",
                                "91c863e6-6978-42bd-8015-8d64c18ae04b"
                            ]
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
                            "sample_values": ["52.39", "39.52", "12.55"]
                        }
                    ],
                    "metrics": [
                        {
                            "name": "revenue"
                        }
                    ],
                    "time_dimensions": [
                        {
                            "name": "ORDERED_AT",
                            "sample_values": [
                                "2025-11-25T00:00:00.000+0000",
                                "2025-08-21T00:00:00.000+0000",
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
                            "sample_values": ["35", "57", "4"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": [
                                "amalea.treat@gmail.com",
                                "yevett e.monier@outlook.com",
                                "cyndi.root@gmail.com"
                            ]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Jeno", "Cyndi", "Amalea"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Treat", "Joselson", "Root"]
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
                    "verified_at": 1772032663,
                    "verified_by": "Semantic Model Generator"
                }
            ]
        }'
    )