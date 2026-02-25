
{{ config(materialized='semantic_view') }}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders placed at various store locations. Each record represents a single order and includes details about the customer, order timing and status, financial totals, and delivery timeframes.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents an individual customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total amount of the order.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier for the customer who placed the order.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time elapsed between collection and delivery measured in a numeric unit.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units elapsed between order placement and delivery.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
        ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store where the order was placed.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The timestamp when the order was placed.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
        CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.'
    )
    metrics (
        ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Sum of total amount'
    )
    comment='This semantic view analysis the sales of our business'
    ai_sql_generation 'A fiscal year starting in October means FY2026 runs from October 2025 through September 2026'
    with extension (
        CA='{
            "tables": [
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["15", "27", "63"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["70", "60", "75"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["35", "150"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": [
                                "287feeb5-90cb-4792-bf18-4db0831a73ef",
                                "2f95d7f1-1eed-4266-95c8-2f744a926326",
                                "65344941-ffee-4421-aa0f-7b593b0342b4"
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
                            "sample_values": ["6.37", "53.92", "45.51"]
                        }
                    ],
                    "metrics": [
                        {
                            "name": "Revenue"
                        }
                    ],
                    "time_dimensions": [
                        {
                            "name": "ORDERED_AT",
                            "sample_values": [
                                "2025-07-20T00:00:00.000+0000",
                                "2025-07-19T00:00:00.000+0000",
                                "2025-04-16T00:00:00.000+0000"
                            ]
                        }
                    ]
                },
                {
                    "name": "CUSTOMERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["62", "25", "9"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": [
                                "eliza.joselson@gmail.com",
                                "amalea.treat@gmail.com",
                                "paulett.bonaire@icloud.com"
                            ]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Amalea", "Cyndi", "Sienna"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Treat", "Root", "Lopez"]
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
                    "verified_at": 1772032140,
                    "verified_by": "Semantic Model Generator"
                }
            ]
        }'
    )