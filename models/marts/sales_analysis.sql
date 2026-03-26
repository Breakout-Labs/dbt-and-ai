
{{ 
    config(
        materialized='semantic_view'
        ) 
}}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total value, and delivery timing metrics.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures a customer''s satisfaction survey results alongside a comprehensive summary of their ordering activity, including order counts across various time windows and average delivery time metrics.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of minutes between collection and delivery of an order.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The estimated or actual delivery time measured in minutes from when an order was placed.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
        ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
        CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with customers.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.'
    )
    metrics (
        ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Revenue is the sum of total_amount.'
    )
    ai_sql_generation 'A fiscal year starting in October means FY2026 runs from October 2025 through September 2026 - based on ordered_at date'
    ai_verified_queries (
        "0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1774537994
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id')
    )
    with extension (
        CA = '{
            "tables": [
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["20", "15", "4"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["75", "5", "50"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["35", "20"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": ["68787477-41f4-425a-9155-f88f7ae96c86", "cde2d423-5b16-4a87-bcf8-d05bd40f758f", "0c172d88-05d5-4af9-97cf-01616ab5f8c6"]
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
                            "sample_values": ["32.36", "56.13", "26.56"]
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
                            "sample_values": ["2025-03-24T00:00:00.000+0000", "2025-03-29T00:00:00.000+0000", "2025-06-03T00:00:00.000+0000"]
                        }
                    ]
                },
                {
                    "name": "CUSTOMERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["59", "12", "67"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": ["sienna.lopez@yahoo.com", "amalea.treat@gmail.com", "cyndi.root@gmail.com"]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Amalea", "Rhea", "Camila"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Marklund", "Lopez", "Lancetter"]
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
            ]
        }'
    )