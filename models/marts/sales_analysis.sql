
{{ config(materialized='semantic_view') }}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers with their contact information and order history metrics. Each record includes personal details, satisfaction measurements, and aggregated statistics about ordering behavior across various time periods including delivery performance and order frequency.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount of the order.',
        CUSTOMERS.AVERAGE_DELIVERY_TIME_FROM_COLLECTION as AVERAGE_DELIVERY_TIME_FROM_COLLECTION comment='The average time duration between collection and delivery.',
        CUSTOMERS.AVERAGE_DELIVERY_TIME_FROM_ORDER as AVERAGE_DELIVERY_TIME_FROM_ORDER comment='The average time duration between when an order is placed and when it is delivered.',
        CUSTOMERS.ID as ID comment='Unique identifier for each customer.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier for the customer who placed the order.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time duration between collection and delivery measured in a numeric unit.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units between when an order was placed and when it was delivered.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
        ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The timestamp when the order was placed.',
        CUSTOMERS.ADDRESS as ADDRESS comment='Street addresses for customers.',
        CUSTOMERS.COUNT_ORDERS as COUNT_ORDERS comment='The number of orders associated with the customer.',
        CUSTOMERS.COUNT_ORDERS_LAST_30_DAYS as COUNT_ORDERS_LAST_30_DAYS comment='The number of orders placed by the customer in the last 30 days.',
        CUSTOMERS.COUNT_ORDERS_LAST_360_DAYS as COUNT_ORDERS_LAST_360_DAYS comment='The number of orders placed by the customer in the last 360 days.',
        CUSTOMERS.COUNT_ORDERS_LAST_90_DAYS as COUNT_ORDERS_LAST_90_DAYS comment='The number of orders placed by the customer in the last 90 days.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
        CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.',
        CUSTOMERS.PHONE_NUMBER as PHONE_NUMBER comment='Customer phone numbers.',
        CUSTOMERS.SATISFACTION_SCORE as SATISFACTION_SCORE comment='Customer satisfaction score represented as a numeric value.',
        CUSTOMERS.CREATED_AT as CREATED_AT comment='The timestamp when the customer record was created in the system.',
        CUSTOMERS.FIRST_ORDER_AT as FIRST_ORDER_AT comment='The timestamp when the customer placed their first order.',
        CUSTOMERS.MOST_RECENT_ORDER_AT as MOST_RECENT_ORDER_AT comment='The timestamp of the customer''s most recent order.',
        CUSTOMERS.SURVEY_DATE as SURVEY_DATE comment='The date when the customer survey was conducted.'
    )
    metrics (
        ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Revenue is the sum of total_amount'
    )
    comment='Sales Analysis from Orders and Customers'
    ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October. For example, Fiscal Year 2026 starts in 2025.10.01 and it ends in 2026.09.30'
    with extension (
        CA='{
            "tables": [
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["4", "15", "28"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["85", "5"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["140", "150"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": [
                                "f3ce53f8-92a8-46ca-ab19-fb9760fa17f6",
                                "2e0658d7-bf7e-454f-b7c2-e70eb820b7dc",
                                "ee010b86-d8eb-493f-b269-af013e075ca3"
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
                            "sample_values": ["18.26", "20.79", "8.96"]
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
                                "2025-07-27T00:00:00.000+0000",
                                "2025-07-04T00:00:00.000+0000",
                                "2025-06-15T00:00:00.000+0000"
                            ]
                        }
                    ]
                },
                {
                    "name": "CUSTOMERS",
                    "dimensions": [
                        {
                            "name": "ADDRESS",
                            "sample_values": [
                                "8099 Rockefeller Street",
                                "21862 Almo Center",
                                "44 Ridge Oak Trail"
                            ]
                        },
                        {
                            "name": "COUNT_ORDERS",
                            "sample_values": ["9", "4", "5"]
                        },
                        {
                            "name": "COUNT_ORDERS_LAST_30_DAYS",
                            "sample_values": ["0"]
                        },
                        {
                            "name": "COUNT_ORDERS_LAST_360_DAYS",
                            "sample_values": ["4", "5", "9"]
                        },
                        {
                            "name": "COUNT_ORDERS_LAST_90_DAYS",
                            "sample_values": ["0", "1"]
                        },
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["27", "33", "67"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": [
                                "amalea.treat@gmail.com",
                                "jeno.seawell@yahoo.com",
                                "vallie.lancetter@gmail.com"
                            ]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Sienna", "Rhea", "Lindsey"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Root", "Lopez", "Treat"]
                        },
                        {
                            "name": "PHONE_NUMBER",
                            "sample_values": ["6284419982", "8728800155", "9258466320"]
                        },
                        {
                            "name": "SATISFACTION_SCORE",
                            "sample_values": ["1", "2"]
                        }
                    ],
                    "facts": [
                        {
                            "name": "AVERAGE_DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["31.666667", "53.750000", "45.000000"]
                        },
                        {
                            "name": "AVERAGE_DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["105.000000", "71.250000", "130.000000"]
                        },
                        {
                            "name": "ID",
                            "sample_values": ["69", "4", "27"]
                        }
                    ],
                    "time_dimensions": [
                        {
                            "name": "CREATED_AT",
                            "sample_values": [
                                "2025-04-01T00:00:00.000+0000",
                                "2025-02-23T00:00:00.000+0000",
                                "2025-03-11T00:00:00.000+0000"
                            ]
                        },
                        {
                            "name": "FIRST_ORDER_AT",
                            "sample_values": [
                                "2025-04-01T00:00:00.000+0000",
                                "2025-04-09T00:00:00.000+0000",
                                "2025-04-26T00:00:00.000+0000"
                            ]
                        },
                        {
                            "name": "MOST_RECENT_ORDER_AT",
                            "sample_values": [
                                "2025-11-12T00:00:00.000+0000",
                                "2025-09-26T00:00:00.000+0000",
                                "2025-12-01T00:00:00.000+0000"
                            ]
                        },
                        {
                            "name": "SURVEY_DATE",
                            "sample_values": ["2025-12-07", "2025-11-29"]
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
                    "verified_at": 1772032257,
                    "verified_by": "Semantic Model Generator"
                }
            ]
        }'
    )