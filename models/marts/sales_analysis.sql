{{
    config(
        materialized='semantic_view'
    )
}}

tables (
        {{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.',
        {{ ref('deliveries') }} comment='The table contains records of delivery performance metrics aggregated by customer. Each record includes counts of deliveries by outcome status, timing information for the most recent delivery, and an overall fulfillment success rate.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount of the order.',
        DELIVERIES.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier for the customer receiving the delivery.',
        DELIVERIES.FAILED_DELIVERIES as FAILED_DELIVERIES comment='The number of deliveries that failed.',
        DELIVERIES.FULFILLMENT_RATIO as FULFILLMENT_RATIO comment='The percentage of an order that was successfully fulfilled and delivered.',
        DELIVERIES.OTHER_STATUS_DELIVERIES as OTHER_STATUS_DELIVERIES comment='The number of deliveries with a status classified as other or miscellaneous.',
        DELIVERIES.SUCCESSFUL_DELIVERIES as SUCCESSFUL_DELIVERIES comment='The number of deliveries that were successfully completed.',
        DELIVERIES.TOTAL_DELIVERIES as TOTAL_DELIVERIES comment='The total number of deliveries.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for the customer who placed the order.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time elapsed between collection and delivery measured in a numeric unit.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units elapsed between order placement and delivery.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
        ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer in the system.',
        CUSTOMERS.EMAIL as EMAIL comment='Email addresses for customers.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.',
        DELIVERIES.LAST_DELIVERY_AT as LAST_DELIVERY_AT comment='The timestamp of the most recent delivery.'
    )
    metrics (
        ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Revenue amount'
    )
    comment='Logistics Analysis'
    ai_question_categorization 'When we refer to a year, we mean our fiscal year which starts in October'
    with extension (CA='{
    "tables": [
        {
            "name": "ORDERS",
            "dimensions": [
                {
                    "name": "CUSTOMER_ID",
                    "sample_values": ["49", "63", "4"]
                },
                {
                    "name": "DELIVERY_TIME_FROM_COLLECTION",
                    "sample_values": ["75", "5"]
                },
                {
                    "name": "DELIVERY_TIME_FROM_ORDER",
                    "sample_values": ["150", "95"]
                },
                {
                    "name": "ORDER_ID",
                    "sample_values": ["2f95d7f1-1eed-4266-95c8-2f744a926326", "fc6b9d61-62d3-41c9-afad-2d83217be5e2", "0ee7a82e-5f11-4ec3-9208-f1887a41d6ca"]
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
                    "sample_values": ["34.17", "1.62", "3.48"]
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
                    "sample_values": ["2025-06-05T00:00:00.000+0000", "2025-05-08T00:00:00.000+0000", "2025-09-26T00:00:00.000+0000"]
                }
            ]
        },
        {
            "name": "CUSTOMERS",
            "dimensions": [
                {
                    "name": "CUSTOMER_ID",
                    "sample_values": ["45", "27", "67"]
                },
                {
                    "name": "EMAIL",
                    "sample_values": ["amalea.treat@gmail.com", "avigdor.bromley@proton.me", "cyndi.root@gmail.com"]
                },
                {
                    "name": "FIRST_NAME",
                    "sample_values": ["Sienna", "Concettina", "Amalea"]
                },
                {
                    "name": "LAST_NAME",
                    "sample_values": ["Root", "Sarsfield", "Parcells"]
                }
            ]
        },
        {
            "name": "DELIVERIES",
            "facts": [
                {
                    "name": "CUSTOMER_ID",
                    "sample_values": ["62", "8", "57"]
                },
                {
                    "name": "FAILED_DELIVERIES",
                    "sample_values": ["1", "0", "3"]
                },
                {
                    "name": "FULFILLMENT_RATIO",
                    "sample_values": ["50", "40", "100"]
                },
                {
                    "name": "OTHER_STATUS_DELIVERIES",
                    "sample_values": ["0", "1", "2"]
                },
                {
                    "name": "SUCCESSFUL_DELIVERIES",
                    "sample_values": ["5", "2", "1"]
                },
                {
                    "name": "TOTAL_DELIVERIES",
                    "sample_values": ["5", "2", "1"]
                }
            ],
            "time_dimensions": [
                {
                    "name": "LAST_DELIVERY_AT",
                    "sample_values": ["2025-09-16T01:55:00.000+0000", "2025-03-26T02:25:00.000+0000", "2025-12-04T02:50:00.000+0000"]
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
            "verified_at": 1772026095,
            "verified_by": "Semantic Model Generator"
        }
    ]
}');