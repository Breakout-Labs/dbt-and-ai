{{
    config(
        materialized='semantic_view'
    )
}}
	tables (
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers with their contact information and order history metrics. Each record includes personal details, satisfaction measurements, and aggregated statistics about ordering behavior across various time periods including delivery performance and order frequency.',
		{{ ref('orders') }} comment='The table contains records of customer orders placed at various store locations. Each record represents a single order and includes details about the customer, order timing and status, financial totals, and delivery timeframes.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount of the order.'
	)
	dimensions (
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.',
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for the customer who placed the order.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time duration between collection and delivery measured in a numeric unit.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units elapsed between order placement and delivery.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
		ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('sales')
	)
	comment='This semantic view connects the customers and orders tables to help analyze business performance.'
	ai_sql_generation 'When we refer to a year we mean our fiscal year which starts in October. For example, Fiscal Year 2026 starts in 2025.10.01 and it ends in 2026.09.30.'
    with extension (
        CA='{
            "tables": [
                {
                    "name": "CUSTOMERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["62", "4", "27"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": ["enos.lodewick@outlook.com", "sienna.lopez@yahoo.com", "iggy.scuse@outlook.com"]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Sienna", "Julian", "Cyndi"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Reyes", "Croney", "Lopez"]
                        }
                    ]
                },
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["63", "14", "57"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["5", "55"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["35", "145", "150"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": ["5d42fa87-6860-48e0-8099-e9f8b8ca8395", "acb8a451-15a0-4a4d-8415-448f322a0184", "0dcf16ae-de80-49dd-8463-7c5e94802619"]
                        },
                        {
                            "name": "ORDER_STATUS",
                            "sample_values": ["pending", "ordered", "delivered"]
                        },
                        {
                            "name": "STORE_ID",
                            "sample_values": ["1"]
                        }
                    ],
                    "facts": [
                        {
                            "name": "TOTAL_AMOUNT",
                            "sample_values": ["45.51", "40.01", "58.05"]
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
                            "sample_values": ["2025-04-04T00:00:00.000+0000", "2025-06-03T00:00:00.000+0000", "2025-04-13T00:00:00.000+0000"]
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
                    "verified_at": 1772030456,
                    "verified_by": "Semantic Model Generator"
                }
            ]
        }'
    )