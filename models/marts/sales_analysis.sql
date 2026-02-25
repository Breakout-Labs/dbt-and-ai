{{ config(materialized='semantic_view') }}

tables (
		{{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.',
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information, satisfaction metrics, and order history. Each record represents a single customer and includes personal details, survey responses, and aggregated order statistics across various time periods including delivery performance metrics.'
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
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units between when an order was placed and when it was delivered.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
		ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('sales')
	)
	comment='Sales Analysis'
	ai_sql_generation 'A fiscal year starting in October means FY2026 runs from October 2025 through September 2026'
with extension (
    CA = '{
        "tables": [
            {
                "name": "ORDERS",
                "dimensions": [
                    {
                        "name": "CUSTOMER_ID",
                        "sample_values": ["14", "25", "4"]
                    },
                    {
                        "name": "DELIVERY_TIME_FROM_COLLECTION",
                        "sample_values": ["75", "5"]
                    },
                    {
                        "name": "DELIVERY_TIME_FROM_ORDER",
                        "sample_values": ["35", "150"]
                    },
                    {
                        "name": "ORDER_ID",
                        "sample_values": [
                            "ab805b41-bb2a-4ef2-b3e9-72b4ceef185a",
                            "782ecdb9-4809-4c6f-a257-da0429f55b42",
                            "fa36ab4d-ff9e-49cc-951d-9a51975e3a81"
                        ]
                    },
                    {
                        "name": "ORDER_STATUS",
                        "sample_values": ["ordered", "delivered", "pending"]
                    },
                    {
                        "name": "STORE_ID",
                        "sample_values": ["1"]
                    }
                ],
                "facts": [
                    {
                        "name": "TOTAL_AMOUNT",
                        "sample_values": ["52.92", "27.66", "31.99"]
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
                        "sample_values": ["4", "50", "67"]
                    },
                    {
                        "name": "EMAIL",
                        "sample_values": [
                            "lenna.dahlberg@outlook.com",
                            "sienna.lopez@yahoo.com",
                            "marika.fountain@gmail.com"
                        ]
                    },
                    {
                        "name": "FIRST_NAME",
                        "sample_values": ["Josefa", "Avigdor", "Cammi"]
                    },
                    {
                        "name": "LAST_NAME",
                        "sample_values": ["Sambeck", "Lopez", "Treat"]
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
                "verified_at": 1772031157,
                "verified_by": "Semantic Model Generator"
            }
        ]
    }'
)