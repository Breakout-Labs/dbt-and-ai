
{{ config(
    materialized='semantic_view'
) }}
	tables (
		{{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.',
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a comprehensive order history, including order frequency across various time windows and average delivery time metrics.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
	)
	dimensions (
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to a customer.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time elapsed between collection and delivery of an order measured in minutes.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes between when an order is placed and when it is delivered.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
		ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with each customer.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('sales')
	)
	ai_sql_generation 'the fiscal year starts with october.'
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1774538689
VERIFIED_BY 'CHRISTIAN_WENZEL'
ONBOARDING_QUESTION false
SQL 'SELECT
  o.order_id
  /* order facts */,
  o.ordered_at,
  o.order_status,
  o.total_amount,
  o.store_id,
  o.delivery_time_from_order,
  o.delivery_time_from_collection,
  c.customer_id
  /* customer dimension */,
  c.first_name,
  c.last_name,
  c.email
FROM
  {{ ref('orders') }} AS o
  INNER JOIN {{ ref('customers') }} AS c ON o.customer_id = c.customer_id')
	)
    with extension (
        CA='{
            "tables": [
                {
                    "name": "ORDERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["4", "44", "63"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_COLLECTION",
                            "sample_values": ["5", "95"]
                        },
                        {
                            "name": "DELIVERY_TIME_FROM_ORDER",
                            "sample_values": ["35", "25", "150"]
                        },
                        {
                            "name": "ORDER_ID",
                            "sample_values": [
                                "782ecdb9-4809-4c6f-a257-da0429f55b42",
                                "bb443e75-6999-4062-b92c-0c66dc970f8d",
                                "f8ce19ab-ff92-443a-a62f-3c59d988d521"
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
                            "sample_values": ["10.41", "48.85", "18.26"]
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
                                "2025-10-03T00:00:00.000+0000",
                                "2025-07-21T00:00:00.000+0000",
                                "2025-03-26T00:00:00.000+0000"
                            ]
                        }
                    ]
                },
                {
                    "name": "CUSTOMERS",
                    "dimensions": [
                        {
                            "name": "CUSTOMER_ID",
                            "sample_values": ["4", "73", "2"]
                        },
                        {
                            "name": "EMAIL",
                            "sample_values": [
                                "mercedes.duckers@yahoo.com",
                                "cyndi.root@gmail.com",
                                "egan.peete@yahoo.com"
                            ]
                        },
                        {
                            "name": "FIRST_NAME",
                            "sample_values": ["Laurens", "Cyndi", "Sienna"]
                        },
                        {
                            "name": "LAST_NAME",
                            "sample_values": ["Treat", "Eidler", "Root"]
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
