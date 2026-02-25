{{ config(materialized='semantic_view') }}

	tables (
		{{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.',
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.'
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
		ORDERS.ORDERED_AT as ORDERED_AT comment='The timestamp when the order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
	)
	comment='Semantic view combining orders and customers for AI-powered sales analysis with revenue metrics and fiscal year context'
	ai_sql_generation 'FY2026 = October 1, 2025 to September 30, 2026'
with extension (CA='{
  "tables": [
    {
      "name": "ORDERS",
      "dimensions": [
        {"name": "CUSTOMER_ID", "sample_values": ["67", "63", "15"]},
        {"name": "DELIVERY_TIME_FROM_COLLECTION", "sample_values": ["5", "75"]},
        {"name": "DELIVERY_TIME_FROM_ORDER", "sample_values": ["150", "35"]},
        {"name": "ORDER_ID", "sample_values": [
          "ab805b41-bb2a-4ef2-b3e9-72b4ceef185a",
          "0d310c5a-5a56-48c1-bee9-c8451a113c4b",
          "2f95d7f1-1eed-4266-95c8-2f744a926326"
        ]},
        {"name": "ORDER_STATUS", "sample_values": ["ordered", "delivered", "pending"]},
        {"name": "STORE_ID", "sample_values": ["1"]}
      ],
      "facts": [
        {"name": "TOTAL_AMOUNT", "sample_values": ["45.18", "45.51", "43.57"]}
      ],
      "metrics": [
        {"name": "Revenue"}
      ],
      "time_dimensions": [
        {"name": "ORDERED_AT", "sample_values": [
          "2025-06-03T00:00:00.000+0000",
          "2025-08-30T00:00:00.000+0000",
          "2025-07-02T00:00:00.000+0000"
        ]}
      ]
    },
    {
      "name": "CUSTOMERS",
      "dimensions": [
        {"name": "CUSTOMER_ID", "sample_values": ["44", "13", "27"]},
        {"name": "EMAIL", "sample_values": [
          "avigdor.bromley@proton.me",
          "cyndi.root@gmail.com",
          "amalea.treat@gmail.com"
        ]},
        {"name": "FIRST_NAME", "sample_values": ["Amalea", "Granthem", "Sienna"]},
        {"name": "LAST_NAME", "sample_values": ["Lopez", "Root", "Treat"]}
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
      "sql": "SELECT o.order_id, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id",
      "question": "What are the complete order details combined with customer information?",
      "verified_at": 1772032034,
      "verified_by": "Semantic Model Generator"
    }
  ]
}')