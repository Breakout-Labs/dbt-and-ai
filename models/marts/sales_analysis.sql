{{
    config(
        materialized='semantic_view'
    )
}}

tables (
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a comprehensive order history, including order frequency across various time windows and average delivery time metrics.',
		{{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order value, and delivery performance metrics.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
	)
	dimensions (
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with each customer.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.',
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to a customer.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of minutes between collection and delivery for an order.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes or hours elapsed between when an order is placed and when it is delivered.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
		ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Revenue is the sum of total_amount. Some team members also call it sales. '
	)
	ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October.'
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1776865251
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.ORDER_ID /* order facts */, o.ORDERED_AT, o.ORDER_STATUS, o.TOTAL_AMOUNT, o.STORE_ID, o.DELIVERY_TIME_FROM_ORDER, o.DELIVERY_TIME_FROM_COLLECTION, c.CUSTOMER_ID /* customer dimension */, c.FIRST_NAME, c.LAST_NAME, c.EMAIL FROM orders AS o INNER JOIN customers AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID')
	)
	with extension (CA='{"tables":[{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["4","27","67"]},{"name":"EMAIL","sample_values":["josefa.beevis@gmail.com","amalea.treat@gmail.com","cyndi.root@gmail.com"]},{"name":"FIRST_NAME","sample_values":["Sienna","Mace","Cyndi"]},{"name":"LAST_NAME","sample_values":["Lopez","Grogan","Root"]}]},{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["52","20","34"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["75","5"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["90","150"]},{"name":"ORDER_ID","sample_values":["1b895b86-ecee-4cdb-8148-55a0ccfb0861","fda73a7a-f6f8-40da-835b-d42a724d1591","d259cccd-07d9-4f4a-863a-6d4e6c482545"]},{"name":"ORDER_STATUS","sample_values":["pending","delivered","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["45.1","1.47","23.67"]}],"metrics":[{"name":"Revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-09-18T00:00:00.000+0000","2025-11-26T00:00:00.000+0000","2025-12-19T00:00:00.000+0000"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')