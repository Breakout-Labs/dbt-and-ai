config(materialized='semantic_view') }}
	tables (
		{{ ref('orders') }} comment='The table contains records of customer orders, capturing key details about each transaction. Each record represents a single order and includes information about the customer, store, order status, financial totals, and delivery timing metrics.',
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a comprehensive order history, including order frequency across various time windows and average delivery time metrics.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
	)
	dimensions (
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer associated with an order.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of minutes between collection and delivery for an order.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units between when an order was placed and when it was delivered.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
		ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with each customer.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.'
	)
	metrics (
		SALES as sum(orders.TOTAL_AMOUNT) comment='Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
	)
	comment='The sales team has been using your orders and customers mart models in PowerBI for a while. Now they want to query their data using AI chat tools like SiemensGPT. Some team members have already started downloading CSVs from PowerBI and uploading them to SiemensGPT - time to give them a proper solution.'
	ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October'
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1774533222
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id')
	)
	with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["63","16","30"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["45","60"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["35","150","115"]},{"name":"ORDER_ID","sample_values":["c5509f62-6fd7-413e-b9f5-24890e68f8de","f05c42ad-602a-428b-9a3c-ed16d9399e58","907f2207-77dd-425d-bbcf-933c9f5f3656"]},{"name":"ORDER_STATUS","sample_values":["ordered","delivered","pending"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["49.05","28.03","0.27"]}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-09-18T00:00:00.000+0000","2025-08-27T00:00:00.000+0000","2025-04-04T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["44","27","45"]},{"name":"EMAIL","sample_values":["crysta.duro@gmail.com","cyndi.root@gmail.com","godiva.eidler@icloud.com"]},{"name":"FIRST_NAME","sample_values":["Cyndi","Catie","Lotte"]},{"name":"LAST_NAME","sample_values":["Bromley","Root","Dahlberg"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')