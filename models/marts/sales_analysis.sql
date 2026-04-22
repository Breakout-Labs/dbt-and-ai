{{ config(materialized='semantic_view') }}

	tables (
		{{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.',
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a comprehensive order history, including order frequency across various time windows and average delivery time metrics.',
		{{ ref('deliveries') }} primary key (CUSTOMER_ID) comment='The table contains records of delivery activity summarized at the customer level. Each record includes counts of deliveries by outcome status, the most recent delivery date, and a ratio measuring delivery fulfillment success.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.',
		DELIVERIES.FULFILLMENT_RATIO as FULFILLMENT_RATIO comment='The ratio of fulfilled deliveries relative to the total number of deliveries attempted.'
	)
	dimensions (
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer associated with an order.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The estimated or actual delivery time measured in minutes from the point of collection.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes between when an order is placed and when it is delivered.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
		ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with customers.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.',
		DELIVERIES.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to a customer.',
		DELIVERIES.FAILED_DELIVERIES as FAILED_DELIVERIES comment='The number of failed deliveries.',
		DELIVERIES.OTHER_STATUS_DELIVERIES as OTHER_STATUS_DELIVERIES comment='The count of deliveries that fall under other or miscellaneous status categories.',
		DELIVERIES.SUCCESSFUL_DELIVERIES as SUCCESSFUL_DELIVERIES comment='The count of successfully completed deliveries.',
		DELIVERIES.TOTAL_DELIVERIES as TOTAL_DELIVERIES comment='The total number of deliveries.',
		DELIVERIES.LAST_DELIVERY_DATE as LAST_DELIVERY_DATE comment='The date of the most recent delivery.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('sales')
	)
	ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October.'
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1776865606
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.ORDER_ID /* order facts */, o.ORDERED_AT, o.ORDER_STATUS, o.TOTAL_AMOUNT, o.STORE_ID, o.DELIVERY_TIME_FROM_ORDER, o.DELIVERY_TIME_FROM_COLLECTION, c.CUSTOMER_ID /* customer dimension */, c.FIRST_NAME, c.LAST_NAME, c.EMAIL FROM orders AS o INNER JOIN customers AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID')
	)
	with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["4","34","15"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["5","75"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["165","150"]},{"name":"ORDER_ID","sample_values":["782ecdb9-4809-4c6f-a257-da0429f55b42","6836dbcb-c045-4555-8676-9ea6c6b9af6d","8689fd20-122c-48e5-bce7-850e9372e527"]},{"name":"ORDER_STATUS","sample_values":["delivered","ordered","pending"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["15.65","7.4","2.15"]}],"metrics":[{"name":"revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-09-30T00:00:00.000+0000","2025-12-05T00:00:00.000+0000","2025-11-01T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["13","67","4"]},{"name":"EMAIL","sample_values":["datarhea@gmail.com","amalea.treat@gmail.com","sienna.lopez@yahoo.com"]},{"name":"FIRST_NAME","sample_values":["Cyndi","Mable","Amalea"]},{"name":"LAST_NAME","sample_values":["Treat","Root","Lopez"]}]},{"name":"DELIVERIES","dimensions":[{"name":"CUSTOMER_ID","sample_values":["3","61","14"]},{"name":"FAILED_DELIVERIES","sample_values":["1","0","3"]},{"name":"OTHER_STATUS_DELIVERIES","sample_values":["0","2","1"]},{"name":"SUCCESSFUL_DELIVERIES","sample_values":["1","3","5"]},{"name":"TOTAL_DELIVERIES","sample_values":["6","3","2"]}],"facts":[{"name":"FULFILLMENT_RATIO","sample_values":["0.3333333333","0.5","0.8333333333"]}],"time_dimensions":[{"name":"LAST_DELIVERY_DATE","sample_values":["2025-11-03","2025-11-23","2025-07-06"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')