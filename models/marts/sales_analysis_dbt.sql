{{ config(materialized='semantic_view') }}	
    
    tables (
		{{ ref("orders") }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.',
		{{ ref("customers") }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a comprehensive order history, including order frequency across various time windows and average delivery time metrics.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
	)
	dimensions (
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer associated with an order.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time elapsed between collection and delivery of an order measured in minutes.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The estimated or actual delivery time measured in minutes from when an order was placed.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
		ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time at which an order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with each customer.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.'
	)
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1776865388
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.ORDER_ID /* order facts */, o.ORDERED_AT, o.ORDER_STATUS, o.TOTAL_AMOUNT, o.STORE_ID, o.DELIVERY_TIME_FROM_ORDER, o.DELIVERY_TIME_FROM_COLLECTION, c.CUSTOMER_ID /* customer dimension */, c.FIRST_NAME, c.LAST_NAME, c.EMAIL FROM orders AS o INNER JOIN customers AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID')
	)
	with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["27","63","15"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["75","30","5"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["35","125","130"]},{"name":"ORDER_ID","sample_values":["ab805b41-bb2a-4ef2-b3e9-72b4ceef185a","942a9748-0e37-4976-93bb-fcc8710870d9","782ecdb9-4809-4c6f-a257-da0429f55b42"]},{"name":"ORDER_STATUS","sample_values":["delivered","pending","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["2.32","47.22","6.99"]}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-06-22T00:00:00.000+0000","2025-04-04T00:00:00.000+0000","2025-05-02T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["7","4","27"]},{"name":"EMAIL","sample_values":["avigdor.bromley@proton.me","sienna.lopez@yahoo.com","amalea.treat@gmail.com"]},{"name":"FIRST_NAME","sample_values":["Rodi","Catie","Amalea"]},{"name":"LAST_NAME","sample_values":["Treat","Tourne","Lindström"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')