{{
    config(
        materialized='semantic_view'
    )
}}
	tables (
		{{ ref('orders') }} comment='The table contains records of customer orders, capturing key details about each transaction. Each record represents a single order and includes information about the customer, store, order status, financial totals, and delivery timing metrics.',
		{{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers, including their personal contact information and account details. Each record captures a single customer''s profile alongside aggregated order activity metrics, such as order counts across various time windows, delivery time averages, and satisfaction survey results.'
	)
	relationships (
		ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
	)
	facts (
		ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
	)
	dimensions (
		ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer associated with an order.',
		ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of minutes or hours between collection and delivery of an order.',
		ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The estimated or actual delivery time measured in minutes from when an order was placed.',
		ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
		ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
		ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
		ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.',
		CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
		CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with customers.',
		CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
		CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.'
	)
	metrics (
		ORDERS.REVENUE as sum(total_amount) with synonyms=('Sales')
	)
	ai_sql_generation 'Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
	ai_question_categorization 'Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1774537915
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id')
	)
	with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["4","3","63"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["55","20","75"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["20","35","160"]},{"name":"ORDER_ID","sample_values":["ac45c19b-bb0e-486a-a1b3-1f4d42a742be","79e80a1d-c39d-49d5-850e-b03802ed1654","304cd144-f015-4ad2-b4bf-ac8609d07620"]},{"name":"ORDER_STATUS","sample_values":["ordered","cancelled","pending"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["28.37","40.01","49.05"]}],"metrics":[{"name":"Revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-03-05T00:00:00.000+0000","2025-03-11T00:00:00.000+0000","2025-11-03T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["4","61","7"]},{"name":"EMAIL","sample_values":["william.speak@gmail.com","amalea.treat@gmail.com","sienna.lopez@yahoo.com"]},{"name":"FIRST_NAME","sample_values":["Sienna","Cyndi","Amalea"]},{"name":"LAST_NAME","sample_values":["Lopez","Sambeck","Eidler"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')