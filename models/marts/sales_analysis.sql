{{
    config(
        materialized='semantic_view'
    )
}}

	tables (
		{{ ref('customers') }} primary key (customer_id) comment='The table contains records of individual customers, including their personal contact information and order history. Each record captures a single customer''s activity metrics such as order frequency across various time windows, delivery time averages, and satisfaction survey results.',
		{{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.'
	)
	relationships (
		orders_to_customers as orders(customer_id) references customers(customer_id)
	)
	facts (
		orders.total_amount as total_amount comment='The total monetary amount associated with an order.'
	)
	dimensions (
		customers.customer_id as customer_id comment='Unique identifier assigned to each customer.',
		customers.email as email comment='Email addresses associated with each customer.',
		customers.first_name as first_name comment='The first name of the customer.',
		customers.last_name as last_name comment='The last name of the customer.',
		orders.customer_id as customer_id comment='The unique identifier assigned to a customer.',
		orders.delivery_time_from_collection as delivery_time_from_collection comment='The time elapsed between collection and delivery of an order measured in minutes.',
		orders.delivery_time_from_order as delivery_time_from_order comment='The estimated or actual delivery time measured in minutes from when an order was placed.',
		orders.order_id as order_id comment='Unique identifier assigned to each order.',
		orders.order_status as order_status comment='The current status of an order.',
		orders.store_id as store_id comment='The unique identifier for the store associated with an order.',
		orders.ordered_at as ordered_at comment='The date and time when the order was placed.'
	)
	metrics (
		revenue as sum(orders.total_amount) with synonyms=('sales')
	)
	ai_sql_generation 'A year starting in October means 2026 runs from October 2025 through September 2026'
	ai_question_categorization 'year '
	ai_verified_queries (
		"0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1776866713
VERIFIED_BY 'JOAO_O_COELHO'
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
	with extension (CA='{"tables":[{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["41","74","17"]},{"name":"EMAIL","sample_values":["crysta.duro@gmail.com","amalea.treat@gmail.com","flss.welton@outlook.com"]},{"name":"FIRST_NAME","sample_values":["Mercedes","Mace","Cyndi"]},{"name":"LAST_NAME","sample_values":["Root","Pryell","Lopez"]}]},{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["4","40","26"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["5","75"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["150","35"]},{"name":"ORDER_ID","sample_values":["782ecdb9-4809-4c6f-a257-da0429f55b42","bd9a2102-4a56-4470-8c0c-f9f362c10539","b516f32e-8bfa-4a3b-856f-a8a70fbac734"]},{"name":"ORDER_STATUS","sample_values":["ordered","pending","delivered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["7.4","56.58","7.94"]}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-04-04T00:00:00.000+0000","2025-07-20T00:00:00.000+0000","2025-09-18T00:00:00.000+0000"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')