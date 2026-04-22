{{
    config(
        materialized='semantic_view'
    )
}}

tables (
    {{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.',
    {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a summary of their ordering activity, including order counts across various time windows and average delivery times.'
)
relationships (
    ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
)
facts (
    ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
)
dimensions (
    ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer.',
    ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of minutes between collection and delivery for an order.',
    ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes between when an order is placed and when it is delivered.',
    ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
    ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
    ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
    ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.',
    CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
    CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with each customer.',
    CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
    CUSTOMERS.LAST_NAME as LAST_NAME comment='Last names of customers.'
)
metrics (
    ORDERS.REVENUE as sum(total_amount) with synonyms=('sales')
)
ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October.'
ai_verified_queries (
    "0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1776864205
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.ORDER_ID /* order facts */, o.ORDERED_AT, o.ORDER_STATUS, o.TOTAL_AMOUNT, o.STORE_ID, o.DELIVERY_TIME_FROM_ORDER, o.DELIVERY_TIME_FROM_COLLECTION, c.CUSTOMER_ID /* customer dimension */, c.FIRST_NAME, c.LAST_NAME, c.EMAIL FROM orders AS o INNER JOIN customers AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID')
)
with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["63","4","15"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["75","5"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["150","35","130"]},{"name":"ORDER_ID","sample_values":["06a893c1-8caf-45cc-bff1-edbb84e9580d","5126b790-c5fc-41c6-89a2-ae247dac1ef5","ab805b41-bb2a-4ef2-b3e9-72b4ceef185a"]},{"name":"ORDER_STATUS","sample_values":["ordered","delivered","pending"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["57.67","2.49","44.02"]}],"metrics":[{"name":"Revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-04-06T00:00:00.000+0000","2025-07-22T00:00:00.000+0000","2025-06-01T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["50","67","4"]},{"name":"EMAIL","sample_values":["amalea.treat@gmail.com","cyndi.root@gmail.com","sheff.ceyssen@outlook.com"]},{"name":"FIRST_NAME","sample_values":["Crysta","Amalea","Cyndi"]},{"name":"LAST_NAME","sample_values":["Treat","Lopez","Sheekey"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')