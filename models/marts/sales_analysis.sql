{{ config(materialized='semantic_view') }}

tables (
    {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their contact information and account details. Each record captures customer satisfaction survey data alongside a summary of their ordering activity, including order counts across various time windows and average delivery times.',
    {{ ref('orders') }} comment='The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.'
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
    ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of minutes or hours between collection and delivery of an order.',
    ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes or hours between when an order is placed and when it is delivered.',
    ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
    ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
    ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
    ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.'
)
ai_verified_queries (
    "0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1776863163
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.ORDER_ID /* order facts */, o.ORDERED_AT, o.ORDER_STATUS, o.TOTAL_AMOUNT, o.STORE_ID, o.DELIVERY_TIME_FROM_ORDER, o.DELIVERY_TIME_FROM_COLLECTION, c.CUSTOMER_ID /* customer dimension */, c.FIRST_NAME, c.LAST_NAME, c.EMAIL FROM orders AS o INNER JOIN customers AS c ON o.CUSTOMER_ID = c.CUSTOMER_ID')
)
with extension (CA='{"tables":[{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["4","67","49"]},{"name":"EMAIL","sample_values":["cyndi.root@gmail.com","lissie.aspinall@gmail.com","concettina.croney@yahoo.com"]},{"name":"FIRST_NAME","sample_values":["Sienna","Oskar","Dalia"]},{"name":"LAST_NAME","sample_values":["Root","Treat","Digginson"]}]},{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["14","63","15"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["5","75"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["70","10"]},{"name":"ORDER_ID","sample_values":["fa36ab4d-ff9e-49cc-951d-9a51975e3a81","839b844e-d768-4d6a-8890-ee3884a88900","61458ebb-0552-4fb9-b027-24235f435f33"]},{"name":"ORDER_STATUS","sample_values":["pending","delivered","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["1.17","45.51","40.01"]}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-07-18T00:00:00.000+0000","2025-09-18T00:00:00.000+0000","2025-06-22T00:00:00.000+0000"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')