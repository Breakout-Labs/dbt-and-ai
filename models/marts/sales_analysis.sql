{{
    config(
        materialized='semantic_view'
    )
}}

    tables (
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures customer satisfaction survey data alongside a summary of their ordering activity, including order counts across various time windows and average delivery times.',
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
        CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with customers.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.',
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to a customer.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The estimated or actual delivery time measured from the point of collection.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes between when an order is placed and when it is delivered.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
        ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when an order was placed.'
    )
    metrics (
        ORDERS.REVENUE as sum(orders.total_amount) with synonyms=('sales') comment='Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
    )
    comment='sales analysis semantic model for sgpt'
    ai_sql_generation 'When we refer to a year, we mean our fiscal year which starts in October.'
    ai_verified_queries (
        "0;1" AS ( 
QUESTION 'What are the complete order details combined with customer information?' 
VERIFIED_AT 1774537852
VERIFIED_BY 'Semantic Model Generator'
ONBOARDING_QUESTION false
SQL 'SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id')
    )
    with extension (CA='{"tables":[{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["67","27","39"]},{"name":"EMAIL","sample_values":["cordula.tinn@proton.me","cyndi.root@gmail.com","amalea.treat@gmail.com"]},{"name":"FIRST_NAME","sample_values":["Amalea","Marika","Cello"]},{"name":"LAST_NAME","sample_values":["Cogzell","Bransgrove","Hendrix"]}]},{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["3","40","4"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["75","50"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["150","35"]},{"name":"ORDER_ID","sample_values":["782ecdb9-4809-4c6f-a257-da0429f55b42","e3927646-c5b9-4656-b220-2fc81fe329ca","a3ea69ee-566e-4883-bbee-ac10d418bf88"]},{"name":"ORDER_STATUS","sample_values":["pending","delivered","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["37.28","44.02","43.33"]}],"metrics":[{"name":"Revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-06-03T00:00:00.000+0000","2025-12-02T00:00:00.000+0000","2025-08-19T00:00:00.000+0000"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}')