
{{
    config(
        materialized='semantic_view'
    )
}}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders, capturing key details about each transaction. Each record represents a single order and includes information about the customer, store, order timing, financial totals, and fulfillment status including delivery time metrics.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of individual customers, including their personal contact information and account details. Each record captures a customer''s satisfaction survey results alongside a comprehensive summary of their order history, including order frequency across various time windows and average delivery time metrics.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total monetary amount associated with an order.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier assigned to each customer.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The number of days between collection and delivery for an order.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of minutes between when an order is placed and when it is delivered.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier assigned to each order.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of an order.',
        ORDERS.STORE_ID as STORE_ID comment='The unique identifier for the store associated with an order.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier assigned to each customer.',
        CUSTOMERS.EMAIL as EMAIL comment='Email addresses associated with each customer.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='The last name of the customer.'
    )
    metrics (
        ORDERS.SALES as SUM(TOTAL_AMOUNT) comment='total revenue'
    )
    ai_sql_generation 'A Year will always refer to a fiscal year, which goes from October (of the previous year) to October.'
    ai_verified_queries (
        "0;1" AS ( 
            QUESTION 'What are the complete order details combined with customer information?' 
            VERIFIED_AT 1774537826
            VERIFIED_BY 'Semantic Model Generator'
            ONBOARDING_QUESTION false
            SQL 'SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id')
    )
    with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["20","63","15"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["5","75"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["20","35"]},{"name":"ORDER_ID","sample_values":["ab805b41-bb2a-4ef2-b3e9-72b4ceef185a","26d6efbc-9a7f-4e2a-8034-49385e5c7eae","2f95d7f1-1eed-4266-95c8-2f744a926326"]},{"name":"ORDER_STATUS","sample_values":["delivered","pending","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["6.37","55.46","45.51"]}],"metrics":[{"name":"sales"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-12-02T00:00:00.000+0000","2025-06-07T00:00:00.000+0000","2025-11-19T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["3","14","27"]},{"name":"EMAIL","sample_values":["cyndi.root@gmail.com","amalea.treat@gmail.com","gussie.heams@yahoo.com"]},{"name":"FIRST_NAME","sample_values":["Sienna","Veronique","Oskar"]},{"name":"LAST_NAME","sample_values":["Treat","Root","Lopez"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}]}');