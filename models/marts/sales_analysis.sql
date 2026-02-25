
{{ config(materialized='semantic_view') }}

    tables (
        {{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.',
        {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.'
    )
    relationships (
        ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
    )
    facts (
        ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total amount of the order.'
    )
    dimensions (
        ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for the customer who placed the order.',
        ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time elapsed between collection and delivery measured in a numeric unit.',
        ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units between when an order was placed and when it was delivered.',
        ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
        ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
        ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
        ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.',
        CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer in the system.',
        CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
        CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
        CUSTOMERS.LAST_NAME as LAST_NAME comment='Last names of customers.'
    )
    metrics (
        ORDERS.REVENUE as sum(total_amount) comment='Revenue is the sum of 
total_amount
. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
    )
    ai_sql_generation 'ordered_at >= ''2025-10-01'' and ordered_at < ''2026-10-01'''
    ai_question_categorization 'A fiscal year starting in October means FY2026 runs from October 2025 through September 2026'
    with extension (CA='{"tables":[{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["63","15","4"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["15","5","10"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["35","85"]},{"name":"ORDER_ID","sample_values":["0d8caabc-9792-4158-ae99-f9577adf5ef8","147f440d-cea9-4803-89d6-879286a8e679","096a4fa8-7a2d-4bb2-a5cd-8273f394e600"]},{"name":"ORDER_STATUS","sample_values":["pending","delivered","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["46.68","40.01","18.26"]}],"metrics":[{"name":"revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-09-25T00:00:00.000+0000","2025-06-02T00:00:00.000+0000","2025-10-06T00:00:00.000+0000"]}]},{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["60","36","67"]},{"name":"EMAIL","sample_values":["amalea.treat@gmail.com","sienna.lopez@yahoo.com","patrick.flewan@gmail.com"]},{"name":"FIRST_NAME","sample_values":["Sienna","Amalea","William"]},{"name":"LAST_NAME","sample_values":["Treat","Joselson","Speak"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}],"verified_queries":[{"name":"0;1","sql":"SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id","question":"What are the complete order details combined with customer information?","verified_at":1772032387,"verified_by":"Semantic Model Generator"}]}')