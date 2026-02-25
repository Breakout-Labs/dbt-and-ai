{{
    config(
        materialized='semantic_view'
    )
}}

tables (
    {{ ref('customers') }} primary key (CUSTOMER_ID) comment='The table contains records of customers and their associated contact information and order history metrics. Each record represents a single customer and includes personal details, satisfaction feedback, and aggregated order statistics across various time periods including delivery performance measures.',
    {{ ref('orders') }} comment='The table contains records of customer orders placed at stores. Each record represents a single order and includes details about the customer, timing information, order status, and financial amounts, as well as delivery timeframes.'
)
relationships (
    ORDERS_TO_CUSTOMERS as ORDERS(CUSTOMER_ID) references CUSTOMERS(CUSTOMER_ID)
)
facts (
    ORDERS.TOTAL_AMOUNT as TOTAL_AMOUNT comment='The total amount of the order.'
)
dimensions (
    CUSTOMERS.CUSTOMER_ID as CUSTOMER_ID comment='Unique identifier for each customer.',
    CUSTOMERS.EMAIL as EMAIL comment='Customer email addresses.',
    CUSTOMERS.FIRST_NAME as FIRST_NAME comment='The first name of the customer.',
    CUSTOMERS.LAST_NAME as LAST_NAME comment='Customer last names.',
    ORDERS.CUSTOMER_ID as CUSTOMER_ID comment='The unique identifier for the customer who placed the order.',
    ORDERS.DELIVERY_TIME_FROM_COLLECTION as DELIVERY_TIME_FROM_COLLECTION comment='The time elapsed between collection and delivery measured in a numeric unit.',
    ORDERS.DELIVERY_TIME_FROM_ORDER as DELIVERY_TIME_FROM_ORDER comment='The number of time units elapsed between order placement and delivery.',
    ORDERS.ORDER_ID as ORDER_ID comment='Unique identifier for each order in the system.',
    ORDERS.ORDER_STATUS as ORDER_STATUS comment='The current status of the order in its fulfillment lifecycle.',
    ORDERS.STORE_ID as STORE_ID comment='The identifier for the store where the order was placed.',
    ORDERS.ORDERED_AT as ORDERED_AT comment='The date and time when the order was placed.'
)
metrics (
    ORDERS.REVENUE as sum(total_amount) with synonyms=('sales') comment='Sum of total sales'
)
comment='A semantic view for sales'
ai_sql_generation 'Revenue is the sum of total_amount. Some team members also call it sales. When we refer to a year, we mean our fiscal year which starts in October.'
with extension (CA='{"tables":[{"name":"CUSTOMERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["67","27","4"]},{"name":"EMAIL","sample_values":["sienna.lopez@yahoo.com","meara.drinkale@gmail.com","laurens.grogan@gmail.com"]},{"name":"FIRST_NAME","sample_values":["Sienna","Amalea","Austin"]},{"name":"LAST_NAME","sample_values":["Root","Lopez","Treat"]}]},{"name":"ORDERS","dimensions":[{"name":"CUSTOMER_ID","sample_values":["63","4","16"]},{"name":"DELIVERY_TIME_FROM_COLLECTION","sample_values":["5","100"]},{"name":"DELIVERY_TIME_FROM_ORDER","sample_values":["105","35","150"]},{"name":"ORDER_ID","sample_values":["ab805b41-bb2a-4ef2-b3e9-72b4ceef185a","757de210-4530-48db-987d-b3fb6b34d93d","0ee7a82e-5f11-4ec3-9208-f1887a41d6ca"]},{"name":"ORDER_STATUS","sample_values":["pending","delivered","ordered"]},{"name":"STORE_ID","sample_values":["1"]}],"facts":[{"name":"TOTAL_AMOUNT","sample_values":["47.13","37.1","18.26"]}],"metrics":[{"name":"revenue"}],"time_dimensions":[{"name":"ORDERED_AT","sample_values":["2025-04-04T00:00:00.000+0000","2025-09-18T00:00:00.000+0000","2025-05-14T00:00:00.000+0000"]}]}],"relationships":[{"name":"ORDERS_TO_CUSTOMERS","relationship_type":"many_to_one","join_type":"inner"}],"verified_queries":[{"name":"\\"0;1\\"","sql":"SELECT o.order_id /* order facts */, o.ordered_at, o.order_status, o.total_amount, o.store_id, o.delivery_time_from_order, o.delivery_time_from_collection, c.customer_id /* customer dimension */, c.first_name, c.last_name, c.email FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.customer_id","question":"What are the complete order details combined with customer information?","verified_at":1771616819,"verified_by":"Semantic Model Generator"}]}')