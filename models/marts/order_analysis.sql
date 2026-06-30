{{
    config(
        materialized='snowflake_semantic_view'
    )
}}

{#- The metric description is preserved verbatim; wrapped in yaml_long_text() per the brief. -#}
{%- set average_order_value_description -%}
Average value of an order (total_amount), measured over delivered orders only - excludes orders still pending or ordered that may never be fulfilled. Business users may call this "basket size" or "AOV".
{%- endset -%}

tables:
  - name: CUSTOMERS
    synonyms: []
    description: The table contains records of individual customers, including their personal contact information and order history. Each record captures customer satisfaction metrics alongside aggregated ordering behavior, such as order counts across various time windows and average delivery times.
    base_table: {{ sv_ref('customers') }}
    dimensions:
      - name: ADDRESS
        description: Street addresses of customers.
        expr: ADDRESS
        data_type: VARCHAR
        sample_values:
          - 89 Mission Bay Blvd
          - 8099 Rockefeller Street
          - 21862 Almo Center
      - name: COUNT_ORDERS
        description: The total number of orders associated with a customer.
        expr: COUNT_ORDERS
        data_type: NUMBER
        sample_values:
          - '4'
          - '9'
          - '5'
      - name: COUNT_ORDERS_LAST_30_DAYS
        description: The number of orders placed by a customer in the last 30 days.
        expr: COUNT_ORDERS_LAST_30_DAYS
        data_type: NUMBER
        sample_values:
          - '0'
      - name: COUNT_ORDERS_LAST_360_DAYS
        description: The number of orders placed by a customer in the last 360 days.
        expr: COUNT_ORDERS_LAST_360_DAYS
        data_type: NUMBER
        sample_values:
          - '5'
          - '1'
          - '2'
      - name: COUNT_ORDERS_LAST_90_DAYS
        description: The total number of orders placed by a customer in the last 90 days.
        expr: COUNT_ORDERS_LAST_90_DAYS
        data_type: NUMBER
        sample_values:
          - '0'
      - name: CUSTOMER_ID
        description: Unique identifier assigned to each customer.
        expr: CUSTOMER_ID
        data_type: FLOAT
        sample_values:
          - '75'
          - '4'
          - '67'
      - name: EMAIL
        description: Email addresses associated with each customer.
        expr: EMAIL
        data_type: VARCHAR
        sample_values:
          - erl.digginson@gmail.com
          - mace.lowell@outlook.com
          - camila.reyes92@yahoo.com
      - name: FIRST_NAME
        description: The first name of the customer.
        expr: FIRST_NAME
        data_type: VARCHAR
        sample_values:
          - Bambie
          - Cyndi
          - Priya
      - name: LAST_NAME
        description: The last name of the customer.
        expr: LAST_NAME
        data_type: VARCHAR
        sample_values:
          - Root
          - Welton
          - Collins
      - name: PHONE_NUMBER
        description: The phone number associated with a customer.
        expr: PHONE_NUMBER
        data_type: NUMBER
        sample_values:
          - '6034921471'
          - '3873972014'
          - '8728800155'
      - name: SATISFACTION_SCORE
        description: A numeric score representing customer satisfaction.
        expr: SATISFACTION_SCORE
        data_type: NUMBER
        sample_values:
          - '2'
          - '1'
    time_dimensions:
      - name: CREATED_AT
        description: The timestamp indicating when the customer record was created.
        expr: CREATED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-04-14T00:00:00.000Z'
          - '2025-03-13T00:00:00.000Z'
          - '2025-02-19T00:00:00.000Z'
      - name: FIRST_ORDER_AT
        description: The timestamp of when a customer placed their first order.
        expr: FIRST_ORDER_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-03-17T00:00:00.000Z'
          - '2025-04-09T00:00:00.000Z'
          - '2025-04-01T00:00:00.000Z'
      - name: MOST_RECENT_ORDER_AT
        description: The timestamp of the most recent order placed by the customer.
        expr: MOST_RECENT_ORDER_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-12-13T00:00:00.000Z'
          - '2025-10-26T00:00:00.000Z'
          - '2025-07-31T00:00:00.000Z'
      - name: SURVEY_DATE
        description: The date on which a customer survey was conducted.
        expr: SURVEY_DATE
        data_type: DATE
        sample_values:
          - '2025-12-07'
          - '2025-11-29'
    facts:
      - name: AVERAGE_DELIVERY_TIME_FROM_COLLECTION
        description: The average time elapsed between collection and delivery for a customer.
        expr: AVERAGE_DELIVERY_TIME_FROM_COLLECTION
        data_type: NUMBER
        sample_values:
          - '40.000000'
          - '60.000000'
          - '53.750000'
      - name: AVERAGE_DELIVERY_TIME_FROM_ORDER
        description: The average amount of time from order placement to delivery for a customer.
        expr: AVERAGE_DELIVERY_TIME_FROM_ORDER
        data_type: NUMBER
        sample_values:
          - '125.000000'
          - '105.000000'
          - '92.500000'
      - name: ID
        description: Unique identifier for each customer.
        expr: ID
        data_type: FLOAT
        sample_values:
          - '4'
          - '62'
          - '73'
    primary_key:
      columns:
        - CUSTOMER_ID
  - name: ORDERS
    synonyms: []
    description: The table contains records of customer orders, including details about timing, status, and financials. Each record represents a single order and captures information about the associated customer and store, order status, total amount, and delivery timing metrics.
    base_table: {{ sv_ref('orders') }}
    dimensions:
      - name: CUSTOMER_ID
        description: The unique identifier assigned to a customer.
        expr: CUSTOMER_ID
        data_type: NUMBER
        sample_values:
          - '69'
          - '15'
          - '27'
      - name: DELIVERY_TIME_FROM_COLLECTION
        description: The number of minutes or hours between collection and delivery of an order.
        expr: DELIVERY_TIME_FROM_COLLECTION
        data_type: NUMBER
        sample_values:
          - '10'
          - '5'
          - '75'
      - name: DELIVERY_TIME_FROM_ORDER
        description: The number of minutes between when an order is placed and when it is delivered.
        expr: DELIVERY_TIME_FROM_ORDER
        data_type: NUMBER
        sample_values:
          - '150'
          - '35'
      - name: ORDER_ID
        description: Unique identifier assigned to each order.
        expr: ORDER_ID
        data_type: VARCHAR
        sample_values:
          - 43bb0804-fe3b-4005-8afa-ebabb01a6072
          - c4318d2c-d027-4147-a29b-2370d5e36b0d
          - f2fc2c11-6391-4409-87b2-0262b6d21f48
      - name: ORDER_STATUS
        description: The current status of an order.
        expr: ORDER_STATUS
        data_type: VARCHAR
        sample_values:
          - delivered
          - ordered
          - pending
      - name: STORE_ID
        description: The unique identifier for the store associated with an order.
        expr: STORE_ID
        data_type: NUMBER
        sample_values:
          - '1'
    time_dimensions:
      - name: ORDERED_AT
        description: The date and time when an order was placed.
        expr: ORDERED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-12-30T00:00:00.000Z'
          - '2025-12-10T00:00:00.000Z'
          - '2025-04-15T00:00:00.000Z'
    facts:
      - name: TOTAL_AMOUNT
        description: The total monetary amount associated with an order.
        expr: TOTAL_AMOUNT
        data_type: FLOAT
        sample_values:
          - '30'
          - '51.77'
          - '0.5'
    metrics:
      - name: average_order_value
        description: {{ yaml_long_text(average_order_value_description) }}
        expr: avg(total_amount)
    primary_key:
      columns:
        - ORDER_ID
relationships:
  - name: ORDERS_TO_CUSTOMERS
    left_table: ORDERS
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    right_table: CUSTOMERS
