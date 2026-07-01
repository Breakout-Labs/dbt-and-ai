{{
    config(
        materialized='snowflake_semantic_view'
    )
}}

tables:
  - name: STG_ECOMM__ORDERS
    description: The table contains records of e-commerce orders. Each record represents a single order and includes details about the customer, store, order timing, financial totals, and fulfillment status.
    base_table: {{ sv_ref('stg_ecomm__orders') }}
    dimensions:
      - name: CUSTOMER_ID
        description: Unique identifier for the customer associated with the order.
        expr: CUSTOMER_ID
        data_type: NUMBER
        sample_values:
          - '15'
          - '63'
          - '4'
      - name: ORDER_ID
        description: Unique identifier for each order.
        expr: ORDER_ID
        data_type: VARCHAR
        sample_values:
          - ab805b41-bb2a-4ef2-b3e9-72b4ceef185a
          - 7666a165-4389-4b9e-ac1e-ebd46371dd80
          - 782ecdb9-4809-4c6f-a257-da0429f55b42
      - name: ORDER_STATUS
        description: The current fulfillment status of an e-commerce order (e.g., pending, shipped, delivered, cancelled).
        expr: ORDER_STATUS
        data_type: VARCHAR
        sample_values:
          - ordered
          - delivered
          - pending
      - name: STORE_ID
        description: The unique identifier for the store associated with the order.
        expr: STORE_ID
        data_type: NUMBER
        sample_values:
          - '1'
    time_dimensions:
      - name: _SYNCED_AT
        description: The timestamp indicating when the record was last synced from the source system.
        expr: _SYNCED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-09-19T00:00:00.000Z'
          - '2025-04-07T00:00:00.000Z'
          - '2025-09-29T00:00:00.000Z'
      - name: ORDERED_AT
        description: The date and time when the order was placed, stored without timezone information.
        expr: ORDERED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-04-04T00:00:00.000Z'
          - '2025-11-12T00:00:00.000Z'
          - '2025-06-27T00:00:00.000Z'
    facts:
      - name: TOTAL_AMOUNT
        description: The total monetary amount for the order.
        expr: TOTAL_AMOUNT
        data_type: FLOAT
        sample_values:
          - '18.26'
          - '23.39'
          - '40.01'
    metrics:
      - name: number_of_orders
        expr: count(order_id)
    primary_key:
      columns:
        - ORDER_ID
  - name: STG_ECOMM__ORDER_LINES
    description: The table contains records of individual line items within e-commerce orders. Each record represents a single product entry within an order, including details about the product, quantity, pricing, and data sync timing.
    base_table: {{ sv_ref('stg_ecomm__order_lines') }}
    dimensions:
      - name: LINE_NUMBER
        description: The sequential line number identifying each individual item within an order.
        expr: LINE_NUMBER
        data_type: NUMBER
        sample_values:
          - '1'
          - '2'
          - '3'
      - name: ORDER_ID
        description: The unique identifier for an order associated with a line item.
        expr: ORDER_ID
        data_type: VARCHAR
        sample_values:
          - cde2d423-5b16-4a87-bcf8-d05bd40f758f
          - 6836dbcb-c045-4555-8676-9ea6c6b9af6d
          - 699c43f7-e031-4630-8ca7-a962ae3119bb
      - name: ORDER_LINE_ID
        description: Unique identifier for an individual line item within an order.
        expr: ORDER_LINE_ID
        data_type: VARCHAR
        sample_values:
          - a346e8d8-ebd5-4519-ae23-6d4a13a05bde
          - 2b96fe25-d0a0-425d-b190-38403ea90159
          - 32249deb-0afc-41b5-9a7c-2fb43bd31977
      - name: PRODUCT_ID
        description: The unique identifier for a product associated with an order line.
        expr: PRODUCT_ID
        data_type: VARCHAR
        sample_values:
          - P012
          - P031
          - P026
      - name: QUANTITY
        description: The number of units ordered for a given order line.
        expr: QUANTITY
        data_type: NUMBER
        sample_values:
          - '2'
          - '1'
          - '3'
    time_dimensions:
      - name: _SYNCED_AT
        description: The timestamp indicating when the record was last synchronized, stored without timezone information.
        expr: _SYNCED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2025-03-10T00:00:00.000Z'
          - '2025-12-06T00:00:00.000Z'
          - '2025-09-10T00:00:00.000Z'
    facts:
      - name: UNIT_PRICE
        description: The price of a single unit of the ordered item.
        expr: UNIT_PRICE
        data_type: NUMBER
        sample_values:
          - '24.99'
          - '19.99'
          - '65.00'
    metrics:
      - name: total_revenue
        expr: sum(unit_price * quantity)
    unique_keys:
      - columns:
          - PRODUCT_ID
  - name: STG_ECOMM__PRODUCTS
    description: The table contains records of e-commerce products available in a catalog. Each record represents a single product and includes details about its categorization, pricing, active status, and available variants.
    base_table: {{ sv_ref('stg_ecomm__products') }}
    dimensions:
      - name: IS_ACTIVE
        description: Indicates whether the product is currently active.
        expr: IS_ACTIVE
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
      - name: PRODUCT_CATEGORY
        description: The category classification of the product.
        expr: PRODUCT_CATEGORY
        data_type: VARCHAR
        sample_values:
          - Sports & Outdoors
          - Toys & Games
          - Electronics
      - name: PRODUCT_ID
        description: Unique identifier assigned to each product.
        expr: PRODUCT_ID
        data_type: VARCHAR
        sample_values:
          - P035
          - P003
          - P001
      - name: PRODUCT_NAME
        description: The name of the product.
        expr: PRODUCT_NAME
        data_type: VARCHAR
        sample_values:
          - ArtNest Supplies Kit
          - SkyStand Laptop Riser
          - MindQuest Board Game
      - name: PRODUCT_SUBCATEGORY
        description: The subcategory classification of a product within its broader product category.
        expr: PRODUCT_SUBCATEGORY
        data_type: VARCHAR
        sample_values:
          - Small Appliances
          - Computer Accessories
          - Stationery
      - name: VARIANTS
        description: A column holding data of type VARIANT.
        expr: VARIANTS
        data_type: VARIANT
        sample_values:
          - |-
            [
              {
                "color": "black",
                "title": "Clickaroo Wireless Mouse - Black",
                "variant_id": "P001-BLACK",
                "wireless": true
              },
              {
                "color": "white",
                "title": "Clickaroo Wireless Mouse - White",
                "variant_id": "P001-WHITE",
                "wireless": true
              },
              {
                "color": "gray",
                "title": "Clickaroo Wireless Mouse - Gray",
                "variant_id": "P001-GRAY",
                "wireless": false
              }
            ]
          - |-
            [
              {
                "length_m": 1,
                "title": "FlexiLink USB-C Cable 1m",
                "variant_id": "P002-1M"
              },
              {
                "length_m": 2,
                "title": "FlexiLink USB-C Cable 2m",
                "variant_id": "P002-2M"
              },
              {
                "length_m": 3,
                "title": "FlexiLink USB-C Cable 3m",
                "variant_id": "P002-3M"
              }
            ]
    time_dimensions:
      - name: _SYNCED_AT
        description: The timestamp (without time zone) indicating when the record was last synchronized.
        expr: _SYNCED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2024-10-01T13:45:00.000Z'
      - name: CREATED_AT
        description: The timestamp indicating when the product record was created.
        expr: CREATED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - '2024-01-15T09:30:00.000Z'
          - '2024-02-01T11:35:00.000Z'
          - '2024-01-20T10:15:00.000Z'
    facts:
      - name: UNIT_PRICE
        description: The price of a single unit of a product.
        expr: UNIT_PRICE
        data_type: NUMBER
        sample_values:
          - '29.99'
          - '12.99'
          - '45.00'
    primary_key:
      columns:
        - PRODUCT_ID
relationships:
  - name: STG_ECOMM__ORDER_LINES_TO_STG_ECOMM__ORDERS
    left_table: STG_ECOMM__ORDER_LINES
    relationship_columns:
      - left_column: ORDER_ID
        right_column: ORDER_ID
    right_table: STG_ECOMM__ORDERS
  - name: STG_ECOMM__PRODUCTS_TO_STG_ECOMM__ORDER_LINES
    left_table: STG_ECOMM__PRODUCTS
    relationship_columns:
      - left_column: PRODUCT_ID
        right_column: PRODUCT_ID
    right_table: STG_ECOMM__ORDER_LINES
