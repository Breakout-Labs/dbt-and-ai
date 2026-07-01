# dbt Training Project

## Project Overview
E-commerce dbt project on Snowflake: orders, customers, deliveries, and products
for a US webshop. Used for analytics and AI-driven consumption.

## Layer Definitions
- Staging: one-to-one with sources. Rename, cast, clean - no business logic.
- Intermediate: combine and reshape staging models. Light business logic.
- Mart: final consumption layer - business logic and aggregations.

## Naming Conventions
- Staging models follow the naming convention: stg_<source>__<table>.sql
- Intermediate models follow the naming convention: int_<businss>_<descriptive name>.sql
- Mart models have a short descriptive name like orders or customers

## Constraints
- Each Marts model have an own properties YAML file with description on the model and every column - naming convention: _<model_name>.yml
- Mart models have unique and not_null tests on the primary key
- Staging and intermediate models have a combined properties.yml file per sub-folder e.g. staging/ecomm/properties.yml
- Staging and intermediate models have no tests by default but a description per model and each column
- One source yaml file per source inside the subfolder of the source