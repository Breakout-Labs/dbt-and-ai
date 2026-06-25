# dbt Training Project

E-commerce dbt project on Snowflake: orders, customers, deliveries,
order_lines, and products for a US webshop.

## Layer Definitions
- Staging: one-to-one with the source. Rename and cast only.
  NEVER add business logic - no joins, no aggregations, no recalculated columns.
- Intermediate: combine and reshape staging models. Light business logic.
- Mart: final consumption layer - business logic and aggregations.

## Naming Conventions
- File name: `stg_<source>__<table>.sql`
- Rename the source `id` to `<entity>_id` (e.g. `id as order_line_id`)
- Rename timestamps to end in `_at`
- Use the `source` / `renamed` / `final` CTE structure of the existing staging models

## Constraints
- Every primary key MUST have `not_null` and `unique` tests in a `properties.yml`
- Every column MUST have a description in `properties.yml`
- Staging models must NEVER modify `sources.yml`