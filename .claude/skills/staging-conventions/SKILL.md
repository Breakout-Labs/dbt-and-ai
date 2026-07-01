---
name: staging-conventions
description: Apply this project's staging layer conventions whenever creating or editing a `stg_` model. Enforces staging naming, one-to-one source mapping, column rename rules, YAML placement, and when primary key tests should be added.
---

# Staging conventions

Use this skill whenever you create, modify, or review a staging model in this project.

## Core rule

A staging model is a thin, one-to-one layer over a raw source table. Keep it boring.

What belongs in staging:
- `source()` references to raw tables
- column selection
- light renaming into analytics-friendly names
- simple casting / cleaning when needed
- passthrough sync metadata like `_synced_at`

What does **not** belong in staging:
- joins across sources
- aggregations
- business logic
- deduplication with non-trivial rules
- window-function-driven reshaping
- metrics or KPI calculations

If the transformation changes the grain or combines multiple inputs, it should move to `intermediate` or `marts`.

## File and naming conventions

### Model SQL path
Place staging SQL files under:

```text
models/staging/<source_name>/stg_<source_name>__<table_name>.sql
```

Examples:
- `models/staging/ecomm/stg_ecomm__orders.sql`
- `models/staging/ecomm/stg_ecomm__products.sql`
- `models/staging/sheets/stg_sheets__customer_survey_responses.sql`

### Source YAML
Use one source YAML file per source inside the source subfolder when following project conventions.
If the repo already centralizes source declarations differently, update the existing source YAML instead of creating a duplicate declaration.

### Properties YAML
For staging models, use one shared properties file per subfolder:

```text
models/staging/<source_name>/properties.yml
```

Do not create per-model YAML files for staging models.

## Required SQL shape
Match the existing repo style:

```sql
with

source as (

    select * from {{ source('<source_name>', '<table_name>') }}

),

renamed as (

    select
        ...

    from source

)

select * from renamed
```

Guidelines:
- Prefer a `source` CTE and a `renamed` CTE
- Use `select *` only in the `source` CTE
- In `renamed`, explicitly select columns in the intended order
- End with `select * from renamed`

## Column rename rules

Staging should make raw column names consistent and analytics-friendly without changing meaning.

### Rename patterns to prefer
- raw primary key `id` → `<entity>_id`
  - `id` in orders → `order_id`
  - `id` in customers → `customer_id`
  - `id` in products → `product_id`
- status fields should be explicit when ambiguity is possible
  - `status` in orders → `order_status`
  - `status` in deliveries → `delivery_status`
- category-like fields can be prefixed with the entity when it improves clarity
  - `category` → `product_category`
  - `subcategory` → `product_subcategory`
- timestamps may be renamed when the entity context is useful, but keep the semantic meaning precise
  - good: `created_at as ordered_at`
  - better when needed: `created_at as order_created_at`
  - avoid misleading date-only names for timestamp fields unless you also cast to date

### Keep as-is when already clear
Leave columns unchanged when the raw name is already specific and clean, for example:
- `total_amount`
- `customer_id`
- `store_id`
- `unit_price`
- `is_active`
- `_synced_at`

### Preserve semantics
- Do not rename a timestamp to a date-oriented name unless it is truly a date or explicitly cast to one
- Do not apply business definitions during rename
- Do not hide raw meaning with overly generic names

## Documentation requirements
Every staging model must have in `models/staging/<source_name>/properties.yml`:
- a model-level description
- a description for every column

Use modern dbt YAML syntax.

Always use:

```yml
data_tests:
```

Never use legacy:

```yml
tests:
```

Descriptions should explain what the field represents, not just restate the name.

## Test requirements
Default project rule:
- staging and intermediate models have no tests by default

Exception:
- if the user explicitly asks for primary key tests on a staging model, add column-level:
  - `unique`
  - `not_null`

When adding PK tests, attach them to the renamed business key column in `properties.yml`, for example:

```yml
- name: product_id
  description: Primary key for the product.
  data_tests:
    - unique
    - not_null
```

Do not add extra staging tests unless requested or clearly warranted by the task.

## Validation
After editing staging SQL, validate with the lightest check that exercises the change:

```text
dbt build --select +<staging_model_name>+
```

After editing only staging YAML/properties structure, run:

```text
dbt parse
```

If you changed both SQL and YAML, run the build for the staging model so downstream references and any requested tests are exercised.

## Quick checklist
Before finishing a staging change, confirm:
- file name follows `stg_<source>__<table>.sql`
- model lives under the correct `models/staging/<source>/` folder
- model is one-to-one with a single `source()` table
- renames improve clarity without changing semantics
- raw `id` is renamed to an entity-specific key where appropriate
- status columns are made explicit where helpful
- `_synced_at` is preserved when present in the source
- shared staging `properties.yml` includes model and column descriptions
- YAML uses `data_tests:`
- PK tests are only added when the user asked for them or the task explicitly requires them
