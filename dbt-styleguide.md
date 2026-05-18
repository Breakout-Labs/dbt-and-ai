# dbt style guide

This project should feel boring in the best way: easy to scan, easy to review, and easy to extend without breaking downstream models.

Use this as the default for new models, YAML, tests, and docs.

## Project shape

This repo is configured with:

- `models/`
- `analyses/`
- `tests/`
- `seeds/`
- `macros/`
- `snapshots/`

Default materializations from `dbt_project.yml`:

- models default to `view`
- `models/marts/` defaults to `table`
- `models/intermediate/` defaults to `ephemeral`

Stick to those defaults unless there is a clear performance or maintenance reason not to.

## Layering

Organize models by purpose, not by analyst or ticket.

### Staging

Use staging models to do the minimum necessary cleanup on raw sources:

- one source table per staging model
- rename columns into clean, analytics-friendly names
- cast data types
- standardize booleans, timestamps, and IDs
- keep business logic light

Conventions:

- folder: `models/staging/`
- model names: `stg_<source_entity>`
- use `source()` for raw inputs

### Intermediate

Use intermediate models for reusable transformation steps that are not final business entities.

Typical use cases:

- joins across staging models
- fanout control
- derived fields used in multiple marts
- grain changes before final presentation

Conventions:

- folder: `models/intermediate/`
- model names: `int_<purpose>`
- materialization should usually stay `ephemeral`

If an intermediate model is reused heavily or becomes expensive, promote it intentionally rather than by accident.

### Marts

Use marts for business-facing models that people actually query.

Typical use cases:

- dimensions
- facts
- KPI-ready datasets
- team-facing reporting tables

Conventions:

- folder: `models/marts/`
- model names:
  - `dim_<entity>`
  - `fct_<business_process>`
  - `rpt_<purpose>` for narrow reporting outputs when needed
- use `ref()` only, never `source()` directly from marts unless there is a very good reason

## SQL style

Keep SQL easy to read in a PR.

### Structure

Prefer this shape:

```sql
with orders as (
    select *
    from {{ ref('stg_orders') }}
),

order_items as (
    select *
    from {{ ref('stg_order_items') }}
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        sum(order_items.item_revenue) as order_revenue
    from orders
    left join order_items
        on orders.order_id = order_items.order_id
    group by 1, 2, 3
)

select *
from final
```

Guidelines:

- start with CTEs, end with a `final` CTE when the model is non-trivial
- keep one logical step per CTE
- use descriptive CTE names, not `t1`, `tmp`, or `final_final`
- prefer leading commas only if the repo already standardizes on them; otherwise keep commas trailing and consistent
- use lowercase SQL keywords for consistency

### Aliasing

Alias tables when it improves readability, not by reflex.

Good:

```sql
select
    orders.order_id,
    customers.customer_name
from {{ ref('fct_orders') }} as orders
left join {{ ref('dim_customers') }} as customers
    on orders.customer_id = customers.customer_id
```

Avoid single-letter aliases except in very tight, obvious queries.

### Select lists

Be explicit in marts. `select *` is fine in tightly scoped staging or pass-through CTEs, but not in final business models.

Prefer:

- explicit column lists in final select statements
- stable column ordering
- one column per line
- derived columns near their inputs when it helps readability

### Joins

Make grain changes obvious.

- state the intended grain in a short comment when it is not obvious
- join on declared keys, not fuzzy text fields
- aggregate before joining when that prevents duplication
- if a join can create fanout, handle it deliberately and comment it

### Aggregations

- group by explicit positions is acceptable if the select list is short and stable
- if the query is long or frequently edited, use explicit column names instead
- name aggregated fields clearly, like `order_revenue` or `customer_lifetime_value`

## Naming conventions

Naming should tell you grain and purpose quickly.

### Models

- staging: `stg_<entity>`
- intermediate: `int_<purpose>`
- dimensions: `dim_<entity>`
- facts: `fct_<process>`
- reports: `rpt_<purpose>`

Avoid model names that encode implementation details like `final`, `new`, `v2`, or `fixed`.

### Columns

- primary keys: `<entity>_id`
- foreign keys: match the referenced primary key name
- booleans: `is_...`, `has_...`
- timestamps: end with `_at`
- dates: end with `_date`
- counts: end with `_count`
- percentages or rates: end with `_pct` or `_rate`
- avoid spaces, special characters, and ambiguous abbreviations

If the warehouse column names are messy, fix them in staging once and keep downstream models clean.

## Tests

Every model should earn trust.

### Model-level expectations

At a minimum, add tests for:

- primary keys: `not_null` and `unique`
- important foreign keys: `relationships` where it makes sense
- critical business constraints: accepted values, non-null expectations, or custom singular tests

Use schema YAML with modern syntax:

```yaml
models:
  - name: dim_customers
    columns:
      - name: customer_id
        data_tests:
          - not_null
          - unique
```

Use `data_tests:`, not `tests:`.

### What to test where

- staging: key integrity, accepted values, basic source assumptions
- intermediate: test only when business logic is meaningful and reused
- marts: always test keys, grain, and important business rules

Don’t add noisy tests nobody will maintain.

## Documentation

Document the stuff someone would actually ask in six months.

Add descriptions in schema YAML for:

- every mart model
- important staging models
- key business columns
- any metric-driving logic that is easy to misunderstand

Good descriptions explain meaning and business rules, not just restate the column name.

Weak: `Customer id`

Better: `Unique identifier for a customer in the ecommerce application.`

Prefer standard dbt YAML descriptions over standalone markdown unless you specifically need reusable `docs` blocks.

## Sources

Treat source definitions as contracts with the warehouse.

- define raw inputs with `source()`
- add freshness or source tests where they matter
- keep source naming stable and readable
- don’t let raw warehouse naming leak deep into marts

## Materializations

Default recommendation for this repo:

- `view` for staging and simple transformations
- `ephemeral` for lightweight intermediate logic
- `table` for marts

Only use incremental when there is a clear volume or runtime need.

If you create an incremental model:

- define a real `unique_key`
- make the incremental filter deterministic
- think through late-arriving data
- test full-refresh and incremental behavior separately

## Macros

Use macros for repeated logic, not for hiding simple SQL.

Good candidates:

- repeated case logic
- safe division
- standard surrogate key generation
- cross-model conventions used in several places

If a macro makes a model harder to read than the raw SQL, it probably shouldn’t be a macro.

## PR hygiene

Before considering a model done:

- make sure naming matches the layer and grain
- check that `ref()` and `source()` usage is clean
- add or update YAML docs
- add key `data_tests:`
- compile the changed model
- sanity-check the output with `dbt show` when logic changed materially

## Opinionated defaults

A few strong defaults for this repo:

- prefer one model per business concept over giant all-in-one SQL files
- push raw cleanup into staging, not marts
- keep intermediate models ephemeral until there is a reason not to
- don’t use `select *` in final mart outputs
- don’t expose raw source column names to business users if you can clean them upstream
- if the grain is not obvious, write it down in a short comment

## Example folder layout

```text
models/
  staging/
    stg_customers.sql
    stg_orders.sql
    staging.yml
  intermediate/
    int_orders_joined.sql
    intermediate.yml
  marts/
    dim_customers.sql
    fct_orders.sql
    marts.yml
```

If you want, I can also generate a tighter version of this tailored to your actual current models and naming patterns, or add a companion `CONTRIBUTING.md` with PR expectations.