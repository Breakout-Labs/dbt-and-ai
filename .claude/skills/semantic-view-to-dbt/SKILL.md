---
name: semantic-view-to-dbt
description: Converts a validated Cortex Analyst semantic view YAML into a dbt model using the snowflake_semantic_view materialization, replacing base_table blocks with sv_ref() so dbt tracks lineage to the underlying models. Use when the user pastes a semantic view YAML and asks to turn it into a dbt model, or asks to add/convert a Snowflake semantic view.
---

# Semantic view → dbt model

Use this skill whenever the user provides a Cortex Analyst / Snowflake semantic view YAML
(validated via `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` or the Cortex Analyst UI) and wants it
turned into a dbt model in this project.

## Tooling this relies on

This project already ships the macros needed — do not reimplement them:

- `macros/sdc_semantic_view/snowflake_semantic_view.sql` — the `snowflake_semantic_view`
  custom materialization. It auto-generates the top-level `name:` key from the model's alias
  (uppercased) and calls `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML`. If the pasted YAML already
  starts with a `name:` line, that line is stripped (with a logged warning) and overridden —
  so don't bother preserving it.
- `macros/sdc_semantic_view/sv_ref.sql` — `sv_ref(ref_name, [database_name], [schema_name], [v|version])`.
  Resolves `ref(ref_name)` and emits an indented `database:` / `schema:` / `table:` block.
  This is what creates real dbt lineage — always prefer it over a literal `base_table:`.
- `macros/sdc_semantic_view/yaml_long_text.sql` — `yaml_long_text(long_text, indent=10)`.
  Formats a Python/Jinja string containing `\n` into a YAML block scalar (`|`) with the given
  indent. Use it only for descriptions that actually contain newlines.
- `macros/sdc_semantic_view/readme.md` documents both macros with worked examples.

## Conversion steps

1. **Pick the model name and file path.** Mart models get a short descriptive name (no
   `stg_`/`int_` prefix). Place the file at `models/marts/<model_name>.sql`. If the source YAML
   has a `name:` field like `REVENUE_ANALYSIS_DRAFT`, don't carry a `_DRAFT`/env suffix into the
   dbt model name — pick the clean production name (e.g. `revenue_analysis`); the materialization
   derives the actual semantic view name from this alias anyway.

2. **Add the config block** at the top of the new `.sql` file, since the mart default in
   `dbt_project.yml` is `materialized: table`:

   ```sql
   {{
       config(
           materialized='snowflake_semantic_view'
       )
   }}
   ```

3. **Paste the validated YAML** below the config block, then make only these mechanical edits:

   - **Drop the top-level `name:` line.** It gets stripped/overridden by the materialization
     regardless, and leaving it in is misleading.
   - **Replace every `base_table:` block** with a single line calling `sv_ref()`, pointing at the
     dbt model that corresponds to that Snowflake table (usually a `stg_<source>__<table>` model
     matching the `base_table.table` value):

     ```yaml
     # before
     base_table:
       database: DEMO
       schema: DBT_MGEERKEN_SANDBOX_AI
       table: STG_ECOMM__ORDERS

     # after
     base_table: {{ sv_ref('stg_ecomm__orders') }}
     ```

     Confirm the target model actually exists (`models/staging/...` or wherever) before wiring
     the ref — don't invent a model name. If the semantic view references a table that has no
     dbt model yet, flag that to the user instead of guessing.
   - **Wrap any multiline `description:` fields in `yaml_long_text()`.** Most `sample_values`,
     dimension, and metric descriptions in a Cortex-validated YAML are single-line and should be
     left as plain strings — only reach for the macro when a description genuinely spans
     multiple lines:

     ```yaml
     # before (description contains embedded newlines)
     description: |
       Line one of the description.
       Line two adds more detail.

     # after
     description: {{ yaml_long_text("Line one of the description.\nLine two adds more detail.") }}
     ```

     If you need to keep a long literal string readable in the model file, set it as a Jinja
     variable first and pass the variable into the macro (see `readme.md` for the pattern), then
     reference that variable in the `description:` field.
   - **Everything else stays verbatim** — `description` (single-line), `dimensions`,
     `time_dimensions`, `facts`, `metrics`, `primary_key`, `unique_keys`, and `relationships`
     blocks are copied through unchanged, same wording, same ordering. Do not "fix" anything in
     the validated YAML (e.g. a `unique_keys` column that looks wrong) — flag it as a risk to the
     user instead of silently changing it.

4. **Add a properties YAML** at `models/marts/_<model_name>.yml` per the mart convention (own
   file per model), with a model-level description. **Do not add `unique`/`not_null` column
   tests on a "primary key"** — a semantic view is not a plain selectable relation with a fixed
   row grain, so dbt's generic data tests don't apply the way they do to a normal mart table.
   Note this deviation from the standard mart test rule when presenting the change.

## Validation

- `dbt compile -s <model_name>` — confirms the Jinja (config + `sv_ref()`/`yaml_long_text()`
  calls) renders into valid YAML.
- `dbt list -s +<model_name>` — confirms the DAG shows the semantic view depending on the
  correct upstream models (and their sources), not a hardcoded schema pointer.
- `dbt run -s <model_name>` actually executes `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` against
  Snowflake — this is a real DDL/warehouse action. Only run it with explicit user go-ahead,
  separate from approval of the file changes themselves.

## Quick checklist

- [ ] Model file at `models/marts/<model_name>.sql` with `snowflake_semantic_view` config
- [ ] No leftover top-level `name:` line in the pasted YAML
- [ ] Every `base_table:` replaced with `sv_ref('<upstream_model>')`, verified the model exists
- [ ] Multiline descriptions (only) wrapped in `yaml_long_text()`
- [ ] All other descriptions, dimensions, facts, metrics, keys, and relationships preserved verbatim
- [ ] `models/marts/_<model_name>.yml` added with a model description, no PK data tests
- [ ] `dbt compile -s <model_name>` and `dbt list -s +<model_name>` both checked before offering to `dbt run`
