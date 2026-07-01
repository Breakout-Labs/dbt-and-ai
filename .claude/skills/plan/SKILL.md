---
name: plan
description: Write an implementation plan (what changes, affected files, assumptions, risks) for a task in this dbt project, then wait for explicit approval before editing any code. Use when the user asks to plan, scope, or design a change before implementing it.
---

# Plan

Produce a written implementation plan for the requested task, then stop and
wait for the user to approve it. Do not create, edit, or delete any files
(dbt models, YAML, macros, seeds, etc.) until the user has explicitly approved
the plan.

## Process

1. **Understand the ask.** If the task description passed as `args` is vague,
   ask up to 2-3 clarifying questions before planning (e.g. which source,
   which layer, expected grain).

2. **Research before writing the plan.** Use Read/Grep/Glob (or the Explore
   agent for broad lookups) to understand the current state:
   - Relevant existing models, sources, and macros
   - Naming conventions and folder layout already in use (see project
     `CLAUDE.md` for the rules: staging/intermediate/mart layer definitions,
     naming conventions, and per-model YAML requirements)
   - Any tests or docs already covering the affected area

3. **Write the plan.** Structure it with these sections:
   - **Goal** — one or two sentences on what this change accomplishes.
   - **Changes** — the concrete changes to make, described model-by-model or
     file-by-file (e.g. "new staging model `stg_ecomm__returns.sql` casting
     `return_date` to date, renaming `cust_id` -> `customer_id`").
   - **Files affected** — every file to be created or modified, with its
     path, following this project's conventions:
     - Staging: `stg_<source>__<table>.sql`, tests-free, described in the
       subfolder's shared `properties.yml`
     - Intermediate: `int_<business>_<descriptive_name>.sql`, same shared
       `properties.yml` pattern
     - Mart: short descriptive name, with its own `_<model_name>.yml`
       (description + unique/not_null tests on the primary key)
     - Source YAML: one file per source in that source's subfolder
   - **Assumptions** — anything not explicitly stated in the request that
     you're assuming (grain, data types, how nulls/duplicates are handled,
     which source table is authoritative, etc.).
   - **Risks** — what could break: downstream models that depend on
     something being renamed/removed, tests that might start failing,
     ambiguous business logic, performance concerns on large sources.
   - **Open questions** (if any) — things worth confirming with the user
     before or during implementation, distinct from assumptions you're
     already comfortable making.

4. **Present the plan and stop.** Use `ExitPlanMode` (or, if not already in
   plan mode, `EnterPlanMode` first) to surface the plan for approval. Do not
   proceed to any file-modifying tool call until the user approves.

5. **After approval**, implement exactly what the plan described. If
   implementation reveals the plan was wrong or incomplete in a material way,
   pause and check with the user rather than silently deviating.

## Notes

- This skill is for scoping/design — it does not run `dbt run`, `dbt build`,
  or any command that mutates the warehouse or the repo.
- Keep the plan concrete and file-specific rather than abstract; the goal is
  that the user can approve or redirect specific line items, not a vague
  intent.
