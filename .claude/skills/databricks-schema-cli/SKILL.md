---
name: databricks-schema-cli
description: Explore, snapshot, diff, and generate migration SQL for Databricks Unity Catalog schemas using the `databricks-schema` CLI. Use this whenever the user wants to list catalogs/schemas in a Databricks workspace, pull a Unity Catalog schema (tables, columns, keys, tags) into version-controllable YAML/JSON, compare a live catalog against stored schema files or two schema directories against each other, validate schema files, or generate Spark SQL DDL to reconcile a catalog with a desired state — even if they just say "what's in this Databricks catalog" or "diff prod vs what's checked in" without naming the tool.
---

# Databricks Unity Catalog exploration (`databricks-schema`)

`databricks-schema` is a CLI that talks to a Databricks workspace via the Databricks SDK and
represents Unity Catalog schemas as YAML or JSON. Use it instead of writing ad-hoc SDK calls
whenever the task is "look at / snapshot / diff / generate SQL for" a Unity Catalog schema.

Call it as `databricks-schema <command> ...`. If the command isn't found, install it first (e.g.
`uv tool install databricks-schema`). Run `databricks-schema <command> --help` to confirm exact
usage — flags shown below can drift; the CLI's own `--help` is the source of truth.

## Authentication

Don't pass `--host` / `--token` yourself. Just run the commands — the CLI resolves credentials
on its own via the Databricks SDK, from `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars, an active
`databricks auth login` session, or a profile in `~/.databrickscfg`, whichever is already set up.

If a command fails with `Unauthenticated`/`PermissionDenied`, tell the user auth is
missing/insufficient rather than trying to fix it yourself. Don't confuse this with `NotFound`,
which means the catalog/schema/table name itself is wrong.

## Flags shared across commands

Read this once — command sections below only call out what's *different* for that command.

| Flag | Where | Meaning |
|---|---|---|
| `--schema`/`-s NAME` (repeatable) | `extract`, `diff`, `generate-sql`, `diff-files`, `validate` | Filter to specific schema(s). Omit → all schemas. |
| `catalog.schema[.table]` dotted arg | `extract`'s `catalog`; `diff`'s `catalog` and `target` (when `target` isn't a directory) | Shortcut for filtering to one schema or table instead of `--schema`. Exits **2** if combined with `--schema`. On `diff`, both sides must dot to the same depth (`cat.schema` vs `cat` is an error), but the schema/table **names need not match across sides** — this is how you diff a differently-named copy (e.g. a `_test` schema/table) against the "real" one, same or different catalog. Can't be used on `catalog` when `diff`'s `target` is a directory (use `--schema` there instead). |
| `--include-metadata` | `extract`, `diff`, `generate-sql`, `diff-files` | Adds `owner` to output/comparison. `extract` additionally adds `storage_location` (not compared elsewhere). Default: off. |
| `--include-tags` | `extract`, `diff`, `generate-sql` | Adds Unity Catalog tag lookups (extra API call per entity). Default: off. |
| `--quiet`/`-q` | `extract`, `diff`, `generate-sql`, `diff-files` | Suppresses stderr progress lines (e.g. `Extracting catalog 'X'...`). Errors always print. Use for agentic/scripted runs — actual output and exit code are unaffected. |
| `--output-dir`/`-o DIR` | `extract`, `generate-sql` | Write one file per schema instead of printing to stdout. |
| `--workers N` (default 4) | `extract`, `diff`, `generate-sql` | Parallel table-extraction workers; raise for large catalogs. |

### `--format`/`-f` — same flag name, different meaning per command

| Command(s) | Choices | Default | Controls |
|---|---|---|---|
| `extract` | `yaml`, `json` | `yaml` | File format of the extracted schema **content** |
| `diff`, `diff-files`, `validate`, `list-catalogs`, `list-schemas` | `text`, `json`, `yaml` | `text` | **Representation** of the result (`text` = human-readable) |
| `json-schema` | `json`, `yaml` | `json` | Representation of the JSON Schema output itself |

Before parsing `--format json`/`yaml` output (or extract's YAML/JSON) programmatically, run
`databricks-schema json-schema <model>` to get its exact shape instead of guessing from an
example.

## Orienting yourself in a workspace

```bash
databricks-schema list-catalogs              # what catalogs can I see?
databricks-schema list-schemas <catalog>      # what schemas are in this catalog?
```

Prints names, one per line (or `--format json`/`yaml` for a list) — good for a quick scan or for
building a `--schema` filter list for the commands below.

## `extract` — pull a schema into a readable snapshot

```bash
databricks-schema extract <catalog> --schema main       # one schema, to stdout
databricks-schema extract <catalog>                      # whole catalog, to stdout
databricks-schema extract <catalog> --output-dir ./schemas/   # one file per schema
databricks-schema extract <catalog>.<schema>.<table>      # single table, dotted shortcut
```

Omitting `--output-dir` prints one `Catalog` document to stdout (`name` + `schemas: [...]`)
covering all matching schemas; with it, one file per schema is written, named `<schema>.yaml`
(or `.json`).

Shape of one schema (fields with no value are omitted entirely; foreign keys reference
`ref_schema`+`ref_table` only — always the same catalog as the source table):

```yaml
name: main
comment: Main production schema
tags:
  env: prod
tables:
  - name: users
    table_type: MANAGED
    comment: User accounts
    tags:
      domain: identity
    columns:
      - name: id
        data_type: bigint
        nullable: false
        comment: Primary key
      - name: email
        data_type: string
    primary_key:
      name: pk_users
      columns: [id]
    foreign_keys:
      - name: fk_org
        columns: [org_id]
        ref_schema: orgs
        ref_table: organizations
        ref_columns: [id]
```

Stdout output (no `--output-dir`) wraps this under `schemas:`, with the catalog name at the top
level, instead of writing it standalone.

## `diff` / `diff-files` — compare catalog state

```bash
databricks-schema diff <catalog> ./schemas/                    # live vs. stored directory
databricks-schema diff dev_catalog prod_catalog                 # live vs. live, e.g. dev vs. prod
databricks-schema diff mycat.orders mycat.orders_test           # one schema vs. a differently-named one
databricks-schema diff mycat.sales.orders othercat.sales.orders_v2   # one table vs. another, cross-catalog
databricks-schema diff-files ./schemas-old/ ./schemas-new/      # two local dirs, no Databricks connection
```

For `diff`, `target` is a directory if one exists on disk at that path, otherwise it's read as a
second catalog name (see the dotted-arg row above for schema/table-scoped comparisons). Either
way, `catalog` is the "live"/actual side and `target` is the baseline/reference side — that
ordering decides which side of each diff shows as `+`/`-`.

Text output is a tree with `+` (added), `-` (removed), `~` (modified) markers:

```
~ Schema: main [MODIFIED]
  ~ Table: users [MODIFIED]
    ~ Column: score [MODIFIED]
        data_type: 'int' -> 'double'
    + Column: phone [ADDED]
  + Table: events [ADDED]
- Schema: legacy [REMOVED]
```

`--format json`/`yaml` gives the same comparison as a `{"schemas": [...]}` document instead —
per-schema `status` (`added`/`removed`/`modified`/`unchanged`), `changes` (field-level `old`/`new`
pairs), and nested `tables`/`columns`. Run `databricks-schema json-schema diff` for the exact
shape.

**Exit codes matter — use them instead of parsing stdout for a yes/no:**

| Code | `diff` / `diff-files` |
|---|---|
| 0 | no differences |
| 1 | differences found (normal, not a failure) |
| 2 | usage error — bad directory, mixed YAML+JSON, no schema files found, mismatched dotted-arg depth, or dotted syntax combined with `--schema`/a directory target |

## `validate` — sanity-check local files

```bash
databricks-schema validate ./schemas/
```

Structural integrity check on local YAML/JSON, no Databricks connection (e.g. after hand-editing
one). Exit `0` + `OK — N schema(s) validated` on success, `1` + one `ERROR:` line per issue
otherwise. `--format json`/`yaml` gives `{"issues": [...]}` (`schema`, `table`, `message` per
issue) — see `databricks-schema json-schema validate` for the exact shape.

## `generate-sql` — produce migration DDL

```bash
databricks-schema generate-sql <catalog> ./schemas/                            # to stdout
databricks-schema generate-sql <catalog> ./schemas/ --output-dir ./migrations/  # one .sql per schema
```

Databricks Spark SQL DDL to bring the *live* catalog in line with the *stored* files
(create/alter tables and columns, add/drop keys, etc.). Destructive statements (`DROP SCHEMA`,
`DROP TABLE`, `DROP COLUMN`) are commented out by default — pass `--allow-drop` to make them
executable. Treat this as review-then-run, not something to pipe straight into execution,
especially with `--allow-drop`. Unsupported changes (e.g. `table_type`) become
`-- TODO: unsupported change: ...` comments rather than being silently dropped.

## `json-schema` — introspect output shapes before parsing them

```bash
databricks-schema json-schema catalog    # shape of extract's stdout Catalog document
databricks-schema json-schema schema     # shape of one per-schema extract file
databricks-schema json-schema diff       # shape of diff / diff-files --format json
databricks-schema json-schema validate   # shape of validate --format json
```

No Databricks connection needed. Run this before parsing any `--format json`/`yaml` output (or
extract's YAML/JSON) programmatically, instead of guessing from an example.

## Choosing the right command

| Want to...                                                 | Command        |
|--------------------------------------------------------------|----------------|
| See what catalogs/schemas exist                              | `list-catalogs`, `list-schemas` |
| Read/summarize a live schema's structure                     | `extract` (no `--output-dir`, use `--schema`) |
| Snapshot a catalog for version control                       | `extract --output-dir` |
| Check if a catalog drifted from a checked-in snapshot         | `diff` |
| Compare two schemas/tables directly (incl. differently-named) | `diff` with dotted `catalog.schema[.table]` args |
| Check if two snapshot directories differ (no live access)    | `diff-files` |
| Sanity-check hand-edited YAML/JSON before using it            | `validate` |
| Produce SQL to reconcile live catalog with a snapshot         | `generate-sql` |
| Get the exact shape of any of the above before parsing it     | `json-schema` |
