---
name: databricks-schema-cli
description: Explore, snapshot, diff, and generate migration SQL for Databricks Unity Catalog schemas using the `databricks-schema` CLI. Use this whenever the user wants to list catalogs/schemas in a Databricks workspace, pull a Unity Catalog schema (tables, columns, keys, tags) into version-controllable YAML/JSON, compare a live catalog against stored schema files or two schema directories against each other, validate schema files, or generate Spark SQL DDL to reconcile a catalog with a desired state — even if they just say "what's in this Databricks catalog" or "diff prod vs what's checked in" without naming the tool.
---

# Databricks Unity Catalog exploration (`databricks-schema`)

`databricks-schema` is a CLI that talks to a Databricks workspace via the Databricks SDK and
represents Unity Catalog schemas as YAML/JSON files — one file per schema. Use it instead of
writing ad-hoc SDK calls whenever the task is "look at / snapshot / diff / generate SQL for" a
Unity Catalog schema.

Call it as `databricks-schema <command> ...`. If the command isn't found, install it first (see
`README.md`, e.g. `uv tool install databricks-schema`). Run `databricks-schema <command> --help`
to confirm exact flags before relying on this document for anything unusual — this skill covers
the common path, not every flag.

## Authentication

Don't pass `--host` / `--token` yourself. Just run the commands — the CLI resolves credentials
on its own via the Databricks SDK, from `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars, an active
`databricks auth login` session, or a profile in `~/.databrickscfg`, whichever is already set up
in the environment.

If a command fails with an `Unauthenticated`/`PermissionDenied` error, that means none of those
are configured (or the credential lacks access) — tell the user auth is missing/insufficient
rather than trying to fix it yourself. Don't confuse this with a `NotFound` error, which means
the catalog/schema name itself is wrong.

## Reducing noise for agentic use

`extract`, `diff`, `generate-sql`, and `diff-files` print progress lines (e.g. `Extracting
catalog 'X'...`, `Comparing catalog 'X' against ...`) to stderr before/while they run. Pass
`--quiet` / `-q` to suppress them — the actual result (extracted YAML/JSON, diff tree, generated
SQL, exit code) is unaffected; only the informational narration is dropped. Errors are always
printed regardless of `--quiet`.

## Orienting yourself in a workspace

```bash
databricks-schema list-catalogs              # what catalogs can I see?
databricks-schema list-schemas <catalog>      # what schemas are in this catalog?
```

Both just print names, one per line — good for a quick scan or for building a `--schema` filter
list for the commands below.

## Pulling a schema into a readable snapshot

```bash
# Printed straight to stdout as one Catalog document (no --output-dir) — good for
# "show me schema X" or "show me this whole catalog"
databricks-schema extract <catalog> --schema main
databricks-schema extract <catalog>

# Whole catalog (or a filtered set of schemas) written to one file per schema
databricks-schema extract <catalog> --output-dir ./schemas/
databricks-schema extract <catalog> --schema main --schema raw --output-dir ./schemas/
```

Notes:
- `--schema` / `-s` is repeatable; omit it to extract every schema.
- `--output-dir` writes one file per schema; omitting it prints one `Catalog` document to stdout
  (`name` + `schemas: [...]`) covering all matching schemas.
- `--format json` / `-f json` writes `.json` instead of `.yaml` (same structure either way).
- `--include-metadata` adds `owner` and `storage_location` (excluded by default — smaller, more
  diffable output).
- `--include-tags` adds Unity Catalog tags (excluded by default — extra API calls per entity).
- `--workers N` controls parallel table extraction (default 4); raise it for large catalogs.

### Shape of the output

Each schema file looks like this (fields with no value are omitted entirely):

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

Read this directly to answer questions about a catalog's structure — column names/types,
nullability, primary/foreign keys, per-object tags — without needing SDK calls of your own.
Foreign keys reference `ref_schema` + `ref_table` only (same catalog as the source table).

Stdout output (no `--output-dir`) wraps this in a `Catalog` document instead — same schema shape,
nested under `schemas`, with the catalog name at the top level:

```yaml
name: prod_catalog
schemas:
  - name: main
    comment: Main production schema
    tags:
      env: prod
    tables: [...]
```

## Comparing catalog state

```bash
# Live catalog vs. a directory of stored schema files (format auto-detected: YAML or JSON)
databricks-schema diff <catalog> ./schemas/

# Two local directories, no Databricks connection needed
databricks-schema diff-files ./schemas-old/ ./schemas-new/
```

Both print a tree with `+` (added), `-` (removed), `~` (modified) markers, e.g.:

```
~ Schema: main [MODIFIED]
  ~ Table: users [MODIFIED]
    ~ Column: score [MODIFIED]
        data_type: 'int' -> 'double'
    + Column: phone [ADDED]
  + Table: events [ADDED]
- Schema: legacy [REMOVED]
```

**Exit codes matter — use them instead of parsing stdout when you just need a yes/no:**
- `0` — no differences
- `1` — differences found (this is normal, not a failure)
- `2` — usage error (bad directory, mixed YAML+JSON in one directory, no schema files found)

Same `--schema`, `--include-tags`, `--include-metadata` flags apply as for `extract`.

## Validating schema files

```bash
databricks-schema validate ./schemas/
```

Checks structural integrity of local YAML/JSON files with no Databricks connection (e.g. after
hand-editing one). Exits `0` and prints `OK — N schema(s) validated` on success, `1` with an
`ERROR:` line per issue otherwise.

## Generating migration SQL

```bash
databricks-schema generate-sql <catalog> ./schemas/                       # print to stdout
databricks-schema generate-sql <catalog> ./schemas/ --output-dir ./migrations/  # one .sql per schema
```

Produces Databricks Spark SQL DDL to bring the *live* catalog in line with the *stored* files
(create/alter tables and columns, add/drop keys, etc.). Destructive statements (`DROP SCHEMA`,
`DROP TABLE`, `DROP COLUMN`) are emitted as commented-out SQL by default — pass `--allow-drop` to
make them executable. Treat this as a review-then-run step, not something to pipe straight into
execution, especially with `--allow-drop`. Unsupported changes (e.g. `table_type`) show up as
`-- TODO: unsupported change: ...` comments rather than being silently dropped.

Same `--schema`, `--include-tags`, `--include-metadata` filters apply as above.

## Choosing the right command

| Want to...                                              | Command        |
|-----------------------------------------------------------|----------------|
| See what catalogs/schemas exist                           | `list-catalogs`, `list-schemas` |
| Read/summarize a live schema's structure                  | `extract` (no `--output-dir`, use `--schema`) |
| Snapshot a catalog for version control                    | `extract --output-dir` |
| Check if a catalog drifted from a checked-in snapshot      | `diff` |
| Check if two snapshot directories differ (no live access) | `diff-files` |
| Sanity-check hand-edited YAML/JSON before using it         | `validate` |
| Produce SQL to reconcile live catalog with a snapshot      | `generate-sql` |
