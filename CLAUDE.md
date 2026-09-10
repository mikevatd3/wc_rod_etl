# WC ROD ETL

ETL pipeline that loads Washington County Register of Deeds (ROD) data into a PostgreSQL database (`ipds` on host `edw`, schema `rod`).

## What it does

Reads raw deed records from CSVs and database tables (listed in `conf/datainventory.csv`), splits them by record type (Documents, Parties, Properties), loads them into `rod.*` tables, then runs deduplication and entity-name clustering.

## Source layouts

Two source layouts are supported, declared per source in the `layout` column of `conf/datainventory.csv`. A missing column or blank cell means `v1`.

- **`v1`** — the original headerless 11-column export. Column 4 holds a record-type code (`D`, `N`, `L`, `L(condo)`, …) and `conf/rownames.csv` maps that code to the names of the remaining columns.
- **`v2`** — the newer 19-column export. It *has* a header row and *no* record-type column: the document-level fields repeat on every row and the record type is implicit in which columns are populated (`PARTY_NAME` = grantor, `PARTY_NAME2` = grantee, `TAX_ID1`/`ADDRESS` = property). `PLAT_LIBER`/`PLAT_PAGE`/`LOT` become `property_details` rows of type `platted`. `LIBER` is a composite `liber:page`, `ADDRESS` combines street number and name, and `DM_INSTRUMENT` is the document date despite its name. The `Textbox*` columns are report-generator labels and carry no data.

v1 is read positionally; **v2 is read by column name** from the file's own header (`transforms.py:read_v2`), so a v2 export can gain, lose or reorder columns and still load — extra columns are ignored, the optional `Textbox*` labels are filled in when absent, and a genuinely missing data column raises an error naming it. Selecting by name also absorbs the stray extra fields these exports carry on a handful of rows.

`transforms.py:normalize_v2` then reshapes a v2 frame into the v1 positional intermediate, so both layouts land in the same `rod.*` tables with the same column names and nothing in `sql/` has to change. `layout` is orthogonal to `is_file`, so a v2 source staged as a `raw.*` table works too.

Sample: `rod_new_schema_example.csv`.

## Key files

- **`main.py`** — Full pipeline entry point. Loads sources, writes to Postgres, runs SQL scripts, parses party names via `entity_analyze`, and clusters them.
- **`transforms.py`** — Source-layout handling: the v2 → v1 reshape, the record-type split, `clean_parcel_id`, and `validate_v2`. Imports only pandas, so it is testable with no database and no `entity_analyze`. Run `python transforms.py <file.csv>` for a dry run of a v2 file.
- **`hard_deduplication.py`** — Standalone script for re-running the advanced name clustering step against already-loaded data.
- **`conf/rownames.csv`** — Maps record-type codes (D, N, L, etc.) to column names.
- **`conf/datainventory.csv`** — Lists all source files/tables to ingest, with the `layout` of each. Row order matters: the first source to write a table replaces it, the rest append.
- **`conf/document_type_reference.xlsx`** — Lookup table for document type codes.
- **`sql/`** — Ordered SQL scripts for post-load processing (dedup, indexing, views).

## Pipeline stages (main.py)

1. Load each source from the inventory (reshaping v2 sources first); split rows by type code into `rod.documents`, `rod.parties`, `rod.properties`, and `rod.property_details`. The first write to each table replaces it, the rest append.
2. Deduplicate all three core tables (`0001_deduplication_queries.sql`).
3. Add `naive_name_dedupe` IDs to parties (`0003_create_name_links.sql`).
4. Parse party names with `entity_analyze.breakdown_entity_name` and write `rod.parsed_party_name`.
5. Cluster similar names with `entity_analyze.graph_cluster` and write `rod.party_name_clusters`.
6. Load document type reference into `rod.document_types`.

Every run is a full reload — all `rod` tables are dropped and rebuilt from source.

## SQL scripts not called by main.py

These exist for manual/ad-hoc use:

- `0000_cleanup.sql` — Drops all `rod` tables (manual reset).
- `0002_create_grantor_grantee_tables.sql` — Splits parties into `rod.grantors` / `rod.grantees`.
- `0004_create_indexes_for_core_dedupe.sql` — Trigram + prefix indexes on `parsed_party_name.core`.
- `0005_create_parties_complete.sql` — Materialized view joining parties, clusters, and classifications.
- `0006_create_make_idxes.sql` — Indexes on properties, documents, and assessors tables.

## Dependencies

- Python 3.12+, managed with `uv`
- `entity_analyze` — local sibling package at `../entity_analyze` (editable install)
- PostgreSQL with `pg_trgm` extension (for trigram indexes)
- Key Python libs: pandas, sqlalchemy, psycopg, openpyxl, geopandas

## Running

```
uv run python main.py
```

Tests cover `transforms.py` only and need nothing but pandas:

```
uv run pytest
```
