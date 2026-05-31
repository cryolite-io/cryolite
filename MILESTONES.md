# Milestones

This document tracks the development milestones for CRYOLITE. Each milestone delivers a working, tested increment.

## ✅ M0 – Project Foundation

- Maven build with pinned dependencies
- Docker Compose (Polaris + MinIO + PostgreSQL)
- Quality gates: Spotless, JaCoCo (85%), SonarCloud, OWASP Dependency Check
- Git hooks (pre-commit via `make verify-with-quality`, commit-msg via Conventional Commits)
- CI "Fast Lane" via GitHub Actions
- Apache License 2.0

## ✅ M1 – Embedded Skeleton + Configuration

- `CryoliteEngine` – main entry point with lifecycle (create/close)
- `CryoliteConfig` – immutable configuration with Builder pattern
- `CatalogManager` – Polaris REST Catalog connection
- Health checks for Polaris connectivity
- Full integration with Docker Compose services

## ✅ M2 – Low-Level DDL: Namespace + Table Create

- Iceberg `Catalog` as the low-level API (no unnecessary abstractions)
- Polaris handles credential vending for storage access
- Namespace operations via `SupportsNamespaces`
- Table operations via `Catalog`
- Polaris configured with `DROP_WITH_PURGE_ENABLED`
- Makefile for centralized build commands
- OWASP Dependency Check (CVSS ≥ 7.0)
- Dependabot for automated dependency updates

## ✅ M3 – Low-Level DML: Write Path (Insert/Append) + Commit

- `TableWriter` for writing data to Iceberg tables
- Support for partitioned and unpartitioned tables
- Automatic partition routing with `PartitionedFanoutWriter`
- Snapshot creation and commit
- Parquet file format
- S3-compatible storage verification in tests

## ✅ M4 – Low-Level Read: Scan + Arrow Result (Baseline)

- `SchemaConverter` – Iceberg → Arrow schema conversion (14 primitive types)
- `RecordConverter` – Arrow batch → GenericRecord with FieldAccessor caching
- `TableReader` – dual API: `readBatches()` (Arrow) + `readRecords()` (convenience)
- True streaming: batches read lazily, records created one at a time
- Zero-copy batch wrapping via `ColumnarBatch`
- Full write → read-back cycle verified

## ✅ M5 – High-Level SQL: DDL CREATE TABLE

- `SqlSession` – SQL execution entry point via `CryoliteEngine.createSqlSession()`
- Apache Calcite parser with `calcite-server` extension for DDL
- `SqlDdlInterpreter` – maps `CREATE TABLE` to Iceberg Catalog operations
- `CalciteTypeMapper` – Calcite SQL types → Iceberg types
- Automatic namespace creation (CREATE IF NOT EXISTS)
- Case-preserving identifiers (`Casing.UNCHANGED`)

## ✅ M6 – High-Level SQL: DML INSERT (Write)

- `SqlDmlInterpreter` – maps `INSERT INTO ... VALUES` to engine write path
- `SqlLiteralConverter` – type-directed Calcite literal → Java value conversion
- `SqlIdentifiers` – shared utility for SQL identifier resolution
- Layered architecture: SQL → `CryoliteEngine.append()` → `TableWriter`
- Multi-row VALUES, explicit column lists, NULL handling
- 144 tests with 85%+ coverage across all packages

## ✅ M7 – High-Level SQL: SELECT * (Read)

- `CryoliteEngine.scan()` – Engine-level read API (symmetric to `append()`)
- `SqlQueryInterpreter` – interprets `SELECT * FROM ns.table`, delegates to `engine.scan()`
- `SqlSession.query()` – new query method for SELECT statements, separated from `execute()` (DDL/DML)
- Arrow streaming result via `VectorSchemaRoot` batches
- 154 tests with 85%+ coverage across all packages

## ✅ M8 – SQL WHERE: Comparisons + AND (Residual allowed)

- `BatchPredicate` interface: `evaluate(VectorSchemaRoot) → BitSet` (selection vector)
- `ComparisonPredicate` – column-wise evaluation (single-pass per column, cache-friendly)
- `AndPredicate` – bulk `BitSet.and()` instead of per-row short-circuit
- `ArrowBatchFilter` – two-phase: evaluate predicate → column-wise copy of matching rows
- `SqlWhereConverter` – Calcite WHERE AST → `BatchPredicate` tree
- `TableScanner` – orchestrates filtered scans with dedicated memory allocator
- All six comparison operators (=, !=, <, <=, >, >=) and AND conjunctions
- 208 tests with 85%+ coverage across all packages

## ✅ M9 – Pushdown: Projection + Filter AND → Iceberg

- `BatchPredicate.toIcebergExpression()` – default method mapping predicates to Iceberg `Expression`
- `ComparisonPredicate.toIcebergExpression()` – maps each comparison operator to Iceberg expression
- `AndPredicate.toIcebergExpression()` – combines pushable child expressions via `Expressions.and()`
- `TableScanner.scan(columns, predicate)` – pushes projection (`select`) and filter to Iceberg for manifest/file pruning; always applies Arrow residual filter for row-level correctness
- `CryoliteEngine.scan(tableId, columns, predicate)` – new 3-arg overload delegating to `TableScanner`
- `SqlQueryInterpreter` – resolves column list from SELECT AST; uses 3-arg scan overload
- 225 tests with 85%+ coverage across all packages

## Current: M10 – Pushdown: Partition Pruning / Manifest Pruning

### M11 – Pushdown: Parquet Reader Filter
### M11 – Pushdown: Parquet Reader Filter
### M12 – SQL: OR / NOT
### M13 – SQL: IN / NOT IN
### M14 – SQL: BETWEEN / NOT BETWEEN
### M15 – SQL: LIKE
### M16 – SQL: CAST & Type Conversions
### M17 – SQL: NULL Semantics (3-valued logic)
### M18 – SQL: LIMIT / ORDER BY
### M19 – SQL: Aggregations (COUNT, SUM, MIN, MAX)
### M20 – SQL: GROUP BY
### M21 – SQL: INNER JOIN
### M22 – SQL: OUTER JOINs
### M23 – SQL: Functions (string, arithmetic)
### M24 – DDL/DML: ALTER TABLE (Schema Evolution)
### M25 – DML: UPDATE / DELETE / MERGE
### M26 – Iceberg: Time Travel
### M27 – Iceberg: Branches
### M28 – Iceberg: Tags
### M29 – Gandiva: Minimal Happy Path
### M30 – Parallelism & Performance
### M31 – Gatling Performance Suite

