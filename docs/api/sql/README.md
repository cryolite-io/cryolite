# sql package

**Package**: `io.cryolite.sql` (and subpackages)
**Since**: 0.1.0 (M5 – CREATE TABLE, M6 – INSERT INTO, M7 – SELECT, M8/M9 – WHERE + pushdown)

The SQL package is the high-level API of CRYOLITE. It parses SQL strings with Apache
Calcite and dispatches the resulting AST nodes to interpreter classes that delegate to
the engine's low-level API. The SQL layer never owns I/O primitives directly – every
read or write goes through [`CryoliteEngine`](../core/CryoliteEngine.md).

## Component Overview

```
SqlSession
   ├── parse(sql)                 ← Calcite (SqlDdlParserImpl, Casing.UNCHANGED)
   └── dispatch(SqlNode)
         ├── SqlCreateTable  →  SqlDdlInterpreter        ← M5
         ├── SqlInsert       →  SqlDmlInterpreter        ← M6
         └── SqlSelect       →  SqlQueryInterpreter      ← M7 / M8 / M9
                                    └── SqlWhereConverter   ← Calcite WHERE → BatchPredicate
                                            └── SqlLiteralConverter (per-column types)
```

Shared helpers:

| Class | Purpose |
|---|---|
| `SqlIdentifiers` | Resolves Calcite `SqlIdentifier` → fully-qualified Iceberg `TableIdentifier`, requires `namespace.table` form |
| `CalciteTypeMapper` | Maps Calcite SQL types → Iceberg types (used by DDL) |
| `SqlLiteralConverter` | Type-directed conversion of Calcite literals → Java values that match the target Iceberg column type |
| `SqlExecutionException` | Wrapping exception for parse / unsupported / execution failures |

## SqlSession

Public entry point, obtained via [`CryoliteEngine.createSqlSession()`](../core/CryoliteEngine.md).
`SqlSession implements AutoCloseable`.

```java
try (SqlSession session = engine.createSqlSession()) {
    session.execute("CREATE TABLE ns.t (id BIGINT NOT NULL, name VARCHAR)");
    session.execute("INSERT INTO ns.t VALUES (1, 'a'), (2, 'b')");
    try (CloseableIterable<VectorSchemaRoot> rows =
            session.query("SELECT * FROM ns.t WHERE id > 1")) {
        for (VectorSchemaRoot batch : rows) {
            // ...
        }
    }
}
```

| Method | Use case |
|---|---|
| `execute(String sql)` | Statements that produce no result (DDL, DML). Throws on SELECT. |
| `query(String sql)` | SELECT statements; returns a closeable iterable of Arrow batches. Throws on DDL / DML. |

Parser configuration:

- Uses `SqlDdlParserImpl.FACTORY` (from `calcite-server`) so `CREATE TABLE` and similar
  DDL statements are recognised alongside standard SQL.
- `Casing.UNCHANGED` for quoted **and** unquoted identifiers so that
  `my_namespace.my_table` is not silently upper-cased to `MY_NAMESPACE.MY_TABLE`.

## SqlDdlInterpreter (M5)

Maps Calcite `SqlCreateTable` to Iceberg catalog operations:

- Resolves fully-qualified table identifiers via `SqlIdentifiers`.
- Creates the namespace on demand if the catalog supports it (`SupportsNamespaces`).
- Builds the Iceberg `Schema` from Calcite column declarations via `CalciteTypeMapper`.
- Honours `CREATE TABLE IF NOT EXISTS` by swallowing `AlreadyExistsException`.
- Requires fully-qualified `namespace.table` names; single-part names are rejected for
  consistency with Iceberg's namespace isolation.

Supported:
```
CREATE TABLE [IF NOT EXISTS] namespace.table (
    col1 TYPE [NOT NULL],
    col2 TYPE,
    ...
)
```

## SqlDmlInterpreter (M6)

Maps Calcite `SqlInsert` (`INSERT INTO ... VALUES (...)`) to
[`CryoliteEngine.append(...)`](../core/CryoliteEngine.md):

- Each `VALUES` row becomes one `GenericRecord`.
- Optional explicit column lists are supported; missing columns become `null`.
- Literal values are converted to the target column's Iceberg type via
  `SqlLiteralConverter`.
- The whole INSERT commits as a single atomic Iceberg snapshot.

Supported:
```
INSERT INTO namespace.table [(col1, col2, ...)] VALUES (v1, v2, ...) [, (v1, v2, ...)]
```

## SqlQueryInterpreter (M7 + M8 + M9)

Maps Calcite `SqlSelect` to [`CryoliteEngine.scan(...)`](../core/CryoliteEngine.md):

- Resolves the SELECT list to a column projection (or empty list for `SELECT *`).
- If a `WHERE` clause is present, delegates to `SqlWhereConverter` to obtain a
  [`BatchPredicate`](../filter/README.md).
- Calls the three-arg `engine.scan(tableId, columns, predicate)` so both projection and
  pushable filter parts reach the Iceberg scan layer.

Supported:
```
SELECT *           FROM namespace.table
SELECT col1, col2  FROM namespace.table
SELECT * FROM namespace.table WHERE col = value
SELECT * FROM namespace.table WHERE col1 > 10 AND col2 = 'x'
```

## SqlWhereConverter (M8)

Converts a Calcite `WHERE` AST into a `BatchPredicate` tree:

- Comparison nodes (`=`, `<>`, `<`, `<=`, `>`, `>=`) → `ComparisonPredicate`.
- `AND` nodes → `AndPredicate` over the converted children.
- Literal values are converted using the target column's Iceberg type via
  `SqlLiteralConverter`, so e.g. a numeric literal compared against a `BIGINT` column
  becomes a `Long`, not an `Integer`.
- Anything else throws `SqlExecutionException` (e.g. `OR`, `NOT`, `IN`, function calls –
  staged for M12+).

## Type Mapping

| Helper | Direction | Used by |
|---|---|---|
| `CalciteTypeMapper` | Calcite SQL type → Iceberg type | `SqlDdlInterpreter` |
| `SqlLiteralConverter` | Calcite literal + Iceberg type → Java value | `SqlDmlInterpreter`, `SqlWhereConverter` |

Both layers are deliberately small and isolated so future SQL features (`CAST`,
`BETWEEN`, decimals, timestamps) can be added without touching the interpreters.

## Error Handling

`SqlExecutionException` is the single public exception type:

- Wraps `SqlParseException` from Calcite.
- Used for unsupported statement kinds, unsupported predicates, missing namespaces, and
  schema mismatches.
- Always includes the original SQL statement in the message where helpful.

## Related Components

- **[CryoliteEngine](../core/CryoliteEngine.md)** – exposes `createSqlSession()` and the
  low-level `append` / `scan` operations the interpreters call into
- **[TableWriter](../data/TableWriter.md)** / **[TableScanner](../data/TableScanner.md)** – I/O layers below the engine
- **[filter package](../filter/README.md)** – `BatchPredicate` model produced by `SqlWhereConverter`

## See Also

- [Apache Calcite SQL Parser](https://calcite.apache.org/docs/reference.html)
- [Iceberg Catalog API](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/catalog/Catalog.html)
