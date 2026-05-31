# CryoliteEngine

**Package**: `io.cryolite`  
**Type**: Main Entry Point  
**Since**: 0.1.0

## Overview

`CryoliteEngine` is the main entry point for the CRYOLITE embedded Iceberg table and query
engine. It exposes three complementary API levels backed by the same Iceberg catalog and
storage configuration:

| API level | Methods | Use case |
|---|---|---|
| **Catalog API** | `getCatalog()`, `isHealthy()` | Namespace / table DDL via the raw Iceberg `Catalog` |
| **Low-Level API** | `append(...)`, `scan(...)` | Direct record writes and Arrow batch reads with optional projection / filter pushdown |
| **SQL API** | `createSqlSession()` | Apache Calcite-backed SQL: `CREATE TABLE`, `INSERT INTO`, `SELECT ... WHERE ...` |

## Purpose

This is an embedded library - no CLI, no server, no REST service. It is designed to be
used directly from Java applications for managing and querying Apache Iceberg tables.

## Key Features

- ✅ **Embedded Library**: Direct Java API, no external processes
- ✅ **Three API levels**: Catalog, low-level (records / Arrow), and SQL
- ✅ **Predicate Pushdown**: Filters and projections pushed down to the Iceberg scan layer when possible (partition / manifest / file pruning)
- ✅ **Vectorized Reads**: Backed by Iceberg's `VectorizedTableScanIterable`, returning Arrow `VectorSchemaRoot` batches
- ✅ **Lifecycle Management**: Clean create/close pattern
- ✅ **Health Checks**: Built-in catalog connectivity verification

## Usage Examples

### Catalog API (DDL via raw Iceberg)

```java
CryoliteEngine engine = new CryoliteEngine(config);
Catalog catalog = engine.getCatalog();

SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
nsCatalog.createNamespace(Namespace.of("my_namespace"), Map.of());

TableIdentifier tableId = TableIdentifier.of("my_namespace", "my_table");
Schema schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get()));
catalog.createTable(tableId, schema);

engine.close();
```

### Low-Level API (records + Arrow batches)

```java
TableIdentifier tableId = TableIdentifier.of("my_ns", "orders");

// Write
GenericRecord r = GenericRecord.create(engine.getCatalog().loadTable(tableId).schema());
r.setField("id", 1L);
r.setField("amount", 99.9);
engine.append(tableId, List.of(r));

// Read with projection + filter pushdown
BatchPredicate filter = new ComparisonPredicate("amount", ComparisonOperator.GT, 50.0);
try (CloseableIterable<VectorSchemaRoot> batches =
        engine.scan(tableId, List.of("id", "amount"), filter)) {
    for (VectorSchemaRoot batch : batches) {
        // process batch (memory valid only until next() or close())
    }
}
```

### SQL API (Calcite)

```java
try (SqlSession session = engine.createSqlSession()) {
    session.execute("CREATE TABLE my_ns.orders (id BIGINT NOT NULL, amount DOUBLE)");
    session.execute("INSERT INTO my_ns.orders VALUES (1, 99.9), (2, 12.5)");

    try (CloseableIterable<VectorSchemaRoot> rows =
            session.query("SELECT * FROM my_ns.orders WHERE amount > 50")) {
        for (VectorSchemaRoot batch : rows) {
            // process batch
        }
    }
}
```

## API Reference

### Constructor

```java
public CryoliteEngine(CryoliteConfig config)
```

Creates a new CryoliteEngine with the given configuration.

**Parameters**:
- `config` - The engine configuration (must not be null)

**Throws**:
- `IllegalArgumentException` - If config is null

---

### Methods

#### `getConfig()`

```java
public CryoliteConfig getConfig()
```

Returns the engine configuration.

**Returns**: The configuration used to create this engine

---

#### `getCatalog()`

```java
public org.apache.iceberg.catalog.Catalog getCatalog()
```

Returns the Iceberg Catalog for table and namespace operations.

The returned catalog can be cast to `SupportsNamespaces` for namespace operations.

**Returns**: The Iceberg Catalog instance

**Throws**:
- `IllegalStateException` - If engine is closed

---

#### `isHealthy()`

```java
public boolean isHealthy()
```

Checks if the engine is healthy (catalog is accessible).

**Returns**: `true` if catalog is healthy, `false` otherwise

---

#### `isClosed()`

```java
public boolean isClosed()
```

Checks if the engine is closed.

**Returns**: `true` if closed, `false` otherwise

---

#### `append(TableIdentifier tableId, List<Record> records)`

```java
public void append(TableIdentifier tableId, List<Record> records) throws IOException
```

Appends a list of records to the specified table as a single atomic Iceberg snapshot.
Internally delegates to [`TableWriter`](../data/TableWriter.md). This is the engine-level
write entry point used by the SQL DML layer.

**Parameters**:
- `tableId` - The target table
- `records` - The records to append; must conform to the table's schema

**Throws**:
- `IllegalStateException` - If the engine is closed
- `IOException` - If writing or committing the snapshot fails

---

#### `scan(TableIdentifier tableId)`

```java
public CloseableIterable<VectorSchemaRoot> scan(TableIdentifier tableId) throws IOException
```

Scans the entire table and returns the data as a stream of Arrow `VectorSchemaRoot`
batches. Memory for each batch is owned by Iceberg's vectorized reader and is freed when
the iterator advances or closes.

**Returns**: A closeable iterable of Arrow batches; empty if the table has no snapshots

**Throws**:
- `IllegalStateException` - If the engine is closed
- `IOException` - If reading fails

---

#### `scan(TableIdentifier tableId, BatchPredicate predicate)`

```java
public CloseableIterable<VectorSchemaRoot> scan(TableIdentifier tableId, BatchPredicate predicate)
    throws IOException
```

Convenience overload of the three-argument `scan` with no column projection. The
predicate is pushed down to the Iceberg scan layer when
[`BatchPredicate#toIcebergExpression`](../filter/README.md) returns a non-empty
expression, and is always applied as a residual Arrow-level filter for row-level
correctness.

---

#### `scan(TableIdentifier tableId, List<String> columns, BatchPredicate predicate)`

```java
public CloseableIterable<VectorSchemaRoot> scan(
    TableIdentifier tableId, List<String> columns, BatchPredicate predicate) throws IOException
```

Scans the table with column projection and filter pushdown.

- The column list is forwarded to `TableScan.select(...)` so the Iceberg reader skips
  non-selected columns at the Parquet level.
- The predicate is pushed down to `TableScan.filter(...)` whenever it is fully covered by
  an Iceberg expression, enabling partition / manifest / file / row-group / page-index /
  bloom-filter pruning depending on table properties (see
  [TableWriter](../data/TableWriter.md) for property propagation).
- The Arrow-level residual filter is **always** applied for row-level correctness.

**Parameters**:
- `tableId` - The target table
- `columns` - Columns to project; `null` or empty means all columns
- `predicate` - The filter predicate; must not be null

**Throws**:
- `IllegalStateException` - If the engine is closed
- `IOException` - If reading fails

---

#### `createSqlSession()`

```java
public SqlSession createSqlSession()
```

Creates a new [`SqlSession`](../sql/README.md) for executing SQL statements (DDL, DML,
SELECT). The session uses this engine for all catalog and data operations. Use a
try-with-resources block to ensure the session is properly closed.

**Returns**: A new `SqlSession`

**Throws**:
- `IllegalStateException` - If the engine is closed

---

#### `close()`

```java
public void close()
```

Closes the engine and releases all resources.

After calling this method, the engine cannot be used anymore. This method is idempotent - calling it multiple times is safe.

## Architecture

```
CryoliteEngine
    ├── CryoliteConfig             ← immutable configuration (catalog + storage options)
    ├── CatalogManager             ← Polaris REST Catalog connection
    │       └── RESTCatalog        ← Apache Iceberg REST Catalog
    │
    ├── append() ── TableWriter    ← writes records → Parquet → Iceberg snapshot
    ├── scan()   ── TableReader    ← reads Parquet → VectorizedTableScanIterable → Arrow batches
    │            └─ TableScanner   ← orchestrates projection + filter pushdown + residual eval
    │
    └── createSqlSession() ── SqlSession
                                ├── SqlDdlInterpreter      ← CREATE TABLE
                                ├── SqlDmlInterpreter      ← INSERT INTO
                                └── SqlQueryInterpreter    ← SELECT (+ optional WHERE)
                                        └── SqlWhereConverter ← Calcite AST → BatchPredicate
```

## Design Decisions

### Why Not AutoCloseable?

The engine does not implement `AutoCloseable` to avoid forcing try-with-resources usage. This is intentional because:

- ✅ Engines are typically long-lived objects
- ✅ Explicit `close()` is clearer for lifecycle management
- ✅ Users can still use try-with-resources if desired (manual pattern)

### Why Expose Iceberg Catalog Directly?

The engine exposes the Iceberg `Catalog` interface directly instead of wrapping it because:

- ✅ **Simplified Architecture**: No unnecessary abstraction layers
- ✅ **Full Iceberg API**: Users get access to all Iceberg features
- ✅ **Educational**: Clear relationship between CRYOLITE and Iceberg
- ✅ **Maintainability**: Less code to maintain, fewer bugs

### Why Separate `execute()` and `query()` on `SqlSession`?

SQL is split into two flavours: statements that produce no result (`execute()` for DDL /
DML) and statements that produce Arrow batches (`query()` for SELECT). Keeping the
signatures separate avoids casting `void` to / from `CloseableIterable` and gives
callers an obvious place to put try-with-resources for memory cleanup.

## Related Components

- **[CryoliteConfig](CryoliteConfig.md)** - Configuration management
- **[CatalogManager](CatalogManager.md)** - Catalog connection management
- **[TableWriter](../data/TableWriter.md)** - Low-level write path used by `append()`
- **[TableReader](../data/TableReader.md)** / **[TableScanner](../data/TableScanner.md)** - Low-level read path used by `scan()`
- **[filter package](../filter/README.md)** - Predicates passed to `scan()`
- **[sql package](../sql/README.md)** - SQL session created by `createSqlSession()`

## See Also

- [Apache Iceberg Catalog API](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/catalog/Catalog.html)
- [Apache Polaris REST Catalog](https://polaris.apache.org/)

