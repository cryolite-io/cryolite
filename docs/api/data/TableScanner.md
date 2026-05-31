# TableScanner

Orchestrates filtered table reads by combining a [`TableReader`](TableReader.md) with predicate
pushdown and Arrow-level residual filtering.

## Overview

`TableScanner` sits between the engine and the low-level reader / writer:

| Component | Responsibility |
|---|---|
| [`TableReader`](TableReader.md) | Pure I/O – Iceberg scan → Arrow batches |
| [`TableWriter`](TableWriter.md) | Pure I/O – records → Iceberg snapshot |
| **`TableScanner`** | Orchestrates configure-scan → read → residual filter → memory management |

It is the implementation behind [`CryoliteEngine.scan(...)`](../core/CryoliteEngine.md) and is
not normally instantiated directly by application code.

## Pushdown Semantics

When the caller passes a [`BatchPredicate`](../filter/README.md), the scanner asks the
predicate for its Iceberg-pushable equivalent via `toIcebergExpression()`:

```
TableScanner.scan(columns, predicate)
        │
        ├── TableScan = table.newScan()
        ├── if columns present  →  TableScan = TableScan.select(columns)
        ├── if predicate.toIcebergExpression() present
        │                       →  TableScan = TableScan.filter(expression)
        │                          (enables partition / manifest / file / row-group pruning)
        │
        ├── TableReader.readBatches(TableScan)   ← I/O happens here
        │
        └── FilteredBatchIterable                 ← residual Arrow filter
                ├── ArrowBatchFilter(predicate)
                └── owns BufferAllocator
```

The Arrow-level residual filter is **always** applied for row-level correctness because
Iceberg's vectorized reader only prunes at the file / block level – it does not evaluate
predicates row by row.

## API

### `scan()`

```java
public CloseableIterable<VectorSchemaRoot> scan() throws IOException
```

Reads every row of every column; no projection or filtering.

### `scan(BatchPredicate predicate)`

```java
public CloseableIterable<VectorSchemaRoot> scan(BatchPredicate predicate) throws IOException
```

Convenience overload for `scan(null, predicate)` – no projection, predicate is pushed
down when possible.

### `scan(Collection<String> columns, BatchPredicate predicate)`

```java
public CloseableIterable<VectorSchemaRoot> scan(
    Collection<String> columns, BatchPredicate predicate) throws IOException
```

Full form: projection + filter pushdown + residual evaluation.

| Parameter | Description |
|---|---|
| `columns` | Column projection list; `null` or empty means "all columns" |
| `predicate` | Row predicate; must not be `null`. The pushable part goes into the Iceberg scan; the full predicate is also applied as an Arrow residual filter |

## Memory Lifecycle

Each filtered `VectorSchemaRoot` returned by the iterator is valid only until the next
call to `next()` or until the iterable is closed. This mirrors the contract of
[`TableReader`](TableReader.md). The scanner owns a dedicated `RootAllocator` for the
filtered batches and closes it as part of the iterable's `close()`.

```java
// CORRECT
try (var batches = engine.scan(tableId, columns, predicate)) {
    for (VectorSchemaRoot batch : batches) {
        process(batch); // valid here only
    }
}
```

## Usage Example

```java
// Project two columns and filter on a third
BatchPredicate filter = new ComparisonPredicate("region", ComparisonOperator.EQUALS, "EU");
try (CloseableIterable<VectorSchemaRoot> batches =
        engine.scan(tableId, List.of("id", "amount"), filter)) {
    for (VectorSchemaRoot batch : batches) {
        // batch only contains rows where region = 'EU'
        // and only the columns id and amount
    }
}
```

## Design Decisions

### Why a dedicated allocator per scan?

Each scan owns its own `RootAllocator` for filtered batches. This isolates memory
accounting per query and lets the allocator be closed deterministically when the
iterable is closed, matching the Arrow lifecycle contract.

### Why always run the residual filter, even when pushdown is full?

Pushdown to the Iceberg scan is an **I/O** optimisation: it prunes files, manifests, and
row groups. It does not eliminate individual rows. To guarantee correctness without
adding complex coverage-tracking logic, the Arrow-level filter runs unconditionally.
When pushdown removes the matching files entirely, the residual filter simply iterates
over an empty stream – there is no measurable cost.

## Related Components

- **[TableReader](TableReader.md)** – the I/O layer used internally
- **[TableWriter](TableWriter.md)** – symmetric write path (Parquet property propagation)
- **[filter package](../filter/README.md)** – `BatchPredicate`, `ArrowBatchFilter`
- **[CryoliteEngine](../core/CryoliteEngine.md)** – public entry point that exposes `scan(...)`

## See Also

- [Iceberg TableScan](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/TableScan.html)
- [Apache Arrow Java](https://arrow.apache.org/docs/java/)
