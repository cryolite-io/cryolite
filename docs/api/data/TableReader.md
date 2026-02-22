# TableReader

Low-level API for reading data from an Apache Iceberg table using vectorized Arrow batches.

## Overview

`TableReader` provides two APIs on top of the same Arrow batch pipeline:

| API | Return type | Use case |
|-----|-------------|----------|
| `readBatches()` | `CloseableIterable<VectorSchemaRoot>` | Vectorized processing, SQL execution, Gandiva expressions |
| `readRecords()` | `CloseableIterable<Record>` | Convenience row access |

**Both APIs stream data lazily.** Batches are read from Parquet one at a time; no data is loaded upfront.

## Internal Pipeline

```
Parquet files
     │
     ▼
VectorizedTableScanIterable          ← Iceberg's vectorized reader
     │  one ColumnarBatch at a time
     ▼
ColumnarBatch::createVectorSchemaRootFromVectors   ← zero-copy wrap
     │
     ▼
CloseableIterable<VectorSchemaRoot>  ← readBatches() result
     │
     │  (readRecords() only)
     ▼
BatchToRecordIterator
  ├── buildAccessors()  once per batch   ← resolves vector references + types
  └── convertRow()      once per next()  ← creates exactly one GenericRecord
     │
     ▼
CloseableIterable<Record>            ← readRecords() result
```

## Memory Contract

Each `VectorSchemaRoot` is valid **only during the current iteration step**. The Arrow memory is owned by Iceberg's `VectorizedTableScanIterable` and is freed when the iterator advances or closes.

```java
// CORRECT
try (var batches = reader.readBatches()) {
    for (VectorSchemaRoot batch : batches) {
        process(batch); // batch is valid here only
    }
}

// WRONG - do not store batch references for later
List<VectorSchemaRoot> stored = new ArrayList<>();
for (VectorSchemaRoot batch : batches) {
    stored.add(batch); // batch memory may already be freed
}
```

## Usage

### Arrow API (primary)

```java
Table table = catalog.loadTable(TableIdentifier.of("ns", "events"));
TableReader reader = new TableReader(table);

try (CloseableIterable<VectorSchemaRoot> batches = reader.readBatches()) {
    for (VectorSchemaRoot batch : batches) {
        System.out.printf("Batch: %d rows, schema: %s%n",
            batch.getRowCount(), batch.getSchema());
    }
}
```

### Record API (convenience)

```java
try (CloseableIterable<Record> records = reader.readRecords()) {
    for (Record record : records) {
        System.out.println(record.getField("id"));
    }
}
```

### Custom scan (filters, projections, snapshots)

```java
TableScan scan = table.newScan()
    .filter(Expressions.equal("status", "active"))
    .select("id", "name", "status");

try (CloseableIterable<VectorSchemaRoot> batches = reader.readBatches(scan)) {
    for (VectorSchemaRoot batch : batches) {
        // only projected columns, only matching rows
    }
}
```

## See Also

- [`SchemaConverter`](../arrow/SchemaConverter.md) - Iceberg → Arrow schema conversion
- [`RecordConverter`](../arrow/RecordConverter.md) - Arrow batch → GenericRecord conversion
- [`TableWriter`](TableWriter.md) - Write data to Iceberg tables

