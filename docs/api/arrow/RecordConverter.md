# RecordConverter

Converts Apache Arrow batch data to Apache Iceberg `GenericRecord` objects with lazy,
row-by-row extraction and batch-level accessor caching.

## Overview

`RecordConverter` is the bridge between Arrow's columnar batch format (`VectorSchemaRoot`)
and Iceberg's row format (`GenericRecord`). It is used internally by `TableReader.readRecords()`
but can also be used directly for custom processing.

## FieldAccessor Pattern

### The problem

A `VectorSchemaRoot` stores column vectors in an internal `HashMap<String, FieldVector>`.
A naive per-row approach (`root.getVector("id")` per row) causes O(rows × fields) HashMap
lookups:

```
Row 0: getVector("id") + getVector("name") + ...  → 3 lookups
Row 1: getVector("id") + getVector("name") + ...  → 3 lookups
...
Row 4999:                                           → 3 lookups
                                          TOTAL: 15 000 lookups
```

The vectors do not change within a batch, so this is wasted work.

### The solution

`FieldAccessor` is a small record that caches the resolved `FieldVector` reference plus
the Iceberg `PrimitiveType` once per batch. Lookups drop to O(fields) per batch:

```java
public record FieldAccessor(FieldVector vector, String name, Type.PrimitiveType type) {}
```

Usage:
```
Batch arrives → buildAccessors()  →  List<FieldAccessor> (3 lookups, once)
next() call   → convertRow(accessors, 0)  →  one GenericRecord  (0 HashMap lookups)
next() call   → convertRow(accessors, 1)  →  one GenericRecord
...
Next batch    → buildAccessors()  →  List<FieldAccessor> refreshed
```

## API

### `buildAccessors(Schema icebergSchema, VectorSchemaRoot root)`

Resolves all field vectors from the root and returns a `List<FieldAccessor>`.
Call once when a new batch arrives.

### `convertRow(Schema icebergSchema, List<FieldAccessor> accessors, int rowIndex)`

Extracts values for a single row using the cached accessors.
Call once per `next()` invocation.

## Usage

```java
try (CloseableIterable<VectorSchemaRoot> batches = reader.readBatches()) {
    for (VectorSchemaRoot batch : batches) {
        // Cache vector references once per batch
        List<FieldAccessor> accessors = RecordConverter.buildAccessors(schema, batch);

        // Extract one record per row — no HashMap lookups
        for (int row = 0; row < batch.getRowCount(); row++) {
            GenericRecord record = RecordConverter.convertRow(schema, accessors, row);
            process(record);
        }
    }
}
```

## See Also

- [`SchemaConverter`](SchemaConverter.md) - Iceberg → Arrow schema conversion
- [`TableReader`](../data/TableReader.md) - Uses `RecordConverter` in `BatchToRecordIterator`

