# TableWriter

**Package**: `io.cryolite.data`  
**Type**: Data Write API  
**Since**: 0.1.0

## Overview

`TableWriter` is a low-level writer for appending data to Iceberg tables. It encapsulates the complexity of writing data using the native Iceberg Data API, supporting both partitioned and non-partitioned tables.

## Purpose

Provides a simple, high-level API for writing data to Iceberg tables while handling:

- Parquet file creation
- Partition routing (for partitioned tables)
- Snapshot creation and commit
- Resource management

## Key Features

- ✅ **Partitioned & Non-Partitioned Tables**: Automatic detection and handling
- ✅ **Parquet Format**: Efficient columnar storage
- ✅ **Automatic Partition Routing**: Records routed to correct partitions
- ✅ **Snapshot Management**: Atomic commits
- ✅ **Resource Management**: Implements `Closeable` for try-with-resources
- ✅ **Configurable File Size**: Customizable target file size
- ✅ **Table Property Propagation**: All `write.parquet.*` properties configured on the Iceberg table are passed through to the underlying Parquet writer (since M11.5)

## Usage Example

### Non-Partitioned Table

```java
Table table = catalog.loadTable(TableIdentifier.of("my_namespace", "my_table"));

try (TableWriter writer = new TableWriter(table)) {
    // Create records
    GenericRecord record1 = GenericRecord.create(table.schema());
    record1.setField("id", 1L);
    record1.setField("name", "Alice");
    
    GenericRecord record2 = GenericRecord.create(table.schema());
    record2.setField("id", 2L);
    record2.setField("name", "Bob");
    
    // Write records
    writer.write(record1);
    writer.write(record2);
    
    // Commit creates a new snapshot
    writer.commit();
}
```

### Partitioned Table

```java
// Create partitioned table
PartitionSpec spec = PartitionSpec.builderFor(schema)
    .day("timestamp")
    .build();
Table table = catalog.createTable(tableId, schema, spec);

try (TableWriter writer = new TableWriter(table)) {
    GenericRecord record = GenericRecord.create(table.schema());
    record.setField("id", 1L);
    record.setField("timestamp", OffsetDateTime.now());
    record.setField("data", "value");
    
    // Writer automatically routes to correct partition
    writer.write(record);
    
    writer.commit();
}
```

### Custom File Size

```java
// Use 256 MB target file size instead of default 128 MB
long targetFileSize = 256L * 1024 * 1024;
try (TableWriter writer = new TableWriter(table, targetFileSize)) {
    // ... write records ...
    writer.commit();
}
```

### Parquet Tuning via Table Properties

Since M11.5, `TableWriter` forwards all table properties to the underlying
`GenericAppenderFactory`. This means every Parquet-level knob exposed by Iceberg's
`TableProperties` is honoured at write time:

```java
// Configure the table once - subsequent writes pick it up automatically
table.updateProperties()
    .set("write.parquet.row-group-size-bytes", "4096")        // tiny row groups
    .set("write.parquet.page-size-bytes",      "1024")        // tiny pages
    .set("write.parquet.bloom-filter-enabled.column.id", "true")  // bloom filter on `id`
    .commit();

try (TableWriter writer = new TableWriter(table)) {
    writer.write(record);
    writer.commit();
}
```

Commonly used properties:

| Property | Purpose |
|---|---|
| `write.parquet.row-group-size-bytes` | Target row-group size (default 128 MB) – smaller values give finer-grained row-group pruning |
| `write.parquet.page-size-bytes` | Target data-page size (default 1 MB) – drives the `OffsetIndex` granularity used by Parquet page-level pruning |
| `write.parquet.dict-size-bytes` | Target dictionary size per row group |
| `write.parquet.compression-codec` | Compression algorithm (e.g. `zstd`, `snappy`) |
| `write.parquet.bloom-filter-enabled.column.<name>` | Enables a Bloom Filter for the given column, used by point lookups |

These properties are part of the foundation that lets Iceberg + Parquet apply
file, row-group, page, and bloom-filter pruning during reads. See the
[Deep Pruning Verification milestone](../../../MILESTONES.md#-m115--deep-pruning-verification-combined--rowgroup--page-index--bloom-filter)
for a discussion of the pruning hierarchy.

## API Reference

### Constructors

#### `TableWriter(Table table)`

```java
public TableWriter(Table table)
```

Creates a new TableWriter with default target file size (128 MB).

**Parameters**:
- `table` - The Iceberg table to write to

**Throws**:
- `IllegalArgumentException` - If table is null

---

#### `TableWriter(Table table, long targetFileSizeBytes)`

```java
public TableWriter(Table table, long targetFileSizeBytes)
```

Creates a new TableWriter with custom target file size.

**Parameters**:
- `table` - The Iceberg table to write to
- `targetFileSizeBytes` - Target size for data files in bytes

**Throws**:
- `IllegalArgumentException` - If table is null or targetFileSizeBytes is not positive

---

### Methods

#### `write(Record record)`

```java
public void write(Record record) throws IOException
```

Writes a record to the table.

The record is buffered and will be written to a Parquet file. For partitioned tables, the record is automatically routed to the correct partition.

**Parameters**:
- `record` - The record to write

**Throws**:
- `IOException` - If an I/O error occurs
- `IllegalStateException` - If the writer is closed

---

#### `commit()`

```java
public void commit() throws IOException
```

Commits all written records as a new snapshot.

This method closes any open data files and appends them to the table as a new snapshot. The commit is atomic - either all files are committed or none.

**Throws**:
- `IOException` - If an I/O error occurs
- `IllegalStateException` - If the writer is closed

**Important**: After calling `commit()`, the writer is closed and cannot be used again.

---

#### `getCommittedFiles()`

```java
public List<DataFile> getCommittedFiles()
```

Returns the data files written by this writer.

This method returns the list of data files that were committed. It can be used to inspect what files were created during the write operation.

**Returns**: List of committed data files (may be empty if no data was written)

---

#### `getTable()`

```java
public Table getTable()
```

Returns the table this writer is writing to.

**Returns**: The Iceberg table

---

#### `isClosed()`

```java
public boolean isClosed()
```

Checks if this writer is closed.

**Returns**: `true` if closed, `false` otherwise

---

#### `close()`

```java
public void close()
```

Closes this writer without committing.

Any written but uncommitted data will be lost. Use `commit()` to persist data before closing, or use try-with-resources and call commit within the try block.

## Implementation Details

### Partition Detection

The writer automatically detects if the table is partitioned:

- **Unpartitioned**: Uses `UnpartitionedWriter` with `null` partition key
- **Partitioned**: Uses `PartitionedFanoutWriter` with automatic routing

### Partition Routing

For partitioned tables, the writer uses `InternalRecordWrapper` to handle type conversions (e.g., `OffsetDateTime` → `long` micros for timestamps) before computing the partition key.

### File Naming

Files are named using UUID-based operation IDs to avoid hostname issues and ensure uniqueness across different environments.

### MinIO Compatibility

The writer uses `UnpartitionedWriter` for non-partitioned tables to avoid double-slash path issues that MinIO and other S3-compatible stores reject.

## Design Decisions

### Why Not Reusable After Commit?

The writer is closed after `commit()` because:

- ✅ **Simplicity**: Clear lifecycle (create → write → commit → close)
- ✅ **Safety**: Prevents accidental writes to wrong snapshot
- ✅ **Resource Management**: Ensures proper cleanup

### Why Closeable Instead of AutoCloseable?

`Closeable` is used instead of `AutoCloseable` because:

- ✅ **IOException**: `Closeable.close()` throws `IOException`, which is appropriate for I/O operations
- ✅ **Compatibility**: `Closeable` extends `AutoCloseable`, so try-with-resources still works

### Why Expose DataFiles?

`getCommittedFiles()` exposes the committed data files for:

- ✅ **Testing**: Verify correct files were created
- ✅ **Debugging**: Inspect file locations and sizes
- ✅ **Monitoring**: Track write operations

## Error Handling

- **Write Errors**: Throws `IOException` with underlying cause
- **Commit Errors**: Throws `IOException`, writer is closed
- **Close Errors**: Swallowed (logged internally)

## Performance Considerations

- **Target File Size**: Default 128 MB balances parallelism and overhead
- **Buffering**: Records are buffered in memory before writing
- **Partition Fanout**: Each partition gets its own writer

## Related Components

- **[S3StorageTestHelper](../../testing/S3StorageTestHelper.md)** - Verifies written files in tests

## See Also

- [Apache Iceberg Data API](https://iceberg.apache.org/docs/latest/api/)
- [Parquet Format](https://parquet.apache.org/)

