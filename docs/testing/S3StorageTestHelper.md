# S3StorageTestHelper

**Package**: `io.cryolite.test`  
**Type**: Test Helper Class  
**Since**: M3 (Low-Level DML: Write Path)

## Overview

`S3StorageTestHelper` is a test utility class that provides methods to verify that data files are correctly written to S3-compatible storage (e.g., MinIO, AWS S3). It works with any S3-compatible endpoint configured in `CryoliteConfig`.

## Purpose

This helper class enables integration tests to verify not only Iceberg metadata (snapshots, manifests) but also the actual physical data files in object storage. This ensures end-to-end correctness of the write path.

## Key Features

- ✅ **Generic S3 Compatibility**: Works with MinIO, AWS S3, or any S3-compatible storage
- ✅ **File Existence Checks**: Verify that data files exist in storage
- ✅ **File Size Verification**: Ensure files have content (size > 0)
- ✅ **Configuration-Driven**: Uses `CryoliteConfig` for credentials and endpoint
- ✅ **Resource Management**: Implements `close()` for proper cleanup

## Usage Example

```java
@Test
void testWriteToTable() throws IOException {
  CryoliteEngine engine = new CryoliteEngine(createTestConfig());
  Catalog catalog = engine.getCatalog();
  
  // ... create table and write data ...
  
  try (TableWriter writer = new TableWriter(table)) {
    // Write records
    writer.write(record);
    writer.commit();
    
    // Verify file exists in S3 storage
    S3StorageTestHelper s3Helper = new S3StorageTestHelper(createTestConfig());
    DataFile dataFile = writer.getCommittedFiles().get(0);
    String filePath = dataFile.location();
    
    assertTrue(
        s3Helper.fileExists(filePath),
        "Data file should exist in S3 storage: " + filePath);
    
    long fileSize = s3Helper.getFileSize(filePath);
    assertTrue(fileSize > 0, "Data file should have content");
    
    s3Helper.close();
  }
}
```

## API Reference

### Constructor

```java
public S3StorageTestHelper(CryoliteConfig config)
```

Creates a new S3StorageTestHelper instance.

**Parameters**:
- `config` - The test configuration containing S3-compatible storage credentials and endpoint

**Configuration Requirements**:
- `s3.endpoint` - S3-compatible endpoint URL (e.g., `http://localhost:9000`)
- `s3.access-key-id` - Access key for authentication
- `s3.secret-access-key` - Secret key for authentication
- `warehouse-path` - S3 warehouse path (e.g., `s3://warehouse/path`)

### Methods

#### `fileExists(String filePath)`

Checks if a file exists in S3-compatible storage.

**Parameters**:
- `filePath` - The S3 path to the file (e.g., `"s3://bucket/path/to/file.parquet"`)

**Returns**: `true` if the file exists, `false` otherwise

**Throws**: `RuntimeException` if an error occurs during the check

---

#### `getFileSize(String filePath)`

Gets the size of a file in S3-compatible storage.

**Parameters**:
- `filePath` - The S3 path to the file

**Returns**: The file size in bytes

**Throws**: `RuntimeException` if the file does not exist or an error occurs

---

#### `close()`

Closes the S3 client and releases resources.

**Throws**: `IOException` if an error occurs during cleanup

## Implementation Details

### S3 Client Configuration

The helper creates an AWS SDK `S3Client` with the following configuration:

- **Region**: `US_WEST_2` (required by SDK, but not used for MinIO)
- **Endpoint Override**: Uses the endpoint from `CryoliteConfig`
- **Credentials**: Static credentials from `CryoliteConfig`
- **Path Style Access**: Enabled (`forcePathStyle(true)`) for MinIO compatibility

### Path Handling

The helper extracts bucket and key information from S3 paths:

- **Bucket**: Extracted from `warehouse-path` in config (e.g., `s3://warehouse/...` → `warehouse`)
- **Key**: Extracted from file path (e.g., `s3://bucket/path/to/file.parquet` → `path/to/file.parquet`)

### Error Handling

- **File Not Found**: `fileExists()` returns `false` (does not throw)
- **File Not Found**: `getFileSize()` throws `RuntimeException` with clear message
- **Other Errors**: Both methods throw `RuntimeException` with descriptive messages

## Testing Strategy

This helper is used in integration tests to verify:

1. **Non-Partitioned Tables**: Single data file exists and has content
2. **Partitioned Tables**: All partition files exist and have content
3. **Multiple Writes**: Each write creates new files in storage

## Design Decisions

### Why Not MinIO-Specific?

The class is intentionally generic (`S3StorageTestHelper` instead of `MinIOTestHelper`) because:

- ✅ Works with any S3-compatible storage (MinIO, AWS S3, etc.)
- ✅ Future-proof for production AWS S3 usage
- ✅ Follows the principle of abstraction over implementation

### Why Not Use Iceberg FileIO?

We use AWS SDK directly instead of Iceberg's `FileIO` because:

- ✅ Tests should verify the complete stack, including storage layer
- ✅ Iceberg `FileIO` is already tested by Iceberg project
- ✅ Direct S3 access provides independent verification

## Related Components

- **[TableWriter](../api/data/TableWriter.md)** - Writes data files that this helper verifies
- **[AbstractIntegrationTest](AbstractIntegrationTest.md)** - Base class that uses this helper
- **[TestConfig](TestConfig.md)** - Provides configuration for S3 connection

## See Also

- [Data Write Operations Tests](../../src/test/java/io/cryolite/DataWriteOperationsTest.java)
- [AWS SDK S3 Documentation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3.html)
- [MinIO Documentation](https://min.io/docs/minio/linux/developers/java/minio-java.html)

