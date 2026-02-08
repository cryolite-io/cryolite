# AbstractIntegrationTest

**Package**: `io.cryolite`  
**Type**: Test Base Class  
**Since**: 0.1.0

## Overview

`AbstractIntegrationTest` is the base class for integration tests that require Docker Compose services (Polaris + MinIO + PostgreSQL). It provides common setup and helper methods for creating test configurations and schemas.

## Purpose

Centralizes common integration test setup and provides reusable helper methods to reduce code duplication across test classes.

## Key Features

- ✅ **Automatic Service Startup**: Ensures Docker Compose services are running
- ✅ **Idempotent Setup**: Services started only once per test run
- ✅ **Helper Methods**: Common test configuration and schema creation
- ✅ **Unique Resource Names**: Timestamp-based suffixes for test isolation

## Usage Example

```java
class MyIntegrationTest extends AbstractIntegrationTest {
    
    @Test
    void testSomething() {
        // Services are already running (ensured by @BeforeAll)
        
        // Create test configuration
        CryoliteConfig config = createTestConfig();
        
        // Create test schema
        Schema schema = createTestSchema();
        
        // Create unique table name
        String tableName = "test_table_" + uniqueSuffix();
        
        // ... perform test ...
    }
}
```

## API Reference

### Setup Methods

#### `ensureServicesRunning()`

```java
@BeforeAll
static void ensureServicesRunning() throws Exception
```

Ensures Docker Compose services are running before any tests execute.

This method is called automatically by JUnit's `@BeforeAll` annotation. It delegates to `TestEnvironment.ensureServicesRunning()`.

**Throws**:
- `Exception` - If services cannot be started or do not become healthy

---

### Helper Methods

#### `createTestConfig()`

```java
protected CryoliteConfig createTestConfig()
```

Creates a standard test configuration for CryoliteEngine.

The configuration includes:
- Polaris REST catalog connection
- MinIO S3-compatible storage
- OAuth credentials from environment
- S3 path-style access enabled

**Returns**: A configured `CryoliteConfig` instance

**Configuration Details**:
```java
CryoliteConfig.Builder()
    .catalogOption("uri", TestConfig.getPolarisUri())
    .catalogOption("credential", "client_id:client_secret")
    .catalogOption("scope", TestConfig.getPolarisScope())
    .catalogOption("warehouse", TestConfig.getPolarisWarehouse())
    .storageOption("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .storageOption("s3.endpoint", TestConfig.getMinioEndpoint())
    .storageOption("s3.access-key-id", TestConfig.getMinioAccessKey())
    .storageOption("s3.secret-access-key", TestConfig.getMinioSecretKey())
    .storageOption("s3.path-style-access", "true")
    .storageOption("client.region", "us-west-2")
    .storageOption("warehouse-path", TestConfig.getMinioWarehousePath())
    .build()
```

---

#### `createTestSchema()`

```java
protected Schema createTestSchema()
```

Creates a standard test schema with `id` (long, required) and `name` (string, optional).

**Returns**: A test schema

**Schema Structure**:
```java
new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get())
)
```

---

#### `uniqueSuffix()`

```java
protected String uniqueSuffix()
```

Generates a unique suffix for test resources based on current timestamp.

Useful for creating unique namespace and table names to avoid conflicts between tests.

**Returns**: A unique suffix string (e.g., "1707398400000")

**Usage Example**:
```java
String namespace = "test_ns_" + uniqueSuffix();
String tableName = "test_table_" + uniqueSuffix();
```

## Design Decisions

### Why Abstract Class Instead of Interface?

An abstract class is used instead of an interface because:

- ✅ **Shared State**: Can have instance fields if needed in the future
- ✅ **Protected Methods**: Helper methods are protected, not public
- ✅ **Inheritance**: Clear "is-a" relationship for integration tests

### Why Package-Private?

The class is package-private (no `public` modifier) because:

- ✅ **Internal Use**: Only used within the test package
- ✅ **Encapsulation**: Not part of the public API
- ✅ **Flexibility**: Can change without affecting external code

### Why Timestamp-Based Suffixes?

Timestamp-based suffixes are used instead of UUIDs because:

- ✅ **Readability**: Easier to read in logs and debugging
- ✅ **Sortability**: Natural chronological ordering
- ✅ **Uniqueness**: Sufficient for test isolation

## Test Isolation

Each test should create its own namespaces and tables using `uniqueSuffix()` to avoid conflicts:

```java
@Test
void testCreateTable() {
    String namespace = "test_ns_" + uniqueSuffix();
    String tableName = "test_table_" + uniqueSuffix();
    
    // Create namespace
    SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
    nsCatalog.createNamespace(Namespace.of(namespace), Map.of());
    
    // Create table
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    catalog.createTable(tableId, createTestSchema());
    
    // ... test ...
}
```

## Environment Requirements

Tests require the following environment variables (typically from `.env` file):

- `POLARIS_URI` - Polaris REST catalog endpoint
- `POLARIS_CLIENT_ID` - OAuth client ID
- `POLARIS_CLIENT_SECRET` - OAuth client secret
- `POLARIS_SCOPE` - OAuth scope
- `POLARIS_WAREHOUSE` - Warehouse name
- `MINIO_ENDPOINT` - MinIO endpoint URL
- `MINIO_ACCESS_KEY` - MinIO access key
- `MINIO_SECRET_KEY` - MinIO secret key
- `MINIO_WAREHOUSE_PATH` - S3 warehouse path

See [TestConfig](TestConfig.md) for details on environment configuration.

## Docker Compose Services

The following services must be running:

1. **PostgreSQL** - Polaris metadata database
2. **Polaris** - REST catalog server
3. **MinIO** - S3-compatible object storage

Services are started automatically by `TestEnvironment.ensureServicesRunning()`.

## Related Components

- **[TestConfig](TestConfig.md)** - Loads environment configuration
- **[TestEnvironment](TestEnvironment.md)** - Manages Docker Compose services
- **[S3StorageTestHelper](S3StorageTestHelper.md)** - Verifies S3 storage in tests

## See Also

- [JUnit 5 Documentation](https://junit.org/junit5/docs/current/user-guide/)
- [Docker Compose](https://docs.docker.com/compose/)

