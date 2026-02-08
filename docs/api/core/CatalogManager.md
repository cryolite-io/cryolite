# CatalogManager

**Package**: `io.cryolite.catalog`  
**Type**: Internal Component  
**Since**: 0.1.0

## Overview

`CatalogManager` manages Iceberg catalog connections. It handles initialization and lifecycle of REST catalog connections to Polaris or other REST catalog implementations.

## Purpose

Encapsulates the complexity of initializing and managing Apache Iceberg REST Catalog connections with proper configuration merging and lifecycle management.

## Key Features

- ✅ **REST Catalog Support**: Connects to Apache Polaris or any Iceberg REST Catalog
- ✅ **Configuration Merging**: Combines catalog and storage options for FileIO
- ✅ **Health Checks**: Verifies catalog connectivity
- ✅ **Lifecycle Management**: Proper resource cleanup
- ✅ **OAuth Support**: Handles Polaris OAuth credentials

## Usage Example

```java
// Typically used internally by CryoliteEngine
Map<String, String> catalogOptions = Map.of(
    "uri", "http://localhost:8181/api/catalog",
    "credential", "client_id:client_secret",
    "scope", "PRINCIPAL_ROLE:ALL",
    "warehouse", "my_warehouse"
);

Map<String, String> storageOptions = Map.of(
    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
    "s3.endpoint", "http://localhost:9000",
    "s3.access-key-id", "minioadmin",
    "s3.secret-access-key", "minioadmin",
    "s3.path-style-access", "true"
);

CatalogManager manager = new CatalogManager(catalogOptions, storageOptions);
Catalog catalog = manager.getCatalog();

// Use catalog...

manager.close();
```

## API Reference

### Constructor

```java
public CatalogManager(Map<String, String> catalogOptions, Map<String, String> storageOptions)
```

Creates a CatalogManager with a REST catalog.

**Parameters**:
- `catalogOptions` - Catalog configuration (uri, credential, scope, warehouse, etc.)
- `storageOptions` - Storage configuration (io-impl and backend-specific options)

**Throws**:
- `IllegalArgumentException` - If required parameters are missing or invalid

**Required Catalog Options**:
- `uri` - REST catalog endpoint (default: "http://localhost:8181/api/catalog")

**Optional Catalog Options**:
- `credential` - OAuth credentials (format: "client_id:client_secret")
- `scope` - OAuth scope (e.g., "PRINCIPAL_ROLE:ALL")
- `warehouse` - Warehouse name

**Required Storage Options**:
- `io-impl` - FileIO implementation (default: "org.apache.iceberg.aws.s3.S3FileIO")
- Backend-specific options (e.g., S3 endpoint, credentials)

---

### Methods

#### `getCatalog()`

```java
public Catalog getCatalog()
```

Gets the underlying Iceberg catalog.

**Returns**: The Catalog instance

**Throws**:
- `IllegalStateException` - If CatalogManager is closed

---

#### `isHealthy()`

```java
public boolean isHealthy()
```

Checks if the catalog is accessible.

Performs a lightweight operation (tableExists check) to verify catalog connectivity.

**Returns**: `true` if catalog is accessible, `false` otherwise

---

#### `isClosed()`

```java
public boolean isClosed()
```

Checks if the catalog manager is closed.

**Returns**: `true` if closed, `false` otherwise

---

#### `close()`

```java
public void close()
```

Closes the catalog connection.

This method is idempotent - calling it multiple times is safe.

## Implementation Details

### Configuration Merging

The manager merges catalog and storage options into a single properties map for the REST catalog:

1. **Base Properties**: Catalog options (uri, credential, scope, warehouse)
2. **FileIO Configuration**: `io-impl` from storage options
3. **Storage Options**: All storage options (passed through to FileIO)

This allows the FileIO to be properly configured with S3 credentials, endpoints, etc.

### Health Check Strategy

The health check uses `catalog.tableExists(TableIdentifier.of("_health_check", "_health_check"))` as a lightweight connectivity test. This:

- ✅ Verifies catalog is reachable
- ✅ Verifies authentication works
- ✅ Does not create any resources
- ✅ Fails fast if catalog is down

### OAuth Credential Handling

Polaris uses OAuth 2.0 for authentication. The manager passes credentials in the format expected by Iceberg's REST catalog:

- `credential` - Combined "client_id:client_secret"
- `scope` - OAuth scope (e.g., "PRINCIPAL_ROLE:ALL")

## Design Decisions

### Why Separate Catalog and Storage Options?

Separating catalog and storage options provides:

- ✅ **Clear Separation of Concerns**: Catalog vs. Storage configuration
- ✅ **Flexibility**: Different storage backends can be used
- ✅ **Testability**: Easier to mock and test

### Why Not Expose RESTCatalog Directly?

The manager returns the generic `Catalog` interface instead of `RESTCatalog` because:

- ✅ **Abstraction**: Future support for other catalog types (Hive, Hadoop, etc.)
- ✅ **API Stability**: Iceberg's `Catalog` interface is stable
- ✅ **Flexibility**: Users can cast if they need REST-specific features

## Error Handling

- **Missing Configuration**: Throws `IllegalArgumentException` with clear message
- **Connection Failures**: Health check returns `false` (does not throw)
- **Close Errors**: Swallowed (logged to stderr, but does not throw)

## Related Components

- **[CryoliteEngine](CryoliteEngine.md)** - Uses CatalogManager internally
- **[CryoliteConfig](CryoliteConfig.md)** - Provides configuration

## See Also

- [Apache Iceberg REST Catalog](https://iceberg.apache.org/docs/latest/rest-catalog/)
- [Apache Polaris](https://polaris.apache.org/)
- [Iceberg Catalog API](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/catalog/Catalog.html)

