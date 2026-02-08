# CryoliteEngine

**Package**: `io.cryolite`  
**Type**: Main Entry Point  
**Since**: 0.1.0

## Overview

`CryoliteEngine` is the main entry point for the CRYOLITE embedded Iceberg table engine. It provides access to the Iceberg Catalog for all table and namespace operations.

## Purpose

This is an embedded library - no CLI, no server, no REST service. It is designed to be used directly from Java applications for managing Apache Iceberg tables.

## Key Features

- ✅ **Embedded Library**: Direct Java API, no external processes
- ✅ **Lifecycle Management**: Clean create/close pattern
- ✅ **Health Checks**: Built-in catalog connectivity verification
- ✅ **Resource Management**: Proper cleanup of catalog connections
- ✅ **Thread-Safe**: Safe for concurrent access (catalog operations are thread-safe)

## Usage Example

```java
// Create configuration
CryoliteConfig config = new CryoliteConfig.Builder()
    .catalogOption("uri", "http://localhost:8181/api/catalog")
    .catalogOption("credential", "client_id:client_secret")
    .catalogOption("warehouse", "my_warehouse")
    .storageOption("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .storageOption("s3.endpoint", "http://localhost:9000")
    .storageOption("s3.access-key-id", "minioadmin")
    .storageOption("s3.secret-access-key", "minioadmin")
    .build();

// Create engine
CryoliteEngine engine = new CryoliteEngine(config);

// Get catalog for operations
Catalog catalog = engine.getCatalog();

// Namespace operations (cast to SupportsNamespaces)
SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
nsCatalog.createNamespace(Namespace.of("my_namespace"), Map.of());

// Table operations
TableIdentifier tableId = TableIdentifier.of("my_namespace", "my_table");
Schema schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "name", Types.StringType.get())
);
catalog.createTable(tableId, schema);

// Always close when done
engine.close();
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

#### `close()`

```java
public void close()
```

Closes the engine and releases all resources.

After calling this method, the engine cannot be used anymore. This method is idempotent - calling it multiple times is safe.

## Architecture

```
CryoliteEngine
    ├── CryoliteConfig (immutable configuration)
    └── CatalogManager (manages REST catalog connection)
            └── RESTCatalog (Apache Iceberg REST Catalog)
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

## Related Components

- **[CryoliteConfig](CryoliteConfig.md)** - Configuration management
- **[CatalogManager](CatalogManager.md)** - Catalog connection management

## See Also

- [Apache Iceberg Catalog API](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/catalog/Catalog.html)
- [Apache Polaris REST Catalog](https://polaris.apache.org/)

