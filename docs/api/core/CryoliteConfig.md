# CryoliteConfig

**Package**: `io.cryolite`  
**Type**: Configuration Class  
**Since**: 0.1.0

## Overview

`CryoliteConfig` is an immutable configuration class for `CryoliteEngine`. It holds configuration for catalog, storage, and other engine options without embedding secrets in code.

## Purpose

Provides a type-safe, immutable configuration object that separates concerns:

- **Catalog Configuration**: REST catalog connection details
- **Storage Configuration**: S3-compatible storage settings
- **Engine Configuration**: Engine-specific options (future use)

## Key Features

- ✅ **Immutable**: Thread-safe, cannot be modified after creation
- ✅ **Builder Pattern**: Fluent API for easy configuration
- ✅ **No Secrets in Code**: All credentials come from environment or external config
- ✅ **Type-Safe**: Compile-time safety for configuration keys
- ✅ **Defensive Copies**: Returns copies of internal maps to prevent modification

## Usage Example

```java
CryoliteConfig config = new CryoliteConfig.Builder()
    // Catalog configuration
    .catalogType("rest")  // Default: "rest"
    .catalogOption("uri", "http://localhost:8181/api/catalog")
    .catalogOption("credential", "client_id:client_secret")
    .catalogOption("scope", "PRINCIPAL_ROLE:ALL")
    .catalogOption("warehouse", "my_warehouse")
    
    // Storage configuration
    .storageType("s3")  // Default: "s3"
    .storageOption("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .storageOption("s3.endpoint", "http://localhost:9000")
    .storageOption("s3.access-key-id", "minioadmin")
    .storageOption("s3.secret-access-key", "minioadmin")
    .storageOption("s3.path-style-access", "true")
    .storageOption("client.region", "us-west-2")
    .storageOption("warehouse-path", "s3://warehouse/path")
    
    // Engine options (future use)
    .engineOption("parallelism", "4")
    
    .build();
```

## API Reference

### Builder

```java
public static class Builder
```

Builder for creating `CryoliteConfig` instances.

#### Methods

##### `catalogType(String catalogType)`

Sets the catalog type (default: "rest").

**Parameters**:
- `catalogType` - The catalog type (e.g., "rest", "hive", "hadoop")

**Returns**: This builder for chaining

---

##### `catalogOption(String key, String value)`

Adds a catalog configuration option.

**Parameters**:
- `key` - The option key (e.g., "uri", "credential", "warehouse")
- `value` - The option value

**Returns**: This builder for chaining

**Common Catalog Options**:
- `uri` - REST catalog endpoint URL
- `credential` - OAuth credentials (format: "client_id:client_secret")
- `scope` - OAuth scope (e.g., "PRINCIPAL_ROLE:ALL")
- `warehouse` - Warehouse name in Polaris

---

##### `storageType(String storageType)`

Sets the storage type (default: "s3").

**Parameters**:
- `storageType` - The storage type (e.g., "s3", "hdfs", "local")

**Returns**: This builder for chaining

---

##### `storageOption(String key, String value)`

Adds a storage configuration option.

**Parameters**:
- `key` - The option key
- `value` - The option value

**Returns**: This builder for chaining

**Common Storage Options (S3)**:
- `io-impl` - FileIO implementation class (e.g., "org.apache.iceberg.aws.s3.S3FileIO")
- `s3.endpoint` - S3-compatible endpoint URL
- `s3.access-key-id` - S3 access key
- `s3.secret-access-key` - S3 secret key
- `s3.path-style-access` - Use path-style access (required for MinIO)
- `client.region` - AWS region (required by SDK)
- `warehouse-path` - S3 warehouse path (e.g., "s3://bucket/path")

---

##### `engineOption(String key, String value)`

Adds an engine configuration option.

**Parameters**:
- `key` - The option key
- `value` - The option value

**Returns**: This builder for chaining

---

##### `build()`

Builds the immutable `CryoliteConfig` instance.

**Returns**: A new `CryoliteConfig` instance

---

### CryoliteConfig Methods

#### `getCatalogType()`

```java
public String getCatalogType()
```

Returns the catalog type.

**Returns**: The catalog type (e.g., "rest")

---

#### `getCatalogOptions()`

```java
public Map<String, String> getCatalogOptions()
```

Returns a copy of the catalog options.

**Returns**: Immutable copy of catalog options

---

#### `getStorageType()`

```java
public String getStorageType()
```

Returns the storage type.

**Returns**: The storage type (e.g., "s3")

---

#### `getStorageOptions()`

```java
public Map<String, String> getStorageOptions()
```

Returns a copy of the storage options.

**Returns**: Immutable copy of storage options

---

#### `getEngineOptions()`

```java
public Map<String, String> getEngineOptions()
```

Returns a copy of the engine options.

**Returns**: Immutable copy of engine options

## Design Decisions

### Why Immutable?

- ✅ **Thread-Safety**: Can be safely shared across threads
- ✅ **Predictability**: Configuration cannot change unexpectedly
- ✅ **Defensive**: Prevents accidental modification

### Why String-Based Options?

- ✅ **Flexibility**: Easy to pass through to Iceberg APIs
- ✅ **Compatibility**: Matches Iceberg's configuration model
- ✅ **Extensibility**: New options don't require code changes

### Why Defensive Copies?

All getter methods return new `HashMap` instances to prevent external modification of internal state.

## Security Considerations

⚠️ **NEVER commit credentials to version control!**

- Use environment variables (`.env` file)
- Use external configuration management
- Use secret management systems (AWS Secrets Manager, HashiCorp Vault, etc.)

## Related Components

- **[CryoliteEngine](CryoliteEngine.md)** - Uses this configuration
- **[CatalogManager](CatalogManager.md)** - Consumes catalog and storage options

## See Also

- [Apache Iceberg Configuration](https://iceberg.apache.org/docs/latest/configuration/)
- [AWS SDK S3 Configuration](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html)

