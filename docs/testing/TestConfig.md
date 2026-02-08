# TestConfig

**Package**: `io.cryolite`  
**Type**: Test Utility  
**Since**: 0.1.0

## Overview

`TestConfig` loads test configuration from environment variables, typically from a `.env` file. It provides a centralized way to access test credentials and endpoints without hardcoding them.

## Purpose

Manages test configuration with the following priorities:

1. `.env` file (project-specific configuration)
2. System environment variables
3. Fail if not found (no defaults for security)

## Key Features

- ✅ **Automatic .env Loading**: Loads `.env` file from project root
- ✅ **No Hardcoded Secrets**: All credentials from environment
- ✅ **Clear Error Messages**: Fails fast with helpful error messages
- ✅ **Priority System**: .env file takes precedence over system environment
- ✅ **Comment Support**: Supports `#` comments in .env file

## Usage Example

```java
// Get Polaris configuration
String polarisUri = TestConfig.getPolarisUri();
String clientId = TestConfig.getPolarisClientId();
String clientSecret = TestConfig.getPolarisClientSecret();

// Get MinIO configuration
String minioEndpoint = TestConfig.getMinioEndpoint();
String accessKey = TestConfig.getMinioAccessKey();
String secretKey = TestConfig.getMinioSecretKey();
```

## API Reference

### Polaris Configuration

#### `getPolarisUri()`

```java
public static String getPolarisUri()
```

Returns the Polaris REST catalog endpoint URL.

**Environment Variable**: `POLARIS_URI`  
**Example**: `http://localhost:8181/api/catalog`

**Returns**: Polaris URI

**Throws**:
- `IllegalStateException` - If `POLARIS_URI` is not set

---

#### `getPolarisClientId()`

```java
public static String getPolarisClientId()
```

Returns the Polaris OAuth client ID.

**Environment Variable**: `POLARIS_CLIENT_ID`  
**Example**: `principal`

**Returns**: Client ID

**Throws**:
- `IllegalStateException` - If `POLARIS_CLIENT_ID` is not set

---

#### `getPolarisClientSecret()`

```java
public static String getPolarisClientSecret()
```

Returns the Polaris OAuth client secret.

**Environment Variable**: `POLARIS_CLIENT_SECRET`  
**Example**: `abc123...`

**Returns**: Client secret

**Throws**:
- `IllegalStateException` - If `POLARIS_CLIENT_SECRET` is not set

---

#### `getPolarisWarehouse()`

```java
public static String getPolarisWarehouse()
```

Returns the Polaris warehouse name.

**Environment Variable**: `POLARIS_WAREHOUSE`  
**Example**: `cryolite_warehouse`

**Returns**: Warehouse name

**Throws**:
- `IllegalStateException` - If `POLARIS_WAREHOUSE` is not set

---

#### `getPolarisScope()`

```java
public static String getPolarisScope()
```

Returns the Polaris OAuth scope.

**Environment Variable**: `POLARIS_SCOPE`  
**Example**: `PRINCIPAL_ROLE:ALL`

**Returns**: OAuth scope

**Throws**:
- `IllegalStateException` - If `POLARIS_SCOPE` is not set

---

### MinIO Configuration

#### `getMinioEndpoint()`

```java
public static String getMinioEndpoint()
```

Returns the MinIO endpoint URL.

**Environment Variable**: `MINIO_ENDPOINT`  
**Example**: `http://localhost:9000`

**Returns**: MinIO endpoint

**Throws**:
- `IllegalStateException` - If `MINIO_ENDPOINT` is not set

---

#### `getMinioAccessKey()`

```java
public static String getMinioAccessKey()
```

Returns the MinIO access key.

**Environment Variable**: `MINIO_ACCESS_KEY`  
**Example**: `minioadmin`

**Returns**: Access key

**Throws**:
- `IllegalStateException` - If `MINIO_ACCESS_KEY` is not set

---

#### `getMinioSecretKey()`

```java
public static String getMinioSecretKey()
```

Returns the MinIO secret key.

**Environment Variable**: `MINIO_SECRET_KEY`  
**Example**: `minioadmin`

**Returns**: Secret key

**Throws**:
- `IllegalStateException` - If `MINIO_SECRET_KEY` is not set

---

#### `getMinioWarehousePath()`

```java
public static String getMinioWarehousePath()
```

Returns the S3 warehouse path.

**Environment Variable**: `MINIO_WAREHOUSE_PATH`  
**Example**: `s3://warehouse/cryolite`

**Returns**: Warehouse path

**Throws**:
- `IllegalStateException` - If `MINIO_WAREHOUSE_PATH` is not set

## .env File Format

The `.env` file should be placed in the project root directory:

```bash
# Polaris Configuration
POLARIS_URI=http://localhost:8181/api/catalog
POLARIS_CLIENT_ID=principal
POLARIS_CLIENT_SECRET=abc123...
POLARIS_WAREHOUSE=cryolite_warehouse
POLARIS_SCOPE=PRINCIPAL_ROLE:ALL

# MinIO Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_WAREHOUSE_PATH=s3://warehouse/cryolite
```

**Format Rules**:
- `KEY=VALUE` format
- Lines starting with `#` are comments
- Empty lines are ignored
- Quotes around values are removed
- Whitespace is trimmed

## Design Decisions

### Why No Default Values?

No default credentials are provided because:

- ✅ **Security**: Prevents accidental use of hardcoded credentials
- ✅ **Explicit Configuration**: Forces developers to configure properly
- ✅ **Fail Fast**: Clear error messages if configuration is missing

### Why .env File Priority?

The `.env` file takes precedence over system environment variables because:

- ✅ **Project-Specific**: Each project can have its own configuration
- ✅ **Developer-Friendly**: Easy to edit and version control (with .gitignore)
- ✅ **Isolation**: Doesn't pollute system environment

### Why Static Methods?

Static methods are used because:

- ✅ **Utility Class**: No state, just configuration access
- ✅ **Convenience**: Easy to call from anywhere
- ✅ **Singleton Pattern**: Configuration loaded once

## Security Considerations

⚠️ **IMPORTANT**: The `.env` file contains secrets and must be in `.gitignore`!

```gitignore
# Environment (contains secrets)
.env
.env.local
.env.*.local
```

Never commit credentials to version control!

## Error Handling

If a required environment variable is not set, the method throws `IllegalStateException` with a clear message:

```
Required environment variable 'POLARIS_CLIENT_SECRET' is not set. 
Please configure it in your .env file or system environment.
```

## Related Components

- **[AbstractIntegrationTest](AbstractIntegrationTest.md)** - Uses TestConfig for test setup
- **[TestEnvironment](TestEnvironment.md)** - Manages Docker services

## See Also

- [dotenv Format](https://github.com/motdotla/dotenv)
- [12-Factor App Configuration](https://12factor.net/config)

