# TestEnvironment

**Package**: `io.cryolite`  
**Type**: Test Utility  
**Since**: 0.1.0

## Overview

`TestEnvironment` is a utility class for managing the Docker Compose test environment. It provides methods to start Docker Compose services and wait for them to become healthy using intelligent polling instead of fixed sleep times.

## Purpose

Automates the setup of integration test infrastructure (Polaris + MinIO + PostgreSQL) with:

- Automatic service startup
- Health check polling
- Timeout handling
- Idempotent initialization

## Key Features

- ✅ **Automatic Docker Compose**: Starts services if not running
- ✅ **Health Check Polling**: Waits for services to be ready
- ✅ **Timeout Protection**: Fails fast if services don't start
- ✅ **Idempotent**: Safe to call multiple times
- ✅ **Docker Compose V1 & V2**: Supports both `docker compose` and `docker-compose`

## Usage Example

```java
@BeforeAll
static void setup() throws Exception {
    // Ensure services are running before tests
    TestEnvironment.ensureServicesRunning();
}

@Test
void testSomething() {
    // Services are guaranteed to be running and healthy
    // ... perform test ...
}
```

## API Reference

### Main Method

#### `ensureServicesRunning()`

```java
public static synchronized void ensureServicesRunning() throws Exception
```

Ensures Docker Compose services are running and healthy.

This method is idempotent - calling it multiple times is safe and will only initialize once.

**Process**:
1. Start Docker Compose services (if not already running)
2. Wait for Polaris to become healthy (max 60 seconds)
3. Wait for MinIO to become healthy (max 60 seconds)

**Throws**:
- `Exception` - If services cannot be started or do not become healthy within timeout

**Thread-Safety**: Synchronized to prevent concurrent initialization

---

## Implementation Details

### Service Startup

The method attempts to start services using:

1. **Docker Compose V2**: `docker compose up -d`
2. **Fallback to V1**: `docker-compose up -d` (if V2 fails)

Services are started in detached mode (`-d`) to run in the background.

### Health Check Strategy

Each service is polled using HTTP health endpoints:

**Polaris**:
- Endpoint: `http://localhost:8182/q/health`
- Poll interval: 500ms
- Timeout: 60 seconds

**MinIO**:
- Endpoint: `http://localhost:9000/minio/health/live`
- Poll interval: 500ms
- Timeout: 60 seconds

### Polling Algorithm

```java
while (Instant.now().isBefore(deadline)) {
    try {
        HttpURLConnection connection = ...;
        int responseCode = connection.getResponseCode();
        if (responseCode >= 200 && responseCode < 300) {
            return; // Service is healthy
        }
    } catch (Exception e) {
        // Service not ready yet, continue polling
    }
    Thread.sleep(POLL_INTERVAL.toMillis());
}
throw new RuntimeException("Service did not become healthy within timeout");
```

## Configuration

### Timeouts

- **Default Timeout**: 60 seconds per service
- **Poll Interval**: 500 milliseconds

These values are defined as constants:

```java
private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
```

### Service Endpoints

Health check endpoints are hardcoded for the default Docker Compose setup:

- **Polaris**: `http://localhost:8182/q/health`
- **MinIO**: `http://localhost:9000/minio/health/live`

## Docker Compose Services

The method expects the following services to be defined in `docker-compose.yml`:

1. **PostgreSQL** (`cryolite-postgres`)
   - Polaris metadata database
   - Port: 5432

2. **Polaris** (`cryolite-polaris`)
   - REST catalog server
   - API Port: 8181
   - Health Port: 8182

3. **MinIO** (`cryolite-minio`)
   - S3-compatible object storage
   - API Port: 9000
   - Console Port: 9001

## Design Decisions

### Why Polling Instead of Fixed Sleep?

Polling is used instead of fixed sleep times because:

- ✅ **Faster**: Tests start as soon as services are ready
- ✅ **Reliable**: Adapts to different machine speeds
- ✅ **Fail Fast**: Detects startup failures quickly

### Why Idempotent?

The method is idempotent because:

- ✅ **Multiple Test Classes**: Can be called from multiple test classes
- ✅ **Safety**: No harm in calling multiple times
- ✅ **Performance**: Initialization happens only once

### Why Synchronized?

The method is synchronized because:

- ✅ **Thread-Safety**: Prevents concurrent initialization
- ✅ **Parallel Tests**: Safe for parallel test execution
- ✅ **Volatile Flag**: Works with volatile `initialized` flag

### Why Not Use Testcontainers?

Testcontainers is not used because:

- ✅ **Simplicity**: Docker Compose is simpler for this use case
- ✅ **Reusability**: Services can be reused across test runs
- ✅ **Performance**: Faster startup (services stay running)
- ✅ **Debugging**: Easier to inspect running services

## Error Handling

### Service Startup Failures

If Docker Compose fails to start, the method:

1. Tries Docker Compose V2 (`docker compose`)
2. Falls back to V1 (`docker-compose`)
3. Throws `RuntimeException` with error output if both fail

### Health Check Timeouts

If a service doesn't become healthy within 60 seconds:

```
RuntimeException: Polaris did not become healthy within 60 seconds
```

### Connection Errors

Connection errors during polling are silently caught and retried until timeout.

## Troubleshooting

### Services Don't Start

1. Check Docker is running: `docker ps`
2. Check Docker Compose file: `docker compose config`
3. Check service logs: `docker compose logs polaris`

### Health Checks Fail

1. Check service is running: `docker compose ps`
2. Check health endpoint manually: `curl http://localhost:8182/q/health`
3. Increase timeout if needed (edit `DEFAULT_TIMEOUT`)

### Port Conflicts

If ports are already in use:

1. Stop conflicting services
2. Or modify `docker-compose.yml` to use different ports
3. Update health check URLs in `TestEnvironment.java`

## Performance Considerations

- **First Run**: ~10-30 seconds (services start from scratch)
- **Subsequent Runs**: ~1-2 seconds (services already running)
- **Parallel Tests**: Initialization happens only once

## Related Components

- **[AbstractIntegrationTest](AbstractIntegrationTest.md)** - Calls `ensureServicesRunning()` in `@BeforeAll`
- **[TestConfig](TestConfig.md)** - Provides service endpoints and credentials

## See Also

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Polaris Health Checks](https://polaris.apache.org/)
- [MinIO Health Checks](https://min.io/docs/minio/linux/operations/monitoring/healthcheck-probe.html)

