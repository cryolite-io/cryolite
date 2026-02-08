# CRYOLITE Documentation

This directory contains detailed documentation for the CRYOLITE project.

## Documentation Structure

- **[Architecture](architecture/)** - System architecture and design decisions
- **[API Reference](api/)** - Detailed API documentation
- **[Testing](testing/)** - Testing infrastructure and helpers
- **[Development](development/)** - Development guides and workflows

## Quick Links

### Core Components
- [CryoliteEngine](api/core/CryoliteEngine.md) - Main entry point
- [CryoliteConfig](api/core/CryoliteConfig.md) - Configuration management
- [CatalogManager](api/core/CatalogManager.md) - Catalog connection management

### Data Operations
- [TableWriter](api/data/TableWriter.md) - Low-level data write operations

### Testing Infrastructure
- [AbstractIntegrationTest](testing/AbstractIntegrationTest.md) - Base class for integration tests
- [S3StorageTestHelper](testing/S3StorageTestHelper.md) - S3-compatible storage verification
- [TestConfig](testing/TestConfig.md) - Test configuration management
- [TestEnvironment](testing/TestEnvironment.md) - Docker Compose test environment

## Architecture Overview

CRYOLITE is an embedded Apache Iceberg table/query engine with the following key principles:

1. **Embedded Library**: No CLI, no server, no REST service
2. **Single-Node**: Local execution only
3. **Simplified Architecture**: Iceberg Catalog IS the low-level API
4. **Educational**: Clean, readable, and well-documented code
5. **Production-Ready**: Real optimizations, not toy code

## Technology Stack

- **Apache Iceberg 1.10.1** - Table format
- **Apache Polaris 1.3.0** - REST Catalog
- **Apache Calcite 1.41.0** - SQL parser and optimizer (planned)
- **Apache Arrow 18.3.0** - Vectorized execution (planned)
- **AWS SDK 2.41.19** - S3-compatible storage access
- **MinIO** - S3-compatible object storage (development)

## Development Workflow

See [Development Guide](development/workflow.md) for detailed information on:
- Setting up the development environment
- Running tests
- Code quality checks
- Commit guidelines
- CI/CD pipeline

