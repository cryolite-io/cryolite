# CRYOLITE

**CRYOLITE Runs Your Open Lightweight Iceberg Table Engine**

An embedded, lightweight Apache Iceberg table and query engine for Java applications.

## Features

- **Embedded Library**: No CLI, no server, no REST service
- **Single-Node**: Local execution only
- **JVM-First**: Designed for Java applications
- **Educational**: Clean, readable, and well-documented code
- **Production-Ready**: Real optimizations, not toy code

## Quick Start

### Prerequisites

- Java 21+
- Maven 3.9+
- Docker & Docker Compose (for Polaris + MinIO)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/cryolite-io/cryolite.git
   cd cryolite
   ```

2. **Configure environment variables**
   ```bash
   # Copy the example configuration
   cp .env.example .env

   # Edit .env and set your SonarCloud token
   # Get your token from: https://sonarcloud.io/account/security
   nano .env  # or use your preferred editor
   ```

3. **Install Git hooks** (required for contributors)
   ```bash
   # Install hooks using the provided script
   ./scripts/hooks/install-hooks.sh

   # Verify installation
   ls -la .git/hooks/pre-commit .git/hooks/commit-msg
   ```

4. **Start Docker services** (Polaris + MinIO)
   ```bash
   docker-compose up -d

   # Wait for services to be healthy (takes ~10 seconds)
   docker-compose ps

   # Get auto-generated Polaris credentials
   docker logs cryolite-polaris-setup | grep "POLARIS_CLIENT_ID"

   # Copy the credentials to your .env file
   ```

5. **Load environment and build**
   ```bash
   # Load environment variables from .env
   export $(grep -v '^#' .env | xargs)

   # Build and test
   mvn clean test
   ```

## Development

### Environment Configuration

All configuration is centralized in the `.env` file:

```bash
# Create your local configuration
cp .env.example .env

# Edit and set your values (especially SONAR_TOKEN)
nano .env

# Load environment variables before running tests
export $(grep -v '^#' .env | xargs)
```

**Important**: The `.env` file contains secrets and is in `.gitignore`. Never commit it!

### Pre-Commit Hooks

The project uses Git hooks for quality assurance. All checks run automatically before each commit:

- **Code Formatting**: Spotless (Google Java Format)
- **Unit Tests**: JUnit 5 with coverage tracking
- **Code Quality**: SonarCloud analysis (mandatory, no skipping)

The pre-commit hook will:
1. Format code with Spotless
2. Run all tests with coverage
3. Analyze code with SonarCloud
4. Block commit if any check fails

**Git hooks are stored in `.git/hooks/`** and must be installed by each contributor. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions.

### Manual Quality Checks

```bash
# Load environment variables
export $(grep -v '^#' .env | xargs)

# Format code
mvn spotless:apply

# Run tests with coverage
mvn clean test

# Run pre-commit checks manually
.git/hooks/pre-commit
```

## Project Structure

```
cryolite/
├── src/main/java/io/cryolite/     # Main source code
├── src/test/java/io/cryolite/     # Unit tests (100% coverage)
├── pom.xml                         # Maven configuration
├── docker-compose.yml              # Docker services (Polaris + MinIO)
├── .env                            # Environment variables (secrets)
├── .git/hooks/                     # Git hooks (pre-commit, commit-msg)
├── LICENSE                         # Apache License 2.0
├── README.md                       # This file
├── CONTRIBUTING.md                 # Contribution guidelines
└── .gitignore                      # Git ignore rules
```

## Milestones

### Current: M0 – Project Foundation

- ✅ Maven build with dependencies
- ✅ Docker Compose (Polaris + MinIO)
- ✅ Quality gates (Spotless, JUnit, SonarCloud)
- ✅ Git hooks (pre-commit, commit-msg)
- ✅ Conventional Commits validation
- ✅ Apache License 2.0
- ✅ Contributing guidelines

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Development setup
- Commit message guidelines
- Code quality standards
- Testing requirements
- Pull request process

## License

This project is licensed under the Apache License 2.0 - see [LICENSE](LICENSE) file for details.

## Code Quality

- **Test Coverage**: Minimum 85% line coverage (enforced by JaCoCo)
- **Code Style**: Google Java Format (enforced by Spotless)
- **Quality Gate**: SonarCloud (no critical/high issues)
- **Commits**: Conventional Commits format

