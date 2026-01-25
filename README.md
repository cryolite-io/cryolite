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

2. **Start Docker services** (Polaris + MinIO)
   ```bash
   docker-compose up -d
   ```

3. **Build the project**
   ```bash
   mvn clean test
   ```

## Development

### Pre-Commit Hooks

The project uses Git hooks for quality assurance. All checks run automatically before each commit:

- **Code Formatting**: Spotless (Google Java Format)
- **Unit Tests**: JUnit 5 with 100% coverage requirement
- **Code Quality**: SonarCloud analysis (required)

The pre-commit hook will:
1. Format code with Spotless
2. Run all tests with coverage
3. Analyze code with SonarCloud
4. Block commit if any check fails

### Manual Quality Checks

```bash
# Format code
mvn spotless:apply

# Run tests with coverage
mvn clean test

# Check code quality
mvn spotless:check
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

See `../prompts/cryolite-java.md` for the complete roadmap (M0–M31).

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

- **Test Coverage**: 100% for own logic
- **Code Style**: Google Java Format (enforced by Spotless)
- **Quality Gate**: SonarCloud (no critical/high issues)
- **Commits**: Conventional Commits format

