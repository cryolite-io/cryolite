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

### Development

#### Pre-Commit Hooks

The project uses Git hooks for quality assurance:

- **Code Formatting**: Spotless (Google Java Format)
- **Unit Tests**: JUnit 5
- **SonarCloud Analysis**: Optional (set `SKIP_SONAR=1` to skip)

To run pre-commit checks manually:
```bash
SKIP_SONAR=1 mvn clean test spotless:check
```

#### SonarCloud Integration

SonarCloud analysis is configured but optional in pre-commit hooks:

```bash
# Run SonarCloud analysis
export SONAR_HOST_URL=https://sonarcloud.io
export SONAR_LOGIN=<your-token>
export SONAR_PROJECT_KEY=cryolite
export SONAR_ORGANIZATION=cryolite-io

sonar-scanner
```

## Project Structure

```
cryolite/
├── src/main/java/io/cryolite/     # Main source code
├── src/test/java/io/cryolite/     # Unit tests
├── pom.xml                         # Maven configuration
├── docker-compose.yml              # Docker services
├── .env                            # Environment variables
└── .git/hooks/                     # Git hooks
```

## Milestones

See `../prompts/cryolite-java.md` for the complete roadmap (M0–M31).

### Current: M0 – Project Foundation

- ✅ Maven build with dependencies
- ✅ Docker Compose (Polaris + MinIO)
- ✅ Quality gates (Spotless, JUnit, SonarCloud)
- ✅ Git hooks (pre-commit, commit-msg)
- ✅ Conventional Commits validation

## License

TBD

## Contributing

See CONTRIBUTING.md (TBD)

