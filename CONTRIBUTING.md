# Contributing to CRYOLITE

Thank you for your interest in contributing to CRYOLITE! We welcome contributions from the community.

## Getting Started

### Prerequisites

- Java 21+
- Maven 3.9+
- Docker & Docker Compose
- Git

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/cryolite-io/cryolite.git
cd cryolite

# Start Docker services
docker-compose up -d

# Build and test
mvn clean test
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

Use descriptive branch names:
- `feature/add-sql-support`
- `fix/null-handling-bug`
- `docs/update-readme`

### 2. Make Changes

- Write clean, readable code
- Follow Google Java Format (enforced by Spotless)
- Add unit tests for new functionality
- Aim for 100% code coverage

### 3. Pre-Commit Checks

Before committing, the following checks run automatically:

```bash
# Code formatting (Spotless)
mvn spotless:apply

# Unit tests with coverage (JUnit 5 + JaCoCo)
mvn clean test

# Code quality analysis (SonarCloud)
sonar-scanner
```

All checks must pass before a commit is accepted.

### 4. Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code refactoring
- `test`: Test additions/changes
- `docs`: Documentation
- `build`: Build system changes
- `chore`: Maintenance tasks

**Examples:**
```
feat(sql): add WHERE clause support

Implement basic WHERE clause filtering with AND operator.
Supports residual filtering for unsupported predicates.

Closes #42
```

```
fix(engine): handle null values in comparisons

Implement 3-valued logic for NULL comparisons.
```

### 5. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a PR on GitHub with:
- Clear description of changes
- Reference to related issues
- Screenshots/examples if applicable

## Code Quality Standards

### Mandatory Requirements

- ✅ **100% Unit Test Coverage** (for own logic)
- ✅ **Code Formatting** (Google Java Format via Spotless)
- ✅ **SonarCloud Quality Gate** (no critical/high issues)
- ✅ **Conventional Commits** (enforced by git hooks)
- ✅ **All Tests Pass** (JUnit 5)

### Best Practices

- Write clear, self-documenting code
- Add Javadoc for public APIs
- Keep methods small and focused
- Use meaningful variable names
- Avoid code duplication

## Testing

### Run Tests

```bash
# All tests
mvn clean test

# Specific test class
mvn test -Dtest=CryoliteEngineTest

# With coverage report
mvn clean test jacoco:report
# Report: target/site/jacoco/index.html
```

### Test Structure

```java
class MyFeatureTest {
  @Test
  void testHappyPath() {
    // Arrange
    // Act
    // Assert
  }

  @Test
  void testEdgeCase() {
    // ...
  }

  @Test
  void testErrorHandling() {
    // ...
  }
}
```

## Documentation

- Update README.md for user-facing changes
- Add Javadoc for public APIs
- Document architectural decisions in code comments
- Keep docs in sync with code

## Reporting Issues

Use GitHub Issues with:
- Clear title
- Detailed description
- Steps to reproduce (for bugs)
- Expected vs. actual behavior
- Environment info (Java version, OS, etc.)

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

## Questions?

- Open a GitHub Discussion
- Check existing issues/PRs
- Review the project roadmap in `../prompts/cryolite-java.md`

Thank you for contributing! 🙏

