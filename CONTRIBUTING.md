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
# 1. Clone the repository
git clone https://github.com/cryolite-io/cryolite.git
cd cryolite

# 2. Configure environment variables
cp .env.example .env

# 3. Edit .env and set your tokens
# - SONAR_TOKEN: Get from https://sonarcloud.io/account/security
# - NVD_API_KEY (optional): Get from https://nvd.nist.gov/developers/request-an-api-key
nano .env

# 4. Install Git hooks (REQUIRED for all contributors)
./scripts/hooks/install-hooks.sh
# See "Git Hooks Setup" section below for details

# 5. Start Docker services
make docker-up

# 6. Get auto-generated Polaris credentials
docker logs cryolite-polaris-setup | grep "POLARIS_CLIENT_ID"
# Copy the output to your .env file

# 7. Run tests
make test
```

### Git Hooks Setup

**IMPORTANT**: Git hooks are stored in `.git/hooks/` which is **NOT tracked by Git**. Each contributor must install them manually.

#### Why Git Hooks Can't Be Pushed

Git does not track the `.git/hooks/` directory for security reasons. If hooks could be pushed, malicious code could be executed on every developer's machine. Therefore, each contributor must install hooks manually.

#### Installing Git Hooks

The project requires two hooks:

1. **pre-commit**: Runs code formatting, tests, and SonarCloud analysis
2. **commit-msg**: Validates commit message format (Conventional Commits)

**Recommended: Use the install script**
```bash
# Run the installation script
./scripts/hooks/install-hooks.sh

# Verify installation
ls -la .git/hooks/pre-commit .git/hooks/commit-msg
```

**Alternative: Manual installation**
```bash
# Copy hooks from scripts/hooks/ to .git/hooks/
cp scripts/hooks/pre-commit .git/hooks/
cp scripts/hooks/commit-msg .git/hooks/
chmod +x .git/hooks/pre-commit .git/hooks/commit-msg
```

#### Verifying Hook Installation

```bash
# Test pre-commit hook manually
.git/hooks/pre-commit

# Test commit-msg hook manually
echo "feat: test commit" | .git/hooks/commit-msg

# Make a test commit to verify hooks run automatically
git commit --allow-empty -m "test: verify hooks are working"
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

Before committing, the following checks run automatically via `make verify-with-quality`:

1. ✅ Code formatting (Spotless)
2. ✅ Unit tests with coverage (JUnit 5 + JaCoCo, 85% minimum)
3. ✅ Dependency vulnerability check (OWASP, CVSS ≥ 7.0)
4. ✅ Code quality analysis (SonarCloud)

All checks must pass before a commit is accepted.

**To run checks manually**:
```bash
# Run all verification checks (Makefile loads .env automatically)
make verify

# Or run individual checks
make format              # Format code
make test                # Run tests
make dependency-check    # Check for vulnerabilities
make quality             # SonarCloud analysis

# Run pre-commit hook manually
.git/hooks/pre-commit
```

**If checks fail**:
- Fix the issues reported
- Re-run the checks
- Commit again

**Never skip hooks**: The hooks ensure code quality and prevent broken code from being committed.

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

- ✅ **Minimum 85% Line Coverage** (enforced by JaCoCo, focus on meaningful tests)
- ✅ **Code Formatting** (Google Java Format via Spotless)
- ✅ **SonarCloud Quality Gate** (no critical/high issues)
- ✅ **Dependency Security** (OWASP check, CVSS ≥ 7.0 fails build)
- ✅ **Conventional Commits** (enforced by git hooks)
- ✅ **All Tests Pass** (JUnit 5)

### Best Practices

- Write clear, self-documenting code
- Add Javadoc for public APIs
- Keep methods small and focused
- Use meaningful variable names
- Avoid code duplication

## Testing

### Coverage Policy

**Target: Minimum 85% line coverage**

We enforce a **85% minimum line coverage** threshold instead of 100% because:
- **Quality over quantity**: Focus on meaningful tests that verify actual behavior
- **Realistic goal**: Some code paths (error handling, edge cases) are difficult to test without artificial scenarios
- **Maintainability**: Avoid writing tests just to reach 100% that don't add real value
- **Pragmatic approach**: 85% provides excellent coverage while allowing flexibility for complex scenarios

**What to test:**
- ✅ All public APIs and their contracts
- ✅ Business logic and data transformations
- ✅ Error handling for expected failures
- ✅ Edge cases and boundary conditions
- ✅ Integration with external services (Polaris, MinIO)

**What NOT to test:**
- ❌ Simple getters/setters without logic
- ❌ Trivial constructors
- ❌ Framework-generated code
- ❌ Unreachable defensive code

### Run Tests

```bash
# Start Docker services (if not already running)
make docker-up

# All tests (Makefile loads .env automatically)
make test

# Specific test class
mvn test -Dtest=CryoliteEngineTest

# With coverage report
mvn clean test jacoco:report
# Report: target/site/jacoco/index.html

# Run all verification checks
make verify
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

Thank you for contributing! 🙏

