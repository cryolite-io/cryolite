# Git Hooks for CRYOLITE

This directory contains Git hooks that enforce code quality standards for all contributors.

## Why Git Hooks Can't Be Pushed

Git does **not** track the `.git/hooks/` directory for security reasons. If hooks could be pushed to a repository, malicious code could be executed on every developer's machine without their knowledge.

Therefore, **each contributor must install hooks manually** after cloning the repository.

## Available Hooks

### 1. pre-commit

Runs before every commit to ensure code quality:

- **Code Formatting**: Applies Google Java Format via Spotless
- **Unit Tests**: Runs all tests with JaCoCo coverage tracking
- **SonarCloud Analysis**: Analyzes code quality (mandatory, no skipping)

If any check fails, the commit is blocked.

### 2. commit-msg

Validates commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) format:

- `feat:` - New feature
- `fix:` - Bug fix
- `refactor:` - Code refactoring
- `test:` - Test additions/changes
- `docs:` - Documentation
- `build:` - Build system changes
- `chore:` - Maintenance tasks

Examples:
```
feat: add SQL WHERE clause support
fix(engine): handle null values correctly
refactor: simplify catalog manager
```

## Installation

### Quick Install (Recommended)

```bash
# From repository root
./scripts/hooks/install-hooks.sh
```

### Manual Install

```bash
# From repository root
cp scripts/hooks/pre-commit .git/hooks/
cp scripts/hooks/commit-msg .git/hooks/
chmod +x .git/hooks/pre-commit .git/hooks/commit-msg
```

## Verification

```bash
# Check hooks are installed
ls -la .git/hooks/pre-commit .git/hooks/commit-msg

# Test pre-commit hook manually
.git/hooks/pre-commit

# Test commit-msg hook manually
echo "feat: test commit" | .git/hooks/commit-msg /dev/stdin
```

## Requirements

Before using the hooks, ensure you have:

1. **Environment configured**: `.env` file with `SONAR_TOKEN` set
2. **Docker running**: Polaris and MinIO services started
3. **Dependencies installed**: Maven, Java 21, sonar-scanner

## Troubleshooting

### Hook fails with "SONAR_TOKEN not set"

```bash
# Create .env file from example
cp .env.example .env

# Edit and set your SonarCloud token
nano .env
```

### Hook fails with "sonar-scanner: command not found"

```bash
# Install SonarScanner
# macOS
brew install sonar-scanner

# Linux
wget https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-6.2.1.4610-linux-x64.zip
unzip sonar-scanner-cli-6.2.1.4610-linux-x64.zip
sudo mv sonar-scanner-6.2.1.4610-linux-x64 /opt/sonar-scanner
sudo ln -s /opt/sonar-scanner/bin/sonar-scanner /usr/local/bin/sonar-scanner
```

### Hook fails with Docker errors

```bash
# Start Docker services
docker-compose up -d

# Wait for services to be healthy
docker-compose ps
```

## Bypassing Hooks (NOT RECOMMENDED)

While it's technically possible to bypass hooks with `git commit --no-verify`, this is **strongly discouraged**. Hooks ensure code quality and prevent broken code from being committed.

If you need to bypass hooks temporarily:
1. Fix the underlying issue instead
2. If absolutely necessary, document why in the commit message
3. Expect the PR to be rejected if quality checks fail in CI/CD

## Updating Hooks

When hooks are updated in the repository:

```bash
# Re-run the install script to get the latest version
./scripts/hooks/install-hooks.sh
```

## Questions?

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for more details on the development workflow.

