.PHONY: help format test coverage quality dependency-check verify verify-with-quality docker-up docker-down docker-logs clean

# Load environment variables from .env file if it exists
# The '-' prefix suppresses errors if the file doesn't exist
-include .env
export

# Default target
help:
	@echo "CRYOLITE Build Commands"
	@echo "======================="
	@echo ""
	@echo "Development:"
	@echo "  make format              - Format code with Spotless"
	@echo "  make test                - Run all tests"
	@echo "  make coverage            - Check test coverage (85% minimum)"
	@echo "  make quality             - Run SonarCloud analysis"
	@echo "  make dependency-check    - Check dependencies for vulnerabilities (CVSS >= 7.0)"
	@echo ""
	@echo "Verification:"
	@echo "  make verify              - Run all checks (format + test + coverage + dependency-check)"
	@echo "  make verify-with-quality - Run all checks including SonarCloud"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up           - Start Docker Compose services (Polaris + MinIO + PostgreSQL)"
	@echo "  make docker-down         - Stop Docker Compose services"
	@echo "  make docker-logs         - Show Docker Compose logs"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean               - Clean Maven build artifacts"
	@echo ""

# Format code with Spotless
format:
	@echo "📝 Formatting code with Spotless..."
	mvn spotless:apply -q
	@echo "✅ Code formatted"

# Run all tests
test:
	@echo "🧪 Running tests..."
	mvn clean test -q
	@echo "✅ Tests passed"

# Check test coverage (85% minimum)
# Note: Coverage check is already included in 'test' target via pom.xml configuration
coverage: test
	@echo "✅ Coverage check passed (85%+ minimum)"

# Run SonarCloud analysis
quality:
	@echo "📊 Running SonarCloud analysis..."
	@if [ -z "$(SONAR_TOKEN)" ]; then \
		echo "❌ Error: SONAR_TOKEN not set in .env"; \
		exit 1; \
	fi
	sonar-scanner \
		-Dsonar.projectKey=cryolite-io_cryolite \
		-Dsonar.organization=cryolite-io \
		-Dsonar.sources=src/main \
		-Dsonar.tests=src/test \
		-Dsonar.java.binaries=target/classes \
		-Dsonar.java.test.binaries=target/test-classes \
		-Dsonar.coverage.jacoco.xmlReportPaths=target/site/jacoco/jacoco.xml \
		-Dsonar.qualitygate.wait=true
	@echo "✅ SonarCloud analysis passed"

# Check dependencies for vulnerabilities
dependency-check:
	@echo "🔍 Checking dependencies for vulnerabilities..."
	@if [ -n "$(NVD_API_KEY)" ]; then \
		echo "   Using NVD API Key for faster updates..."; \
		mvn org.owasp:dependency-check-maven:check -DnvdApiKey=$(NVD_API_KEY) -q; \
	else \
		echo "   (Tip: Set NVD_API_KEY in .env for faster updates)"; \
		mvn org.owasp:dependency-check-maven:check -q; \
	fi
	@echo "✅ Dependency check passed"

# Verify all checks (format + test + coverage + dependency-check)
verify: format test coverage dependency-check
	@echo "✅ All verification checks passed!"

# Verify all checks including SonarCloud
verify-with-quality: format test coverage dependency-check quality
	@echo "✅ All verification checks passed (including SonarCloud)!"

# Start Docker Compose services
docker-up:
	@echo "🐳 Starting Docker Compose services..."
	docker-compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 5
	@echo "✅ Docker Compose services started"
	@echo "   Polaris: http://localhost:8181"
	@echo "   MinIO:   http://localhost:9000"

# Stop Docker Compose services
docker-down:
	@echo "🐳 Stopping Docker Compose services..."
	docker-compose down
	@echo "✅ Docker Compose services stopped"

# Show Docker Compose logs
docker-logs:
	docker-compose logs -f

# Clean Maven build artifacts
clean:
	@echo "🧹 Cleaning Maven build artifacts..."
	mvn clean -q
	@echo "✅ Clean complete"

