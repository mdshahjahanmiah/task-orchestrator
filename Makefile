# Variables
APP_NAME=task-orchestrator
GO_CMD=go
TEST_CMD=$(GO_CMD) test
COVERAGE_FILE=coverage.out
LINT_CMD=golangci-lint run

.PHONY: all test coverage lint clean

# Default target
all: test coverage

# Run all tests
test:
	@echo "Running tests..."
	$(TEST_CMD) ./... -v

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(TEST_CMD) ./... -cover -coverprofile=$(COVERAGE_FILE)
	@echo "Generating coverage report..."
	$(GO_CMD) tool cover -func=$(COVERAGE_FILE)
	@echo "HTML coverage report generated as coverage.html"
	$(GO_CMD) tool cover -html=$(COVERAGE_FILE) -o coverage.html

# Run linter (requires golangci-lint installed)
lint:
	@echo "Running linter..."
	$(LINT_CMD)

# Clean up generated files
clean:
	@echo "Cleaning up..."
	@rm -f $(COVERAGE_FILE) coverage.html
