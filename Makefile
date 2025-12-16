# --- Variables ---
APP_NAME := plgm
SRC_DIR := cmd/plgm/main.go
BIN_DIR := bin

# Get the latest git tag or commit hash.
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Linker flags to inject the version.
# -s -w strips debugging information to make the binary smaller (good for static builds).
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

# --- Commands ---

.PHONY: all build build-linux build-mac clean run help

# Default target
all: build

# Build and Package for all platforms
build: clean build-linux build-mac
	@echo "All builds and packages complete. Artifacts are in $(BIN_DIR)/"

# Build and Package for Linux (CentOS, RHEL, Ubuntu, Debian, etc.)
build-linux:
	@echo "Building and packaging $(APP_NAME) for Linux (amd64)..."
	@mkdir -p $(BIN_DIR)
	# CGO_ENABLED=0 creates a static binary that runs on CentOS/RHEL/Alpine
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME)-linux-amd64 $(SRC_DIR)
	tar -czvf $(BIN_DIR)/$(APP_NAME)-linux-amd64.tar.gz -C $(BIN_DIR) $(APP_NAME)-linux-amd64

# Build and Package for Mac
build-mac:
	@echo "Building and packaging $(APP_NAME) for Mac..."
	@mkdir -p $(BIN_DIR)
	
	# Mac Intel (amd64)
	# CGO is usually required for Mac system calls, so we keep it enabled (default) or rely on pure Go.
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME)-darwin-amd64 $(SRC_DIR)
	tar -czvf $(BIN_DIR)/$(APP_NAME)-darwin-amd64.tar.gz -C $(BIN_DIR) $(APP_NAME)-darwin-amd64

	# Mac Silicon (arm64)
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME)-darwin-arm64 $(SRC_DIR)
	tar -czvf $(BIN_DIR)/$(APP_NAME)-darwin-arm64.tar.gz -C $(BIN_DIR) $(APP_NAME)-darwin-arm64

# Build for the CURRENT OS (Local development)
build-local:
	@echo "Building $(APP_NAME) for local OS..."
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME) $(SRC_DIR)

# Clean up binaries
clean:
	@echo "Cleaning up..."
	@rm -rf $(BIN_DIR)

# Run the tool locally
run: build-local
	@./$(BIN_DIR)/$(APP_NAME) --help

# Show this help menu
help:
	@echo "Makefile for $(APP_NAME)"
	@echo "Usage:"
	@echo "  make build       - Create .tar.gz releases for Linux (CentOS/Ubuntu) and Mac"
	@echo "  make build-local - Build native binary for local testing"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make run         - Build locally and run (shows help)"