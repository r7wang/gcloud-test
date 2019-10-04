GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
TARGET_DIR := build/$(GO_OS)-$(GO_ARCH)

all: clean deps build

.PHONY: clean
clean:
	@rm -rf build/
	@rm -f coverage.txt

.PHONY: deps
deps:
	@go get ./...

.PHONY: test
test:
	@./go-test.sh

.PHONY: build build-spanner build-spanner-datagen build-spanner-test build-bigtable build-bigtable-datagen build-bigtable-test
build: build-spanner build-bigtable

build-spanner: build-spanner-datagen build-spanner-test

build-spanner-datagen:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) \
		-o $(TARGET_DIR)/spanner-datagen \
		./main/spanner-datagen

build-spanner-test:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) \
		-o $(TARGET_DIR)/spanner-test \
		./main/spanner-test

build-bigtable: build-bigtable-datagen build-bigtable-test

build-bigtable-datagen:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) \
		-o $(TARGET_DIR)/bigtable-datagen \
		./main/bigtable-datagen

build-bigtable-test:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) \
		-o $(TARGET_DIR)/bigtable-test \
		./main/bigtable-test

