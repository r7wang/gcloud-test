GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
TARGET_DIR := build/$(GO_OS)-$(GO_ARCH)
TARGET_PREFIX := spanner

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

.PHONY: build build-datagen build-query
build: build-datagen build-query

build-datagen:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o $(TARGET_DIR)/$(TARGET_PREFIX)-datagen \
		./main/datagen

build-query:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o $(TARGET_DIR)/$(TARGET_PREFIX)-query \
		./main/query

