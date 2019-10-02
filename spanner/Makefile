GO_OS := $(shell go env GOOS)
GO_ARCH := $(shell go env GOARCH)
BIN_NAME := "spanner-create"

all: clean deps build

.PHONY: clean
clean:
	@rm -rf $(BIN_NAME)/build/
	@rm -f coverage.txt

.PHONY: deps
deps:
	@go get ./...

.PHONY: test
test:
	@./go-test.sh

.PHONY: build
build:
	GOOS=$(GO_OS) GOARCH=$(GO_ARCH) CGO_ENABLED=0 go build $(GO_FLAGS) -o $(BIN_NAME)/build/$(GO_OS)-$(GO_ARCH)/$(BIN_NAME) \
		github.com/r7wang/gcloud-test/spanner/main/spanner.go

