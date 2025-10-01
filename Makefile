# Output binary name
APP_NAME = crdb-authzed-load-test

# Default build
build:
	go build -o $(APP_NAME) cmd/main.go

# Cross-compilation targets
build-linux:
	GOOS=linux GOARCH=amd64 go build -o $(APP_NAME)-linux cmd/main.go

build-mac:
	GOOS=darwin GOARCH=arm64 go build -o $(APP_NAME)-mac cmd/main.go

build-windows:
	GOOS=windows GOARCH=amd64 go build -o $(APP_NAME).exe cmd/main.go

build-all: build build-linux build-mac build-windows

# Clean up all binaries
clean:
	rm -f $(APP_NAME) $(APP_NAME)-linux $(APP_NAME)-mac $(APP_NAME).exe
