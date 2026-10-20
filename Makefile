# Shortcuts for the commands CI runs. Postgres tests need DATABASE_URL;
# `make up` starts one on localhost:5432.
DATABASE_URL ?= postgres://webhooks:webhooks@localhost:5432/webhooks?sslmode=disable
STATICCHECK  := honnef.co/go/tools/cmd/staticcheck@2025.1.1
GOVULNCHECK  := golang.org/x/vuln/cmd/govulncheck@v1.1.4
# Build and analyse with go.mod's toolchain whatever Go is installed, so the
# tools above are compiled with the same release as the code they check.
export GOTOOLCHAIN := $(shell awk '/^toolchain/ {print $$2}' go.mod)

.PHONY: test lint vuln up down

test:
	DATABASE_URL="$(DATABASE_URL)" go test -race ./...

lint:
	gofmt -l . | (! grep .) || (echo "run gofmt -w ." && exit 1)
	go vet ./...
	go run $(STATICCHECK) ./...

# Known vulnerabilities in the code paths this module actually calls.
vuln:
	go run $(GOVULNCHECK) ./...

up:
	docker compose up --build -d --wait

down:
	docker compose down
