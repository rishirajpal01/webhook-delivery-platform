# Shortcuts for the commands CI runs. Postgres tests need DATABASE_URL;
# `make up` starts one on localhost:5432.
DATABASE_URL ?= postgres://webhooks:webhooks@localhost:5432/webhooks?sslmode=disable
STATICCHECK  := honnef.co/go/tools/cmd/staticcheck@2025.1.1

.PHONY: test lint up down

test:
	DATABASE_URL="$(DATABASE_URL)" go test -race ./...

lint:
	gofmt -l . | (! grep .) || (echo "run gofmt -w ." && exit 1)
	go vet ./...
	go run $(STATICCHECK) ./...

up:
	docker compose up --build -d --wait

down:
	docker compose down
