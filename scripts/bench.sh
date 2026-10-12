#!/usr/bin/env bash
# Benchmarks delivery with 1, 2 and 4 dispatcher replicas on this machine.
#
# For each replica count it recreates the database, starts one api process
# and the dispatchers, then runs loadgen twice:
#   steady: sends at RATE messages/s                    -> lag under normal load
#   burst:  sends MESSAGES messages as fast as possible -> throughput
#
# Needs Go and psql, and a Postgres where ADMIN_URL can create databases.
#
#   ADMIN_URL=postgres://postgres@localhost:5432/postgres scripts/bench.sh
set -euo pipefail

ADMIN_URL=${ADMIN_URL:-postgres://postgres@localhost:5432/postgres?sslmode=disable}
DB=webhookd_bench
DB_URL=${ADMIN_URL/\/postgres\?/\/$DB?}
MESSAGES=${MESSAGES:-3000}   # burst size (x ENDPOINTS deliveries)
STEADY=${STEADY:-1000}       # messages in the steady run
RATE=${RATE:-20}             # steady send rate, messages/s
ENDPOINTS=${ENDPOINTS:-20}   # each message fans out to every endpoint
LATENCY=${LATENCY:-20ms}     # receiver response time
REPLICAS=${REPLICAS:-"1 2 4"}
KEY=bench-api-key-0123456789

bin=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$bin"' EXIT
go build -o "$bin" ./cmd/webhookd ./cmd/loadgen

start() { # role port
  DATABASE_URL=$DB_URL ROLE=$1 LISTEN_ADDR=127.0.0.1:$2 API_KEY=$KEY ALLOW_PRIVATE_NETWORKS=true \
    "$bin/webhookd" >/dev/null 2>&1 &
}

echo "machine: $(nproc) CPUs, $(uname -sm); Postgres on the same machine"
echo "load: $ENDPOINTS endpoints (max_in_flight 4), receiver latency $LATENCY, 16 sends per dispatcher"
for n in $REPLICAS; do
  psql -q "$ADMIN_URL" -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB"
  start api 8090
  until curl -fs localhost:8090/readyz >/dev/null; do sleep 0.2; done
  for i in $(seq "$n"); do start dispatcher $((8090 + i)); done
  sleep 1

  echo; echo "== $n dispatcher replica(s) =="
  for mode in steady burst; do
    if [ $mode = burst ]; then args=(-messages "$MESSAGES"); else args=(-messages "$STEADY" -rate "$RATE"); fi
    printf '%s: ' $mode
    "$bin/loadgen" -api http://localhost:8090 -key $KEY -endpoints "$ENDPOINTS" -latency "$LATENCY" "${args[@]}" | tail -2 | paste -sd" " -
  done
  kill $(jobs -p); wait 2>/dev/null || true
done
