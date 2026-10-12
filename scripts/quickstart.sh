#!/usr/bin/env bash
# Runs the README's quickstart block exactly as written, then checks the
# mock receiver accepted the signature. This is the phase 4 exit gate: a
# new user gets from clone to a verified delivery with only the README.
# CI runs it on every push. Needs Docker; leaves the stack running.
set -euo pipefail
cd "$(dirname "$0")/.."

# The first ```sh block after the quickstart marker.
block=$(awk '/<!-- quickstart:/ {found=1; next}
             found && /^```sh$/ {inside=1; next}
             inside && /^```$/ {exit}
             inside {print}' README.md)
if [ -z "$block" ]; then
  echo "no quickstart block in README.md" >&2
  exit 1
fi

bash -euo pipefail -x -c "$block
docker compose logs mockreceiver | grep \"\$MSG\" | grep -q '\"signature\":\"ok\"'
echo 'quickstart: delivered and verified'"
