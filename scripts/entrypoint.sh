#!/bin/sh
set -eu

echo "[entrypoint] running migrations..."
bun run scripts/migrate.ts

echo "[entrypoint] starting server..."
exec bun run src/index.ts


