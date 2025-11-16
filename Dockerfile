FROM oven/bun:1 as base

WORKDIR /app

# Install deps first (better layer caching)
COPY bun.lock package.json ./
RUN bun install --frozen-lockfile

# Copy source
COPY . .

# Make entrypoint executable
RUN chmod +x scripts/entrypoint.sh

# Expose default port and set defaults
ENV PORT=3000
ENV DUCKDB_PATH=/data/main.duckdb

# Declare a volume for DuckDB data (maps via compose)
VOLUME ["/data"]

EXPOSE 3000

CMD ["./scripts/entrypoint.sh"]

