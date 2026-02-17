# Solana Geyser Plugin for ClickHouse

Simple production-ready setup for streaming account updates from a Solana validator into ClickHouse.

## What This Plugin Does

- Receives account update notifications from Geyser.
- Buffers updates through an in-memory channel.
- Writes updates in batches to ClickHouse with retry/backoff.
- Automatically creates database and table schema (optional).

## Prerequisites

- Rust stable toolchain
- Solana validator / `solana-test-validator`
- Docker + Docker Compose

## 1) Start ClickHouse

```bash
docker compose up -d
```

ClickHouse will be available at:
- HTTP: `http://127.0.0.1:8123`
- Native: `127.0.0.1:9000`

## 2) Build Plugin

```bash
cargo build --release
```

Plugin artifact will be one of:
- macOS: `target/release/libsolana_geyser_clickhouse.dylib`
- Linux: `target/release/libsolana_geyser_clickhouse.so`

## 3) Configure Geyser

Update `config.json`:

- Set `libpath` to the absolute path of your built plugin artifact.
- Keep `clickhouse_url` as `http://127.0.0.1:8123` unless running remotely.

Example:

```json
{
  "libpath": "/absolute/path/to/libsolana_geyser_clickhouse.so",
  "clickhouse_url": "http://127.0.0.1:8123",
  "clickhouse_database": "solana",
  "clickhouse_table": "accounts",
  "batch_size": 1000,
  "batch_timeout_ms": 5000,
  "max_retries": 3,
  "channel_capacity": 100000,
  "create_schema": true
}
```

## 4) Run Validator with Plugin

```bash
solana-test-validator --geyser-plugin-config ./config.json
```

## 5) Verify Data Flow

Trigger account activity (transfer, airdrop, etc.), then query ClickHouse:

```bash
docker exec -it solana-clickhouse clickhouse-client --query "
SELECT slot, pubkey, lamports, write_version
FROM solana.accounts
ORDER BY slot DESC
LIMIT 10"
```

## Table Schema

The plugin creates this table automatically when `create_schema=true`:

- `slot UInt64`
- `pubkey String`
- `owner String`
- `lamports UInt64`
- `executable UInt8`
- `rent_epoch UInt64`
- `data String` (hex-encoded account data)
- `updated_at_unix_ms Int64`
- `txn_signature Nullable(String)`
- `write_version UInt64`

Engine:
- `ReplacingMergeTree(write_version)`
- `ORDER BY (pubkey, slot)`

## Operational Notes

- Keep `channel_capacity` high enough for your account update throughput.
- Tune `batch_size` and `batch_timeout_ms` for your latency/throughput target.
- On unload, the plugin drains remaining channel messages before worker exit.
