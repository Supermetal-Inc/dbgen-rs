# dbgen-rs

TPC-H load generator. Generates data at configurable scale factors, loads via native bulk protocols, and continuously applies CDC mutations.


## Why TPC-H

We benchmark snapshot (backfill) performance, which is bounded by replications tool rather than the source database. TPC-H is a good fit because:

- **Clean scaling.** SF=1 produces ~6M rows in `lineitem`; SF=100 produces ~600M. Throughput numbers (rows/sec, GB/hr) stay meaningful without synthetic inflation.
- **Widely understood.** TPC-H scale factors are a common reference point, making results comparable without explaining a custom schema.
- **Skewed table sizes.** `lineitem` is ~75% of all rows at any scale factor, while `region` and `nation` are trivial. A single run exercises both inter-table parallelism and intra-table chunking.

## Build

```
cargo build --release
```

Binaries: `target/release/pg`, `target/release/sqlserver`

## Usage

Binaries share the same subcommands and TPC-H options.

### Snapshot

Load TPC-H tables from scratch.

```
pg snapshot [OPTIONS]
sqlserver snapshot [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scale-factor` | `0.01` | TPC-H scale factor (0.01 ~ 10K rows, 1.0 ~ 6M rows) |
| `--tables` | `all` | Comma-separated list or `all` |
| `--drop` | `false` | Drop tables before creating |
| `--create-only` | `false` | Create tables without loading data |

Tables are loaded in parallel. PostgreSQL uses `COPY BINARY`, SQL Server uses bulk insert.

### CDC

Continuously upsert rows into existing tables.

```
pg cdc [OPTIONS]
sqlserver cdc [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scale-factor` | `1.0` | TPC-H scale factor for data generation |
| `--tables` | `lineitem,orders` | Tables to mutate |
| `--rate` | `100` | Target upserts/sec (total, split across tables) |

Runs indefinitely. Each table gets a dedicated worker that upserts batches at the target rate via temp table + merge/upsert.

### Connection flags

**PostgreSQL** (`pg`)

| Flag | Default |
|------|---------|
| `--host` | `localhost` |
| `--port` | `5432` |
| `--database` | `postgres` |
| `--user` | `postgres` |
| `--password` | `postgres` |

**SQL Server** (`sqlserver`)

| Flag | Default |
|------|---------|
| `--host` | `localhost` |
| `--port` | `1433` |
| `--database` | `supermetal` |
| `--user` | `sa` |
| `--password` | `p@ssword01` |

### Tables

region, nation, supplier, customer, part, partsupp, orders, lineitem
