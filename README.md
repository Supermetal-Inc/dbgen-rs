# dbgen-rs

TPC-H load generator. Generates data at configurable scale factors, loads via bulk protocols and batched inserts, and continuously applies CDC mutations.


## Why TPC-H

We benchmark snapshot (backfill) performance, which is bounded by replications tool rather than the source database. TPC-H is a good fit because:

- **Clean scaling.** SF=1 produces ~6M rows in `lineitem`; SF=100 produces ~600M. Throughput numbers (rows/sec, GB/hr) stay meaningful without synthetic inflation.
- **Widely understood.** TPC-H scale factors are a common reference point, making results comparable without explaining a custom schema.
- **Skewed table sizes.** `lineitem` is ~75% of all rows at any scale factor, while `region` and `nation` are trivial. A single run exercises both inter-table parallelism and intra-table chunking.

## Build

```
cargo build --release
```

Binaries: `target/release/pg`, `target/release/sqlserver`, `target/release/oracle`, `target/release/mysql`

The Oracle binary requires the Oracle Instant Client libraries to be installed and on the linker path.

## Usage

Binaries share the same subcommands and TPC-H options.

### Snapshot

Load TPC-H tables from scratch.

```
pg snapshot [OPTIONS]
sqlserver snapshot [OPTIONS]
oracle snapshot [OPTIONS]
mysql snapshot [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scale-factor` | `0.01` | TPC-H scale factor (0.01 ~ 10K rows, 1.0 ~ 6M rows) |
| `--tables` | `all` | Comma-separated list or `all` |
| `--drop` | `false` | Drop tables before creating |
| `--create-only` | `false` | Create tables without loading data |

Tables are loaded in parallel. PostgreSQL uses `COPY BINARY`, SQL Server uses bulk insert, Oracle uses array-bound batch inserts. MySQL uses parameterized multi-row inserts (up to 500 rows per statement), with exact decimal strings and InnoDB primary keys.

### CDC

Continuously upsert rows into existing tables.

```
pg cdc [OPTIONS]
sqlserver cdc [OPTIONS]
oracle cdc [OPTIONS]
mysql cdc [OPTIONS]
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

**Oracle** (`oracle`)

| Flag | Default |
|------|---------|
| `--host` | `localhost` |
| `--port` | `1521` |
| `--database` | `FREEPDB1` |
| `--user` | `test` |
| `--password` | `test` |

**MySQL** (`mysql`, tested with MySQL 8.1)

| Flag | Default |
|------|---------|
| `--host` | `localhost` |
| `--port` | `3306` |
| `--database` | `supermetal` |
| `--user` | `root` |
| `--password` | `mysql` |

Create the database before running. The user needs SELECT, INSERT, UPDATE, CREATE,
DROP and CREATE TEMPORARY TABLES privileges. The loader uses strict SQL mode and
UTF-8 sessions. CDC staging tables are session-local and disappear on disconnect;
use row-based binlogs so replication captures target-table upserts, not staging data.
Configure the server's binlog/replication user separately in the benchmark harness.

```bash
cargo build --bin mysql -j 2
./target/debug/mysql --host 127.0.0.1 --port 3306 snapshot --scale-factor 0.01 --drop
./target/debug/mysql --host 127.0.0.1 --port 3306 cdc --scale-factor 0.01 --rate 100
```

CDC runs until stopped. Rate is a pacing target, not a guaranteed achieved rate;
upserts are batched and the shared timer may catch up after delays. Low total rates
are assigned to the first selected table when there are fewer ops/sec than tables.
Use SF0.01 or larger for all-table smoke tests: very small scales can generate
repeated `partsupp` keys, which are rejected rather than silently discarded.
Snapshot or CDC SQL failures return a nonzero exit status; a failed snapshot can
leave a partial load, so use `--drop` to restart it from scratch.

### MySQL verification

Unit tests: `cargo test --bin mysql -j 2`. The opt-in integration test **drops all
eight TPC-H tables** in its selected database. Use a disposable local instance:

```bash
podman run -d --name dbgen-mysql-test --cpus 2 --memory 1g \
  -p 127.0.0.1:13316:3306 -e MYSQL_ROOT_PASSWORD=mysql -e MYSQL_DATABASE=supermetal \
  docker.io/library/mysql:8.1 --server-id=71 --log-bin=mysql-bin --binlog-format=ROW
# Wait until mysqladmin ping succeeds, then:
MYSQL_TEST_PORT=13316 cargo test --bin mysql mysql_snapshot_and_cdc_roundtrip -- --ignored
podman rm -f dbgen-mysql-test
```

The test checks empty `--create-only` tables, every generated value at SF0.01,
composite-key upserts, insert/update branches, staging cleanup, invalid-input rejection and snapshot/CDC failure
propagation. `MYSQL_TEST_DATABASE` and `MYSQL_TEST_PASSWORD` override test defaults.

### Tables

region, nation, supplier, customer, part, partsupp, orders, lineitem
