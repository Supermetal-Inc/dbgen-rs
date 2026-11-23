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
| `--nested` | `false` | Load `lineitem` in the nested shape (PostgreSQL only) |
| `--workers` | `8` | Shared snapshot writer count; connection pools grow to match |
| `--commit-rows` | `100000` | Maximum rows per snapshot transaction |

Tables are loaded in parallel. PostgreSQL uses `COPY BINARY`, SQL Server uses bulk insert, Oracle uses array-bound batch inserts, MySQL uses `LOAD DATA LOCAL INFILE`.

Snapshots default to 8 shared workers and at most 100,000 rows per commit:

```sh
oracle snapshot --scale-factor 1 --workers 8 --commit-rows 100000
```

Workers share chunks across tables, including the final large table. At most one
Arrow chunk per worker is resident, plus generator and driver buffers; generation
is serialized and overlaps writes. Connections and commits remain backend-specific.

Progress logs every five seconds distinguish generated rows, successfully written
batches, and acknowledged committed rows. Written rows can later roll back;
a failed commit response has an unknown outcome. Errors and Ctrl-C stop new work
and roll back unfinished chunks. PostgreSQL, MySQL and SQL Server let active database
calls finish first, while Oracle's client cancels them. A second Ctrl-C exits
immediately, and the database rolls back open transactions when the connection drops. Earlier commits
remain: this is **not an atomic snapshot or a resumable load**. Recreate the selected
tables before retrying, for example with `--drop` on a disposable target. CDC is unchanged.

Backend transaction tests are ignored by default. Run them one target at a time against
a disposable localhost database, for example
`DBGEN_TEST_PORT=5432 DBGEN_TEST_PASSWORD=... cargo test --bin pg -- --ignored`.
They use database `bench` with the default superuser (Oracle: `FREEPDB1`, user `bench`).

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
| `--nested` | `false` | Use the nested `lineitem` shape. Must match the snapshot |

Runs indefinitely. Each table gets a dedicated worker that upserts batches at the target rate via temp table + merge/upsert.

### Nested shape

`--nested` borrows the nested TPC-H layout from [apache/datafusion-comet#7044](https://github.com/apache/datafusion-comet/pull/7044). It tests the JSON decode and write paths, which flat TPC-H does not touch.

`lineitem` keeps the primary key (`l_orderkey`, `l_linenumber`), `l_partkey`, `l_suppkey` and `l_shipdate` as columns. All other columns move into `lineitem_data JSONB`, which holds an array with exactly one object. Other tables stay flat. CDC updates `l_comment` inside the array element.

Flatten the nested table to compare it with a flat load of the same scale factor:

```sql
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, d.l_quantity, d.l_extendedprice,
       d.l_discount, d.l_tax, d.l_returnflag, d.l_linestatus, l_shipdate, d.l_commitdate,
       d.l_receiptdate, d.l_shipinstruct, d.l_shipmode, d.l_comment
FROM lineitem,
     jsonb_to_recordset(lineitem_data) AS d(
       l_quantity numeric(15,2), l_extendedprice numeric(15,2), l_discount numeric(15,2),
       l_tax numeric(15,2), l_returnflag text, l_linestatus text, l_commitdate date,
       l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)
ORDER BY l_orderkey, l_linenumber;
```

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

**MySQL** (`mysql`)

| Flag | Default |
|------|---------|
| `--host` | `localhost` |
| `--port` | `3306` |
| `--database` | `supermetal` |
| `--user` | `root` |
| `--password` | `mysql` |

Enable `local_infile=ON` on the MySQL server.

### Tables

region, nation, supplier, customer, part, partsupp, orders, lineitem
