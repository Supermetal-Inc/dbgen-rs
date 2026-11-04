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

Tables are loaded in parallel. PostgreSQL uses `COPY BINARY`, SQL Server uses bulk insert, Oracle uses array-bound batch inserts, MySQL uses `LOAD DATA LOCAL INFILE`.

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
