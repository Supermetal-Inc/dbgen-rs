use anyhow::{Result, bail, ensure};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use dbgen_rs::tpch::{self, Mode, TpchBackend, get_comment_column, get_pk_columns};
use log::info;
use oracledb::{Connection, Pool, PoolConfig};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "oracle")]
#[command(about = "TPC-H load generator for Oracle")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 1521)]
    port: u16,
    #[arg(long, default_value = "FREEPDB1")]
    database: String,
    #[arg(long, default_value = "test")]
    user: String,
    #[arg(long, default_value = "test")]
    password: String,
}

struct OracleBackend {
    pool: Arc<Pool>,
}

impl OracleBackend {
    fn new(cli: &Cli) -> Result<Self> {
        let conn_str = format!("//{}:{}/{}", cli.host, cli.port, cli.database);
        let config = PoolConfig::default()
            .set_credentials(&cli.user, &cli.password)
            .set_connect_string(&conn_str)?
            .set_min_connections(0)
            .set_max_connections(cli.mode.snapshot_workers().max(16))
            .set_stmtcachesize(1024);
        Ok(Self {
            pool: Arc::new(oracledb::create_pool(config)?),
        })
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn arrow_type_to_sql(dt: &DataType) -> Result<String> {
    Ok(match dt {
        DataType::Int32 => "NUMBER(10)".into(),
        DataType::Int64 => "NUMBER(19)".into(),
        DataType::Float32 => "BINARY_FLOAT".into(),
        DataType::Float64 => "BINARY_DOUBLE".into(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "VARCHAR2(4000 CHAR)".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(p, s) => format!("NUMBER({}, {})", p, s),
        _ => bail!("unsupported Arrow type for Oracle: {:?}", dt),
    })
}

// ORA-00942: table or view does not exist.
fn drop_table_sync(conn: &Connection, table: &str) -> Result<()> {
    let plsql = format!(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE {}'; \
         EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
        quote_ident(table).replace('\'', "''")
    );
    conn.execute(&plsql, &[])?;
    Ok(())
}

// ORA-00955: name is already used by an existing object.
fn create_table_sync(
    conn: &Connection,
    table: &str,
    schema: &Schema,
    pk_cols: Option<&[&str]>,
) -> Result<()> {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let nullable = if f.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!(
                "{} {}{}",
                quote_ident(f.name()),
                arrow_type_to_sql(f.data_type())?,
                nullable
            ))
        })
        .collect::<Result<_>>()?;

    let pk_clause = match pk_cols {
        Some(pk) => {
            let list: Vec<String> = pk.iter().map(|c| quote_ident(c)).collect();
            format!(", PRIMARY KEY ({})", list.join(", "))
        }
        None => String::new(),
    };

    let create_sql = format!(
        "CREATE TABLE {} ({}{})",
        quote_ident(table),
        cols.join(", "),
        pk_clause
    );
    let plsql = format!(
        "BEGIN EXECUTE IMMEDIATE '{}'; \
         EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;",
        create_sql.replace('\'', "''")
    );
    conn.execute(&plsql, &[])?;
    Ok(())
}

fn insert_sql(table: &str, schema: &Schema) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!(":{}", i)).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(table),
        cols.join(", "),
        placeholders.join(", ")
    )
}

fn generate_merge_sql(target: &str, temp: &str, schema: &Schema) -> String {
    let pk = get_pk_columns(target);
    let comment = get_comment_column(target);
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let join = pk
        .iter()
        .map(|c| format!("T.{}=S.{}", quote_ident(c), quote_ident(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let update = cols
        .iter()
        .filter(|c| !pk.contains(*c))
        .map(|c| {
            let qc = quote_ident(c);
            if *c == comment {
                format!(
                    "T.{}=SUBSTR(S.{},1,40) || ' CDC:' || TO_CHAR(SYSTIMESTAMP,'YYYY-MM-DD HH24:MI:SS.FF3')",
                    qc, qc
                )
            } else {
                format!("T.{}=S.{}", qc, qc)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let ins_cols = cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let ins_vals = cols
        .iter()
        .map(|c| format!("S.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "MERGE INTO {} T USING {} S ON ({}) WHEN MATCHED THEN UPDATE SET {} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        quote_ident(target),
        quote_ident(temp),
        join,
        update,
        ins_cols,
        ins_vals
    )
}

impl TpchBackend for OracleBackend {
    async fn drop_table(&self, table: &str) -> Result<()> {
        let conn = self.pool.acquire()?;
        drop_table_sync(&conn, table)
    }

    async fn create_table(&self, table: &str, schema: &Schema) -> Result<()> {
        let conn = self.pool.acquire()?;
        let pk = get_pk_columns(table);
        create_table_sync(&conn, table, schema, Some(pk))
    }

    async fn create_temp_table(&self, table: &str, schema: &Schema) -> Result<String> {
        let name = format!("_temp_{}_{}", table, std::process::id());
        let conn = self.pool.acquire()?;
        drop_table_sync(&conn, &name)?;
        create_table_sync(&conn, &name, schema, None)?;
        Ok(name)
    }

    async fn copy_batches(
        &self,
        table: &str,
        schema: &Schema,
        batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
    ) -> Result<usize> {
        let conn = self.pool.acquire()?;
        let sql = insert_sql(table, schema);
        let mut total = 0;
        for batch in batches {
            total += batch.num_rows();
            conn.execute_batch(&sql, batch)?;
        }
        conn.commit()?;
        Ok(total)
    }

    async fn upsert_from_temp(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        let conn = self.pool.acquire()?;
        let sql = generate_merge_sql(target, temp, schema);
        conn.execute(&sql, &[])?;
        conn.commit()?;
        Ok(())
    }

    async fn truncate_table(&self, table: &str) -> Result<()> {
        let conn = self.pool.acquire()?;
        conn.execute(&format!("TRUNCATE TABLE {}", quote_ident(table)), &[])?;
        Ok(())
    }

    async fn drop_temp_table(&self, table: &str) -> Result<()> {
        let conn = self.pool.acquire()?;
        drop_table_sync(&conn, table)
    }

    async fn write_snapshot_chunk(
        &self,
        table: String,
        batch: RecordBatch,
        progress: Arc<dbgen_rs::snapshot::Progress>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            ensure!(!cancel.is_cancelled(), "snapshot cancelled");
            let conn = pool.acquire()?;
            let rows = batch.num_rows();
            let sql = insert_sql(&table, &batch.schema());
            // The chunk is one array insert in one round trip, so cancellation can only
            // stop it before the insert or before the commit.
            let result = (|| {
                conn.execute_batch(&sql, batch)?;
                progress.add_written(rows);
                ensure!(!cancel.is_cancelled(), "snapshot cancelled");
                conn.commit()?;
                Ok(())
            })();
            match &result {
                Ok(()) => progress.add_committed(rows),
                // Preserve the original write/commit error. A failed commit reply may
                // still mean the server committed; never retry automatically.
                Err(_) => {
                    let _ = conn.rollback();
                }
            }
            result
        })
        .await?
    }

    fn is_blocking() -> bool {
        true
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    info!("Target: {}:{}/{}", cli.host, cli.port, cli.database);
    let backend = Arc::new(OracleBackend::new(&cli)?);
    tpch::run(backend, cli.mode).await?;
    Ok(())
}

#[cfg(test)]
mod snapshot_integration_tests {
    use super::*;
    use dbgen_rs::testing::{check_snapshot_chunks, cli_args};

    #[tokio::test]
    #[ignore = "requires a disposable localhost FREEPDB1 with user bench and DBGEN_TEST_PORT/PASSWORD"]
    async fn commits_failures_and_cancellation() -> Result<()> {
        let cli = Cli::try_parse_from(cli_args("oracle", "bench", "FREEPDB1")?)?;
        let backend = Arc::new(OracleBackend::new(&cli)?);
        let conn = backend.pool.acquire()?;
        drop_table_sync(&conn, "dbgen_snapshot_test")?;
        conn.execute(
            "CREATE TABLE \"dbgen_snapshot_test\" (\"id\" NUMBER(10) PRIMARY KEY)",
            &[],
        )?;
        let client = &conn;
        let result = check_snapshot_chunks(backend.clone(), "dbgen_snapshot_test", || async move {
            let row = client.query_row("SELECT COUNT(*) FROM \"dbgen_snapshot_test\"", &[])?;
            Ok(row.get::<i64>(0)? as u64)
        })
        .await;
        drop_table_sync(&conn, "dbgen_snapshot_test")?;
        result
    }
}
