use anyhow::{Result, anyhow, bail};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use clap::Parser;
use dbgen_rs::tpch::{
    self, Mode, TpchBackend, TypedValue, UNIX_EPOCH, batch_rows, format_decimal128,
    get_comment_column, get_pk_columns,
};
use log::info;
use oracle::pool::{GetMode, Pool, PoolBuilder};
use oracle::sql_type::ToSql;
use std::sync::Arc;
use std::time::Duration;

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
        let mut pb = PoolBuilder::new(&cli.user, &cli.password, &conn_str);
        pb.min_connections(0);
        pb.max_connections(u32::try_from(cli.mode.snapshot_workers().max(16))?);
        pb.timeout(Duration::from_secs(60))?;
        pb.get_mode(GetMode::Wait);
        pb.stmt_cache_size(1024);
        pb.max_lifetime_connection(Duration::from_secs(3600))?;
        Ok(Self {
            pool: Arc::new(pb.build()?),
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

enum OraValue<'a> {
    I32(Option<i32>),
    I64(Option<i64>),
    Str(Option<&'a str>),
    Date(Option<NaiveDate>),
    Num(Option<String>),
}

impl OraValue<'_> {
    fn as_to_sql(&self) -> &dyn ToSql {
        match self {
            OraValue::I32(v) => v,
            OraValue::I64(v) => v,
            OraValue::Str(v) => v,
            OraValue::Date(v) => v,
            OraValue::Num(v) => v,
        }
    }
}

fn typed_value_to_ora<'a>(val: &TypedValue<'a>) -> OraValue<'a> {
    match val {
        TypedValue::Int32(v) => OraValue::I32(*v),
        TypedValue::Int64(v) => OraValue::I64(*v),
        TypedValue::Utf8(v) => OraValue::Str(*v),
        TypedValue::Date32(v) => {
            OraValue::Date(v.map(|days| UNIX_EPOCH + chrono::Duration::days(days as i64)))
        }
        TypedValue::Decimal128(v, scale) => {
            OraValue::Num(v.map(|val| format_decimal128(val, *scale)))
        }
    }
}

// ORA-00942: table or view does not exist.
fn drop_table_sync(conn: &oracle::Connection, table: &str) -> Result<()> {
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
    conn: &oracle::Connection,
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

fn bulk_insert_sync(
    conn: &oracle::Connection,
    table: &str,
    schema: &Schema,
    batches: impl Iterator<Item = RecordBatch> + Send,
    snapshot: Option<(
        &dbgen_rs::snapshot::Progress,
        &tokio_util::sync::CancellationToken,
    )>,
) -> Result<usize> {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!(":{}", i)).collect();
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(table),
        cols.join(", "),
        placeholders.join(", ")
    );

    let mut batch = conn.batch(&sql, 10000).build()?;
    let mut total = 0;
    let check_cancel = || -> Result<()> {
        if snapshot.is_some_and(|(_, token)| token.is_cancelled()) {
            bail!("snapshot cancelled");
        }
        Ok(())
    };

    for rb in batches {
        let mut iter = batch_rows(&rb)?;
        let mut vals = Vec::with_capacity(rb.num_columns());
        while iter.next_into(&mut vals) {
            check_cancel()?;
            for (index, val) in vals.iter().enumerate() {
                batch.set(index + 1, typed_value_to_ora(val).as_to_sql())?;
            }
            batch.append_row(&[])?;
        }
        total += rb.num_rows();
    }

    batch.execute()?;
    if let Some((progress, _)) = snapshot {
        progress.add_written(total);
    }
    check_cancel()?;
    conn.commit()?;
    if let Some((progress, _)) = snapshot {
        progress.add_committed(total);
    }
    Ok(total)
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
        let conn = self.pool.get()?;
        drop_table_sync(&conn, table)
    }

    async fn create_table(&self, table: &str, schema: &Schema) -> Result<()> {
        let conn = self.pool.get()?;
        let pk = get_pk_columns(table);
        create_table_sync(&conn, table, schema, Some(pk))
    }

    async fn create_temp_table(&self, table: &str, schema: &Schema) -> Result<String> {
        let name = format!("_temp_{}_{}", table, std::process::id());
        let conn = self.pool.get()?;
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
        let conn = self.pool.get()?;
        bulk_insert_sync(&conn, table, schema, batches, None)
    }

    async fn upsert_from_temp(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        let conn = self.pool.get()?;
        let sql = generate_merge_sql(target, temp, schema);
        conn.execute(&sql, &[])?;
        conn.commit()?;
        Ok(())
    }

    async fn truncate_table(&self, table: &str) -> Result<()> {
        let conn = self.pool.get()?;
        conn.execute(&format!("TRUNCATE TABLE {}", quote_ident(table)), &[])?;
        Ok(())
    }

    async fn drop_temp_table(&self, table: &str) -> Result<()> {
        let conn = self.pool.get()?;
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
            let conn = pool.get()?;
            let schema = batch.schema();
            let result = bulk_insert_sync(
                &conn,
                &table,
                &schema,
                std::iter::once(batch),
                Some((&progress, &cancel)),
            );
            if result.is_err() {
                // Preserve the original write/commit error. A failed commit reply may
                // still mean the server committed; never retry automatically.
                let _ = conn.rollback();
            }
            result.map(|_| ())
        })
        .await?
        .map_err(|e| {
            // ORA-01013: OCI's SIGINT handler still runs after ours and cancels the call.
            let interrupted = e
                .downcast_ref::<oracle::Error>()
                .and_then(oracle::Error::oci_code)
                == Some(1013);
            if interrupted {
                anyhow!("snapshot cancelled")
            } else {
                e
            }
        })
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

    // OCI installs its own SIGINT handler during init and doesn't restore it
    // (https://github.com/kubo/rust-oracle/issues/37). Tokio's handler, registered
    // after the pool is built, chains to OCI's rather than replacing it, so Ctrl-C
    // also cancels in-flight calls with ORA-01013. CDC exits here. Snapshots cancel
    // through the coordinator.
    if matches!(&cli.mode, Mode::Cdc(_)) {
        tokio::spawn(async {
            let _ = tokio::signal::ctrl_c().await;
            std::process::exit(130);
        });
    }

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
        let conn = backend.pool.get()?;
        drop_table_sync(&conn, "dbgen_snapshot_test")?;
        conn.execute(
            "CREATE TABLE \"dbgen_snapshot_test\" (\"id\" NUMBER(10) PRIMARY KEY)",
            &[],
        )?;
        let client = &conn;
        let result = check_snapshot_chunks(backend.clone(), "dbgen_snapshot_test", || async move {
            let row = client.query_row("SELECT COUNT(*) FROM \"dbgen_snapshot_test\"", &[])?;
            Ok(row.get::<_, u64>(0)?)
        })
        .await;
        drop_table_sync(&conn, "dbgen_snapshot_test")?;
        result
    }
}
