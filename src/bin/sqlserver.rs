use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use dbgen_rs::tpch::{
    self, Mode, TpchBackend, TypedValue, UNIX_EPOCH, batch_rows, get_comment_column, get_pk_columns,
};
use deadpool_tiberius::{Manager, Pool};
use log::info;
use std::sync::Arc;
use tiberius::{IntoSql, TokenRow, numeric::Numeric};

#[derive(Parser)]
#[command(name = "sqlserver")]
#[command(about = "TPC-H load generator for SQL Server")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 1433)]
    port: u16,
    #[arg(long, default_value = "supermetal")]
    database: String,
    #[arg(long, default_value = "sa")]
    user: String,
    #[arg(long, default_value = "p@ssword01")]
    password: String,
}

struct SqlServerBackend {
    pool: Pool,
}

impl SqlServerBackend {
    fn new(cli: &Cli) -> Result<Self> {
        let manager = Manager::new()
            .host(&cli.host)
            .port(cli.port)
            .database(&cli.database)
            .authentication(tiberius::AuthMethod::sql_server(&cli.user, &cli.password))
            .trust_cert()
            .max_size(cli.mode.snapshot_workers().max(16));
        Ok(Self {
            pool: manager.create_pool()?,
        })
    }
}

// Tiberius has no RAII transaction. Discard the connection on errors or dropped
// futures so an open transaction/protocol stream can never return to the pool.
struct SnapshotConnection {
    client: Option<deadpool_tiberius::deadpool::managed::Object<Manager>>,
}
impl Drop for SnapshotConnection {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            drop(deadpool_tiberius::deadpool::managed::Object::take(client));
        }
    }
}

async fn bulk_batch(
    client: &mut deadpool_tiberius::Client,
    table: &str,
    batch: &RecordBatch,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<usize> {
    let mut iter = batch_rows(batch)?;
    let mut vals = Vec::with_capacity(batch.num_columns());
    let mut req = client.bulk_insert(table).await?;
    while iter.next_into(&mut vals) {
        anyhow::ensure!(
            !cancel.is_some_and(|c| c.is_cancelled()),
            "snapshot cancelled"
        );
        let mut row = TokenRow::with_capacity(vals.len());
        for val in &vals {
            value_to_sql(&mut row, val);
        }
        req.send(row).await?;
    }
    Ok(req.finalize().await?.total() as usize)
}

fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

fn value_to_sql(row: &mut TokenRow<'static>, val: &TypedValue) {
    match val {
        TypedValue::Int32(v) => row.push(v.into_sql()),
        TypedValue::Int64(v) => row.push(v.into_sql()),
        TypedValue::Utf8(v) => row.push(v.map(str::to_owned).into_sql()),
        TypedValue::Date32(v) => {
            let date = v.map(|days| UNIX_EPOCH + chrono::Duration::days(days as i64));
            row.push(date.into_sql());
        }
        TypedValue::Decimal128(v, scale) => {
            let numeric = v.map(|val| Numeric::new_with_scale(val, *scale as u8));
            row.push(numeric.into_sql());
        }
    }
}

fn arrow_type_to_sql(dt: &DataType) -> Result<String> {
    Ok(match dt {
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "FLOAT".into(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "NVARCHAR(MAX)".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(p, s) => format!("DECIMAL({}, {})", p, s),
        _ => bail!("unsupported Arrow type for SQL Server: {:?}", dt),
    })
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
                    "T.{}=LEFT(S.{},40)+' CDC:'+CONVERT(varchar,GETDATE(),121)",
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
        "MERGE {} AS T USING {} AS S ON {} WHEN MATCHED THEN UPDATE SET {} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});",
        quote_ident(target),
        quote_ident(temp),
        join,
        update,
        ins_cols,
        ins_vals
    )
}

impl TpchBackend for SqlServerBackend {
    async fn drop_table(&self, table: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(&format!("DROP TABLE IF EXISTS {}", quote_ident(table)), &[])
            .await?;
        Ok(())
    }

    async fn create_table(&self, table: &str, schema: &Schema) -> Result<()> {
        let pk = get_pk_columns(table);
        let pk_list = pk
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let cols: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let null = if f.is_nullable() { "NULL" } else { "NOT NULL" };
                Ok(format!(
                    "{} {} {}",
                    quote_ident(f.name()),
                    arrow_type_to_sql(f.data_type())?,
                    null
                ))
            })
            .collect::<Result<_>>()?;
        let qt = quote_ident(table);
        let ddl = format!(
            "IF OBJECT_ID(N'{}','U') IS NULL CREATE TABLE {} ({}, PRIMARY KEY ({}))",
            table,
            qt,
            cols.join(", "),
            pk_list
        );
        self.pool.get().await?.execute(&ddl, &[]).await?;
        Ok(())
    }

    async fn create_temp_table(&self, table: &str, schema: &Schema) -> Result<String> {
        let name = format!("##temp_{}_{}", table, std::process::id());
        let cols: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                Ok(format!(
                    "{} {}",
                    quote_ident(f.name()),
                    arrow_type_to_sql(f.data_type())?
                ))
            })
            .collect::<Result<_>>()?;
        let mut client = self.pool.get().await?;
        client
            .execute(&format!("DROP TABLE IF EXISTS {}", quote_ident(&name)), &[])
            .await?;
        client
            .execute(
                &format!("CREATE TABLE {} ({})", quote_ident(&name), cols.join(", ")),
                &[],
            )
            .await?;
        Ok(name)
    }

    async fn copy_batches(
        &self,
        table: &str,
        _schema: &Schema,
        batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
    ) -> Result<usize> {
        let mut client = self.pool.get().await?;
        let mut total = 0;
        for batch in batches {
            total += bulk_batch(&mut client, table, &batch, None).await?;
        }
        Ok(total)
    }

    async fn upsert_from_temp(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        let sql = generate_merge_sql(target, temp, schema);
        self.pool.get().await?.execute(&sql, &[]).await?;
        Ok(())
    }

    async fn truncate_table(&self, table: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(&format!("TRUNCATE TABLE {}", quote_ident(table)), &[])
            .await?;
        Ok(())
    }

    async fn drop_temp_table(&self, table: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(&format!("DROP TABLE IF EXISTS {}", quote_ident(table)), &[])
            .await?;
        Ok(())
    }

    async fn write_snapshot_chunk(
        &self,
        table: String,
        batch: RecordBatch,
        progress: Arc<dbgen_rs::snapshot::Progress>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        anyhow::ensure!(!cancel.is_cancelled(), "snapshot cancelled");
        let mut guard = SnapshotConnection {
            client: Some(self.pool.get().await?),
        };
        let client = guard.client.as_mut().unwrap();
        client
            .simple_query("SET XACT_ABORT ON; BEGIN TRANSACTION")
            .await?
            .into_results()
            .await?;
        let rows = bulk_batch(client, &table, &batch, Some(&cancel)).await?;
        anyhow::ensure!(rows == batch.num_rows(), "bulk insert row count mismatch");
        progress
            .written
            .fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
        anyhow::ensure!(!cancel.is_cancelled(), "snapshot cancelled");
        client
            .simple_query("COMMIT TRANSACTION")
            .await?
            .into_results()
            .await?;
        // Only a successfully committed connection can return to the pool.
        drop(guard.client.take());
        progress
            .committed
            .fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Tiberius logs each BEGIN and COMMIT at INFO, two lines per snapshot chunk.
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info,tiberius=warn"),
    )
    .init();
    let cli = Cli::parse();
    info!("Target: {}:{}/{}", cli.host, cli.port, cli.database);
    let backend = Arc::new(SqlServerBackend::new(&cli)?);
    tpch::run(backend, cli.mode).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiberius::ColumnData;

    #[test]
    fn bulk_rows_use_native_date_and_decimal_values() {
        let mut row = TokenRow::new();
        value_to_sql(&mut row, &TypedValue::Date32(Some(0)));
        value_to_sql(&mut row, &TypedValue::Decimal128(Some(12_345), 2));

        assert!(matches!(row.get(0), Some(ColumnData::Date(Some(_)))));
        let Some(ColumnData::Numeric(Some(numeric))) = row.get(1) else {
            panic!("expected a native numeric value");
        };
        assert_eq!(numeric.value(), 12_345);
        assert_eq!(numeric.scale(), 2);
    }
}

#[cfg(test)]
mod snapshot_integration_tests {
    use super::*;
    use dbgen_rs::testing::{check_snapshot_chunks, cli_args};

    #[tokio::test]
    #[ignore = "requires a disposable localhost database and DBGEN_TEST_PORT/PASSWORD"]
    async fn commits_failures_and_cancellation() -> Result<()> {
        let cli = Cli::try_parse_from(cli_args("sqlserver", "sa", "bench")?)?;
        let backend = Arc::new(SqlServerBackend::new(&cli)?);
        let mut conn = backend.pool.get().await?;
        conn.execute("DROP TABLE IF EXISTS dbgen_snapshot_test; CREATE TABLE dbgen_snapshot_test (id INT PRIMARY KEY)", &[]).await?;
        let pool = &backend.pool;
        let result = check_snapshot_chunks(backend.clone(), "dbgen_snapshot_test", || async move {
            let row = pool
                .get()
                .await?
                .query("SELECT COUNT(*) FROM dbgen_snapshot_test", &[])
                .await?
                .into_row()
                .await?;
            Ok(row.and_then(|r| r.get::<i32, _>(0)).unwrap_or(0) as u64)
        })
        .await;
        conn.execute("DROP TABLE dbgen_snapshot_test", &[]).await?;
        result
    }
}
