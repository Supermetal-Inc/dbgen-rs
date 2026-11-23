use anyhow::{Result, bail};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, BytesMut};
use chrono::NaiveDate;
use clap::Parser;
use dbgen_rs::nested::is_json;
use dbgen_rs::tpch::{
    self, Mode, TpchBackend, TypedValue, UNIX_EPOCH, batch_rows, get_comment_column, get_pk_columns,
};
use deadpool_postgres::{Config, Pool, PoolConfig, Runtime};
use log::info;
use pg_bigdecimal::{BigDecimal, PgNumeric};
use std::error::Error;
use std::sync::Arc;
use tokio_postgres::NoTls;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{IsNull, ToSql, Type, accepts, to_sql_checked};

#[derive(Parser)]
#[command(name = "pg")]
#[command(about = "TPC-H load generator for PostgreSQL")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 5432)]
    port: u16,
    #[arg(long, default_value = "postgres")]
    database: String,
    #[arg(long, default_value = "postgres")]
    user: String,
    #[arg(long, default_value = "postgres")]
    password: String,
}

struct PostgresBackend {
    pool: Pool,
}

impl PostgresBackend {
    fn new(cli: &Cli) -> Result<Self> {
        let mut cfg = Config::new();
        cfg.host = Some(cli.host.clone());
        cfg.port = Some(cli.port);
        cfg.dbname = Some(cli.database.clone());
        cfg.user = Some(cli.user.clone());
        cfg.password = Some(cli.password.clone());
        let mut pool = PoolConfig::default();
        pool.max_size = pool.max_size.max(cli.mode.snapshot_workers());
        cfg.pool = Some(pool);
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
        Ok(Self { pool })
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn arrow_type_to_sql(field: &Field) -> Result<String> {
    if is_json(field) {
        return Ok("JSONB".into());
    }
    Ok(match field.data_type() {
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "TEXT".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(p, s) => format!("NUMERIC({},{})", p, s),
        _ => bail!(
            "unsupported Arrow type for PostgreSQL: {:?}",
            field.data_type()
        ),
    })
}

fn arrow_type_to_pg_type(field: &Field) -> Result<Type> {
    if is_json(field) {
        return Ok(Type::JSONB);
    }
    Ok(match field.data_type() {
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Type::TEXT,
        DataType::Date32 => Type::DATE,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        _ => bail!(
            "unsupported Arrow type for PostgreSQL: {:?}",
            field.data_type()
        ),
    })
}

fn schema_to_pg_types(schema: &Schema) -> Result<Vec<Type>> {
    schema
        .fields()
        .iter()
        .map(|f| arrow_type_to_pg_type(f))
        .collect()
}

/// Concrete typed value for PostgreSQL. Avoids a Box<dyn ToSql> allocation per
/// cell by holding the PG-ready value inline and delegating via `pg_as_to_sql`.
/// Borrows strings from Arrow storage through TypedValue.
enum PgValue<'a> {
    I32(Option<i32>),
    I64(Option<i64>),
    Str(Option<&'a str>),
    Date(Option<NaiveDate>),
    Numeric(PgNumeric),
    Jsonb(Option<Jsonb<'a>>),
}

/// JSON text sent as binary `jsonb`, which `&str` does not accept.
#[derive(Debug)]
struct Jsonb<'a>(&'a str);

impl ToSql for Jsonb<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // jsonb binary format version
        out.put_u8(1);
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    accepts!(JSONB);
    to_sql_checked!();
}

fn pg_as_to_sql<'r, 'v>(value: &'r PgValue<'v>) -> &'r (dyn ToSql + Sync) {
    match value {
        PgValue::I32(v) => v,
        PgValue::I64(v) => v,
        PgValue::Str(v) => v,
        PgValue::Date(v) => v,
        PgValue::Numeric(v) => v,
        PgValue::Jsonb(v) => v,
    }
}

fn typed_value_to_pg<'a>(val: &TypedValue<'a>, ty: &Type) -> PgValue<'a> {
    match val {
        TypedValue::Int32(v) => PgValue::I32(*v),
        TypedValue::Int64(v) => PgValue::I64(*v),
        TypedValue::Utf8(v) if *ty == Type::JSONB => PgValue::Jsonb(v.map(Jsonb)),
        TypedValue::Utf8(v) => PgValue::Str(*v),
        TypedValue::Date32(v) => {
            PgValue::Date(v.map(|days| UNIX_EPOCH + chrono::Duration::days(days as i64)))
        }
        TypedValue::Decimal128(v, scale) => PgValue::Numeric(PgNumeric::new(
            v.map(|val| BigDecimal::new(val.into(), *scale as i64)),
        )),
    }
}

async fn binary_copy_batches(
    client: &tokio_postgres::Client,
    table: &str,
    schema: &Schema,
    batches: impl Iterator<Item = RecordBatch> + Send,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<usize> {
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();
    let types = schema_to_pg_types(schema)?;
    let stmt = format!(
        "COPY {} ({}) FROM STDIN BINARY",
        quote_ident(table),
        col_names.join(", ")
    );

    let sink = client.copy_in(&stmt).await?;
    let writer = BinaryCopyInWriter::new(sink, &types);
    tokio::pin!(writer);

    for batch in batches {
        let mut iter = batch_rows(&batch)?;
        let mut vals = Vec::with_capacity(batch.num_columns());
        let mut row = Vec::with_capacity(batch.num_columns());
        while iter.next_into(&mut vals) {
            anyhow::ensure!(
                !cancel.is_some_and(|c| c.is_cancelled()),
                "snapshot cancelled"
            );
            row.clear();
            row.extend(
                vals.iter()
                    .zip(&types)
                    .map(|(v, ty)| typed_value_to_pg(v, ty)),
            );
            writer
                .as_mut()
                .write_raw(row.iter().map(pg_as_to_sql))
                .await?;
        }
    }

    Ok(writer.finish().await? as usize)
}

fn generate_upsert_sql(target: &str, temp: &str, schema: &Schema) -> String {
    let pk = get_pk_columns(target);
    let comment = get_comment_column(target);
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let col_list = cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let conflict_cols = pk
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let update = schema
        .fields()
        .iter()
        .filter(|f| !pk.contains(&f.name().as_str()))
        .map(|f| {
            let c = f.name().as_str();
            let qc = quote_ident(c);
            if c == comment {
                format!(
                    "{} = LEFT(EXCLUDED.{}, 40) || ' CDC:' || NOW()::text",
                    qc, qc
                )
            } else if is_json(f) {
                // The nested shape keeps the comment inside the single array element.
                format!(
                    "{qc} = jsonb_set(EXCLUDED.{qc}, '{{0,{comment}}}', \
                     to_jsonb(LEFT(EXCLUDED.{qc} -> 0 ->> '{comment}', 40) || ' CDC:' || NOW()::text))"
                )
            } else {
                format!("{} = EXCLUDED.{}", qc, qc)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {}",
        quote_ident(target),
        col_list,
        col_list,
        quote_ident(temp),
        conflict_cols,
        update
    )
}

impl TpchBackend for PostgresBackend {
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
                let null = if f.is_nullable() { "" } else { " NOT NULL" };
                Ok(format!(
                    "{} {}{}",
                    quote_ident(f.name()),
                    arrow_type_to_sql(f)?,
                    null
                ))
            })
            .collect::<Result<_>>()?;
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
            quote_ident(table),
            cols.join(", "),
            pk_list
        );
        self.pool.get().await?.execute(&ddl, &[]).await?;
        Ok(())
    }

    async fn create_temp_table(&self, table: &str, schema: &Schema) -> Result<String> {
        let name = format!("_temp_{}_{}", table, std::process::id());
        let quoted = quote_ident(&name);
        let cols: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                Ok(format!(
                    "{} {}",
                    quote_ident(f.name()),
                    arrow_type_to_sql(f)?
                ))
            })
            .collect::<Result<_>>()?;
        let client = self.pool.get().await?;
        client
            .execute(&format!("DROP TABLE IF EXISTS {}", quoted), &[])
            .await?;
        client
            .execute(
                &format!("CREATE UNLOGGED TABLE {} ({})", quoted, cols.join(", ")),
                &[],
            )
            .await?;
        Ok(name)
    }

    async fn copy_batches(
        &self,
        table: &str,
        schema: &Schema,
        batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
    ) -> Result<usize> {
        let client = self.pool.get().await?;
        binary_copy_batches(&client, table, schema, batches, None).await
    }

    async fn upsert_from_temp(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        let sql = generate_upsert_sql(target, temp, schema);
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
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let schema = batch.schema();
        let rows = binary_copy_batches(
            tx.client(),
            &table,
            &schema,
            std::iter::once(batch),
            Some(&cancel),
        )
        .await?;
        progress.add_written(rows);
        anyhow::ensure!(!cancel.is_cancelled(), "snapshot cancelled");
        tx.commit().await?;
        progress.add_committed(rows);
        Ok(())
    }

    fn supports_nested() -> bool {
        true
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    info!("Target: {}:{}/{}", cli.host, cli.port, cli.database);
    let backend = Arc::new(PostgresBackend::new(&cli)?);
    tpch::run(backend, cli.mode).await?;
    Ok(())
}

#[cfg(test)]
mod snapshot_integration_tests {
    use super::*;
    use dbgen_rs::testing::{check_snapshot_chunks, cli_args};

    #[tokio::test]
    #[ignore = "requires a disposable localhost database and DBGEN_TEST_PORT/PASSWORD"]
    async fn commits_failures_and_cancellation() -> Result<()> {
        let cli = Cli::try_parse_from(cli_args("pg", "postgres", "bench")?)?;
        let backend = Arc::new(PostgresBackend::new(&cli)?);
        let conn = backend.pool.get().await?;
        conn.batch_execute("DROP TABLE IF EXISTS dbgen_snapshot_test; CREATE TABLE dbgen_snapshot_test (id INTEGER PRIMARY KEY)").await?;
        let client = &conn;
        let result = check_snapshot_chunks(backend.clone(), "dbgen_snapshot_test", || async move {
            let row = client
                .query_one("SELECT COUNT(*) FROM dbgen_snapshot_test", &[])
                .await?;
            Ok(row.get::<_, i64>(0) as u64)
        })
        .await;
        conn.batch_execute("DROP TABLE dbgen_snapshot_test").await?;
        result
    }
}
