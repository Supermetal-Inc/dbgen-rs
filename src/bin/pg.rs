use anyhow::{bail, Result};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use clap::Parser;
use deadpool_postgres::{Config, Pool, Runtime};
use dbgen_rs::tpch::{
    self, batch_rows, get_comment_column, get_pk_columns, Mode, TpchBackend, TypedValue, UNIX_EPOCH,
};
use log::info;
use pg_bigdecimal::{BigDecimal, PgNumeric};
use std::sync::Arc;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::NoTls;

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
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
        Ok(Self { pool })
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn arrow_type_to_sql(dt: &DataType) -> Result<String> {
    Ok(match dt {
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "TEXT".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(p, s) => format!("NUMERIC({},{})", p, s),
        _ => bail!("unsupported Arrow type for PostgreSQL: {:?}", dt),
    })
}

fn arrow_type_to_pg_type(dt: &DataType) -> Result<Type> {
    Ok(match dt {
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Type::TEXT,
        DataType::Date32 => Type::DATE,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        _ => bail!("unsupported Arrow type for PostgreSQL: {:?}", dt),
    })
}

fn schema_to_pg_types(schema: &Schema) -> Result<Vec<Type>> {
    schema
        .fields()
        .iter()
        .map(|f| arrow_type_to_pg_type(f.data_type()))
        .collect()
}

/// Concrete typed value for PostgreSQL. Avoids a Box<dyn ToSql> allocation per
/// cell by holding the PG-ready value inline and delegating via `as_to_sql`.
/// Borrows strings from the source TypedValue to avoid cloning.
enum PgValue<'a> {
    I32(Option<i32>),
    I64(Option<i64>),
    Str(Option<&'a str>),
    Date(Option<NaiveDate>),
    Numeric(PgNumeric),
}

impl PgValue<'_> {
    fn as_to_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            PgValue::I32(v) => v,
            PgValue::I64(v) => v,
            PgValue::Str(v) => v,
            PgValue::Date(v) => v,
            PgValue::Numeric(v) => v,
        }
    }
}

fn typed_value_to_pg<'a>(val: &'a TypedValue) -> PgValue<'a> {
    match val {
        TypedValue::Int32(v) => PgValue::I32(*v),
        TypedValue::Int64(v) => PgValue::I64(*v),
        TypedValue::Utf8(v) => PgValue::Str(v.as_deref()),
        TypedValue::Date32(v) => {
            PgValue::Date(v.map(|days| UNIX_EPOCH + chrono::Duration::days(days as i64)))
        }
        TypedValue::Decimal128(v, scale) => PgValue::Numeric(PgNumeric::new(
            v.map(|val| BigDecimal::new(val.into(), *scale as i64)),
        )),
    }
}

async fn binary_copy_batches(
    pool: &Pool,
    table: &str,
    schema: &Schema,
    batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
) -> Result<usize> {
    let client = pool.get().await?;
    let col_names: Vec<String> = schema.fields().iter().map(|f| quote_ident(f.name())).collect();
    let types = schema_to_pg_types(schema)?;
    let stmt = format!(
        "COPY {} ({}) FROM STDIN BINARY",
        quote_ident(table),
        col_names.join(", ")
    );

    let sink = client.copy_in(&stmt).await?;
    let writer = BinaryCopyInWriter::new(sink, &types);
    tokio::pin!(writer);

    let mut vals = Vec::with_capacity(schema.fields().len());
    let mut total = 0;

    for batch in batches {
        let mut iter = batch_rows(&batch)?;
        while iter.next_into(&mut vals) {
            let row: Vec<PgValue<'_>> = vals.iter().map(typed_value_to_pg).collect();
            let refs: Vec<&(dyn ToSql + Sync)> = row.iter().map(PgValue::as_to_sql).collect();
            writer.as_mut().write(&refs).await?;
        }
        total += batch.num_rows();
    }

    writer.finish().await?;
    Ok(total)
}

fn generate_upsert_sql(target: &str, temp: &str, schema: &Schema) -> String {
    let pk = get_pk_columns(target);
    let comment = get_comment_column(target);
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let col_list = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let conflict_cols = pk.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let update = cols
        .iter()
        .filter(|c| !pk.contains(*c))
        .map(|c| {
            let qc = quote_ident(c);
            if *c == comment {
                format!("{} = LEFT(EXCLUDED.{}, 40) || ' CDC:' || NOW()::text", qc, qc)
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
        let pk_list = pk.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let cols: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let null = if f.is_nullable() { "" } else { " NOT NULL" };
                Ok(format!(
                    "{} {}{}",
                    quote_ident(f.name()),
                    arrow_type_to_sql(f.data_type())?,
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
                    arrow_type_to_sql(f.data_type())?
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
        binary_copy_batches(&self.pool, table, schema, batches).await
    }

    async fn copy_temp_to_target(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        let cols = schema
            .fields()
            .iter()
            .map(|f| quote_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) SELECT {} FROM {}",
            quote_ident(target),
            cols,
            cols,
            quote_ident(temp)
        );
        self.pool.get().await?.execute(&sql, &[]).await?;
        Ok(())
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
            .execute(
                &format!("DROP TABLE IF EXISTS {}", quote_ident(table)),
                &[],
            )
            .await?;
        Ok(())
    }

    fn needs_temp_for_snapshot() -> bool {
        false
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
