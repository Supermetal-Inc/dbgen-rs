use anyhow::{bail, Result};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use deadpool_tiberius::{Manager, Pool};
use dbgen_rs::tpch::{
    self, batch_rows, get_comment_column, get_pk_columns, Mode, TpchBackend, TypedValue, UNIX_EPOCH,
};
use log::info;
use std::sync::Arc;
use tiberius::{IntoSql, TokenRow};

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
            .max_size(16);
        Ok(Self {
            pool: manager.create_pool()?,
        })
    }
}

fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

fn value_to_sql(row: &mut TokenRow<'static>, val: &TypedValue) {
    match val {
        TypedValue::Int32(v) => row.push(v.into_sql()),
        TypedValue::Int64(v) => row.push(v.into_sql()),
        TypedValue::Utf8(v) => row.push(v.clone().into_sql()),
        TypedValue::Date32(v) => {
            let s = v.map(|days| {
                let date = UNIX_EPOCH + chrono::Duration::days(days as i64);
                date.format("%Y-%m-%d").to_string()
            });
            row.push(s.into_sql());
        }
        TypedValue::Decimal128(v, scale) => {
            let s = v.map(|val| {
                if *scale == 0 {
                    return val.to_string();
                }
                let sign = if val < 0 { "-" } else { "" };
                let abs = val.unsigned_abs();
                let divisor = 10_u128.pow(*scale as u32);
                format!(
                    "{}{}.{:0>width$}",
                    sign,
                    abs / divisor,
                    abs % divisor,
                    width = *scale as usize
                )
            });
            row.push(s.into_sql());
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

fn arrow_type_to_bulk_safe(dt: &DataType) -> Result<String> {
    Ok(match dt {
        DataType::Date32 => "NVARCHAR(10)".into(),
        DataType::Decimal128(_, _) => "NVARCHAR(40)".into(),
        _ => arrow_type_to_sql(dt)?,
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
        quote_ident(target), quote_ident(temp), join, update, ins_cols, ins_vals
    )
}

impl TpchBackend for SqlServerBackend {
    async fn drop_table(&self, table: &str) -> Result<()> {
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
            table, qt,
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
                    arrow_type_to_bulk_safe(f.data_type())?
                ))
            })
            .collect::<Result<_>>()?;
        let mut client = self.pool.get().await?;
        client
            .execute(
                &format!("DROP TABLE IF EXISTS {}", quote_ident(&name)),
                &[],
            )
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
        let mut vals = Vec::new();
        for batch in batches {
            let mut iter = batch_rows(&batch)?;
            let mut req = client.bulk_insert(table).await?;
            while iter.next_into(&mut vals) {
                let mut row = TokenRow::with_capacity(vals.len());
                for val in &vals {
                    value_to_sql(&mut row, val);
                }
                req.send(row).await?;
            }
            total += req.finalize().await?.total() as usize;
        }
        Ok(total)
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
        let sql = generate_merge_sql(target, temp, schema);
        self.pool.get().await?.execute(&sql, &[]).await?;
        Ok(())
    }

    async fn truncate_table(&self, table: &str) -> Result<()> {
        self.pool
            .get()
            .await?
            .execute(
                &format!("TRUNCATE TABLE {}", quote_ident(table)),
                &[],
            )
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
        true
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    info!("Target: {}:{}/{}", cli.host, cli.port, cli.database);
    let backend = Arc::new(SqlServerBackend::new(&cli)?);
    tpch::run(backend, cli.mode).await?;
    Ok(())
}
