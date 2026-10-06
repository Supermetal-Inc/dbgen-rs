use anyhow::{Result, bail};
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
        pb.max_connections(16);
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

fn typed_value_to_ora(val: &TypedValue) -> OraValue<'_> {
    match val {
        TypedValue::Int32(v) => OraValue::I32(*v),
        TypedValue::Int64(v) => OraValue::I64(*v),
        TypedValue::Utf8(v) => OraValue::Str(v.as_deref()),
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
    batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
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
    let mut vals = Vec::with_capacity(schema.fields().len());

    for rb in batches {
        let mut iter = batch_rows(&rb)?;
        while iter.next_into(&mut vals) {
            let ora_vals: Vec<OraValue> = vals.iter().map(typed_value_to_ora).collect();
            let refs: Vec<&dyn ToSql> = ora_vals.iter().map(OraValue::as_to_sql).collect();
            batch.append_row(&refs)?;
        }
        total += rb.num_rows();
    }

    batch.execute()?;
    conn.commit()?;
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
        bulk_insert_sync(&conn, table, schema, batches)
    }

    async fn copy_temp_to_target(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        let conn = self.pool.get()?;
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
        conn.execute(&sql, &[])?;
        conn.commit()?;
        Ok(())
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

    fn needs_temp_for_snapshot() -> bool {
        false
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
    // (https://github.com/kubo/rust-oracle/issues/37). Install ours after the
    // pool is built so it overrides OCI's via sigaction.
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        std::process::exit(130);
    });

    tpch::run(backend, cli.mode).await?;
    Ok(())
}
