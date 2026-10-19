use anyhow::{Result, bail};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use clap::Parser;
use dbgen_rs::tpch::{
    self, Mode, TpchBackend, TypedValue, UNIX_EPOCH, batch_rows, format_decimal128,
    get_comment_column, get_pk_columns,
};
use futures_util::{StreamExt, stream};
use mysql_async::{Conn, Opts, OptsBuilder, TxOpts, prelude::Queryable};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(name = "mysql", about = "TPC-H load generator for MySQL")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value_t = 3306)]
    port: u16,
    #[arg(long, default_value = "supermetal")]
    database: String,
    #[arg(long, default_value = "root")]
    user: String,
    #[arg(long, default_value = "mysql")]
    password: String,
}

type Connection = Arc<Mutex<Conn>>;
struct MysqlBackend {
    opts: Opts,
    // Keep one session per table: MySQL TEMPORARY tables are session scoped.
    connections: Mutex<HashMap<String, Connection>>,
}

impl MysqlBackend {
    fn new(cli: &Cli) -> Self {
        let opts = OptsBuilder::default()
            .ip_or_hostname(&cli.host)
            .tcp_port(cli.port)
            .db_name(Some(&cli.database))
            .user(Some(&cli.user))
            .pass(Some(&cli.password))
            .prefer_socket(false)
            .init(vec![
                "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'",
                "SET NAMES utf8mb4",
            ]);
        Self {
            opts: opts.into(),
            connections: Mutex::new(HashMap::new()),
        }
    }

    async fn connection(&self, table: &str) -> Result<Connection> {
        let mut connections = self.connections.lock().await;
        if let Some(conn) = connections.get(table) {
            return Ok(Arc::clone(conn));
        }
        let conn = Arc::new(Mutex::new(Conn::new(self.opts.clone()).await?));
        connections.insert(table.into(), Arc::clone(&conn));
        Ok(conn)
    }

    async fn execute(&self, table: &str, sql: String) -> Result<()> {
        self.connection(table)
            .await?
            .lock()
            .await
            .query_drop(sql)
            .await?;
        Ok(())
    }
}

fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn arrow_type_to_sql(dt: &DataType) -> Result<String> {
    Ok(match dt {
        DataType::Int32 => "INT".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Utf8 | DataType::Utf8View => "TEXT".into(),
        DataType::Date32 => "DATE".into(),
        DataType::Decimal128(p, s) if *s >= 0 && *s <= 30 && *s as u8 <= *p => {
            format!("DECIMAL({p},{s})")
        }
        _ => bail!("unsupported Arrow type for MySQL: {dt:?}"),
    })
}

fn columns(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn create_sql(table: &str, schema: &Schema, primary_key: Option<&[&str]>) -> Result<String> {
    let mut fields = schema
        .fields()
        .iter()
        .map(|f| {
            Ok(format!(
                "{} {}{}",
                quote_ident(f.name()),
                arrow_type_to_sql(f.data_type())?,
                if f.is_nullable() { "" } else { " NOT NULL" }
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(pk) = primary_key {
        fields.push(format!(
            "PRIMARY KEY ({})",
            pk.iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(format!(
        "CREATE {}TABLE IF NOT EXISTS {} ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
        if primary_key.is_none() {
            "TEMPORARY "
        } else {
            ""
        },
        quote_ident(table),
        fields.join(", ")
    ))
}

// LOAD DATA's escaping is distinct from SQL string escaping. In particular,
// NULL is unescaped \N, while a literal "\N" must escape its backslash.
fn encode_field(value: &TypedValue, out: &mut Vec<u8>) {
    let text = match value {
        TypedValue::Int32(Some(v)) => v.to_string(),
        TypedValue::Int64(Some(v)) => v.to_string(),
        TypedValue::Date32(Some(v)) => (UNIX_EPOCH + chrono::Duration::days(*v as i64)).to_string(),
        TypedValue::Decimal128(Some(v), scale) => format_decimal128(*v, *scale),
        TypedValue::Utf8(Some(v)) => {
            for &byte in v.as_bytes() {
                match byte {
                    b'\0' => out.extend_from_slice(br"\0"),
                    b'\t' => out.extend_from_slice(br"\t"),
                    b'\n' => out.extend_from_slice(br"\n"),
                    b'\r' => out.extend_from_slice(br"\r"),
                    b'\\' => out.extend_from_slice(br"\\"),
                    0x1a => out.extend_from_slice(br"\Z"),
                    _ => out.push(byte),
                }
            }
            return;
        }
        _ => {
            out.extend_from_slice(br"\N");
            return;
        }
    };
    out.extend_from_slice(text.as_bytes());
}

fn encode_chunk(batch: &RecordBatch) -> Result<Bytes> {
    let mut out = Vec::new();
    let mut row = Vec::new();
    let mut iter = batch_rows(batch)?;
    while iter.next_into(&mut row) {
        for (index, value) in row.iter().enumerate() {
            if index > 0 {
                out.push(b'\t');
            }
            encode_field(value, &mut out);
        }
        out.push(b'\n');
    }
    Ok(Bytes::from(out))
}

fn load_sql(table: &str, schema: &Schema) -> String {
    format!(
        r"LOAD DATA LOCAL INFILE 'dbgen-stream' INTO TABLE {} CHARACTER SET utf8mb4
        FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' ({})",
        quote_ident(table),
        columns(schema)
    )
}

async fn load_batch(conn: &mut Conn, sql: &str, batch: RecordBatch) -> Result<usize> {
    let expected = batch.num_rows();
    if expected == 0 {
        return Ok(0);
    }
    // The one-shot handler only supplies generated bytes, never filesystem data.
    // Encode at most 1024 rows at a time, without materializing a whole text file.
    conn.set_infile_handler(async move {
        Ok(stream::iter((0..expected).step_by(1024).map(move |offset| {
            encode_chunk(&batch.slice(offset, (expected - offset).min(1024)))
                .map_err(std::io::Error::other)
        }))
        .boxed())
    });
    let mut tx = conn.start_transaction(TxOpts::default()).await?;
    let result = tx.query_iter(sql).await?;
    let warnings = result.warnings();
    let affected = result.affected_rows();
    result.drop_result().await?;
    // LOCAL downgrades some errors to warnings even with STRICT_ALL_TABLES.
    // Reject the entire batch rather than commit coerced values or skipped keys.
    if warnings != 0 || affected != expected as u64 {
        let details: Vec<(String, u16, String)> = tx.query("SHOW WARNINGS LIMIT 5").await?;
        tx.rollback().await?;
        bail!(
            "LOAD DATA rejected batch: expected {expected} rows, loaded {affected}, warnings {warnings}: {details:?}"
        );
    }
    tx.commit().await?;
    Ok(expected)
}

fn upsert_sql(target: &str, temp: &str, schema: &Schema) -> String {
    let pk = get_pk_columns(target);
    let comment = get_comment_column(target);
    let updates = schema
        .fields()
        .iter()
        .filter(|f| !pk.contains(&f.name().as_str()))
        .map(|f| {
            let col = quote_ident(f.name());
            if f.name() == comment {
                format!("{col} = CONCAT(LEFT(incoming.{col}, 40), ' CDC:', UTC_TIMESTAMP(6))")
            } else {
                format!("{col} = incoming.{col}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) SELECT {} FROM {} AS incoming WHERE TRUE ON DUPLICATE KEY UPDATE {}",
        quote_ident(target),
        columns(schema),
        columns(schema),
        quote_ident(temp),
        updates
    )
}

impl TpchBackend for MysqlBackend {
    async fn drop_table(&self, table: &str) -> Result<()> {
        self.execute(
            table,
            format!("DROP TABLE IF EXISTS {}", quote_ident(table)),
        )
        .await
    }
    async fn create_table(&self, table: &str, schema: &Schema) -> Result<()> {
        self.execute(
            table,
            create_sql(table, schema, Some(get_pk_columns(table)))?,
        )
        .await
    }
    async fn create_temp_table(&self, table: &str, schema: &Schema) -> Result<String> {
        let name = format!("_temp_{table}");
        let conn = self.connection(table).await?;
        conn.lock()
            .await
            .query_drop(create_sql(&name, schema, None)?)
            .await?;
        self.connections.lock().await.insert(name.clone(), conn);
        Ok(name)
    }
    async fn copy_batches(
        &self,
        table: &str,
        schema: &Schema,
        batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
    ) -> Result<usize> {
        let conn = self.connection(table).await?;
        let mut conn = conn.lock().await;
        let sql = load_sql(table, schema);
        let mut total = 0;
        for batch in batches {
            total += load_batch(&mut conn, &sql, batch).await?;
        }
        Ok(total)
    }
    async fn copy_temp_to_target(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        self.execute(
            target,
            format!(
                "INSERT INTO {} ({}) SELECT {} FROM {}",
                quote_ident(target),
                columns(schema),
                columns(schema),
                quote_ident(temp)
            ),
        )
        .await
    }
    async fn upsert_from_temp(&self, target: &str, temp: &str, schema: &Schema) -> Result<()> {
        self.execute(target, upsert_sql(target, temp, schema)).await
    }
    async fn truncate_table(&self, table: &str) -> Result<()> {
        self.execute(table, format!("TRUNCATE TABLE {}", quote_ident(table)))
            .await
    }
    async fn drop_temp_table(&self, table: &str) -> Result<()> {
        self.execute(
            table,
            format!("DROP TEMPORARY TABLE IF EXISTS {}", quote_ident(table)),
        )
        .await?;
        self.connections.lock().await.remove(table);
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
    log::info!("Target: {}:{}/{}", cli.host, cli.port, cli.database);
    tpch::run(Arc::new(MysqlBackend::new(&cli)), cli.mode).await
}
