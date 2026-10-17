use anyhow::{Result, bail};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use clap::Parser;
use dbgen_rs::tpch::{
    self, Mode, TpchBackend, TypedValue, UNIX_EPOCH, batch_rows, format_decimal128,
    get_comment_column, get_pk_columns,
};
use mysql_async::{Conn, Opts, OptsBuilder, Params, Value, prelude::Queryable};
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

fn value(v: &TypedValue) -> Value {
    match v {
        TypedValue::Int32(Some(v)) => Value::Int(*v as i64),
        TypedValue::Int64(Some(v)) => Value::Int(*v),
        TypedValue::Utf8(Some(v)) => Value::Bytes(v.as_bytes().to_vec()),
        TypedValue::Date32(Some(v)) => Value::Bytes(
            (UNIX_EPOCH + chrono::Duration::days(*v as i64))
                .to_string()
                .into_bytes(),
        ),
        TypedValue::Decimal128(Some(v), scale) => {
            Value::Bytes(format_decimal128(*v, *scale).into_bytes())
        }
        _ => Value::NULL,
    }
}

fn insert_sql(table: &str, schema: &Schema, rows: usize) -> String {
    let row = format!("({})", vec!["?"; schema.fields().len()].join(","));
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_ident(table),
        columns(schema),
        vec![row; rows].join(",")
    )
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
        let name = format!("_temp_{table}_{}", std::process::id());
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
        // Bound packet size and parameter count; don't issue one round trip per row.
        const ROWS: usize = 500;
        let sql = insert_sql(table, schema, ROWS);
        let mut total = 0;
        let mut row = Vec::new();
        for batch in batches {
            let mut iter = batch_rows(&batch)?;
            let mut params = Vec::with_capacity(ROWS * schema.fields().len());
            let mut count = 0;
            while iter.next_into(&mut row) {
                params.extend(row.iter().map(value));
                count += 1;
                if count == ROWS {
                    conn.exec_drop(&sql, Params::Positional(std::mem::take(&mut params)))
                        .await?;
                    total += count;
                    count = 0;
                }
            }
            if count > 0 {
                conn.exec_drop(insert_sql(table, schema, count), Params::Positional(params))
                    .await?;
                total += count;
            }
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

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    log::info!("Target: {}:{}/{}", cli.host, cli.port, cli.database);
    tpch::run(Arc::new(MysqlBackend::new(&cli)), cli.mode).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_values_without_sql_escaping() {
        assert_eq!(
            value(&TypedValue::Decimal128(Some(-12345), 2)),
            Value::Bytes(b"-123.45".to_vec())
        );
        assert_eq!(
            value(&TypedValue::Date32(Some(0))),
            Value::Bytes(b"1970-01-01".to_vec())
        );
        assert_eq!(
            value(&TypedValue::Utf8(Some("a'\\\0\n".into()))),
            Value::Bytes(b"a'\\\0\n".to_vec())
        );
        assert_eq!(value(&TypedValue::Int64(None)), Value::NULL);
    }
    #[test]
    fn all_schemas_and_composite_keys() {
        for table in tpch::ALL_TABLES {
            let schema = tpch::TpchTable::new(table, 0.01).unwrap().schema();
            let ddl = create_sql(table, &schema, Some(get_pk_columns(table))).unwrap();
            assert!(ddl.contains("PRIMARY KEY"));
            let sql = upsert_sql(table, "staging", &schema);
            let updates = sql.split("ON DUPLICATE KEY UPDATE ").nth(1).unwrap();
            for pk in get_pk_columns(table) {
                assert!(!updates.contains(&format!("{} =", quote_ident(pk))));
            }
            assert!(updates.contains("UTC_TIMESTAMP(6)"));
        }
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }
    #[tokio::test]
    async fn rejects_invalid_workloads_before_connecting() {
        let cli = Cli::parse_from(["mysql", "snapshot"]);
        let backend = Arc::new(MysqlBackend::new(&cli));
        for scale_factor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let mode = Mode::Snapshot(tpch::SnapshotArgs {
                scale_factor,
                tables: "all".into(),
                drop: false,
                create_only: false,
            });
            assert!(
                tpch::run(Arc::clone(&backend), mode)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("scale factor")
            );
        }
        let mode = Mode::Cdc(tpch::CdcArgs {
            tables: "orders".into(),
            rate: 0,
            scale_factor: 0.01,
        });
        assert!(
            tpch::run(backend, mode)
                .await
                .unwrap_err()
                .to_string()
                .contains("rate")
        );
    }

    // Explicit opt-in: this test drops the eight TPC-H tables in the selected database.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires a disposable MySQL database and MYSQL_TEST_PORT"]
    async fn mysql_snapshot_and_cdc_roundtrip() -> Result<()> {
        let port = std::env::var("MYSQL_TEST_PORT")?.parse()?;
        let cli = Cli {
            mode: Mode::Snapshot(tpch::SnapshotArgs {
                scale_factor: 0.01,
                tables: "all".into(),
                drop: true,
                create_only: true,
            }),
            host: "127.0.0.1".into(),
            port,
            database: std::env::var("MYSQL_TEST_DATABASE").unwrap_or("supermetal".into()),
            user: "root".into(),
            password: std::env::var("MYSQL_TEST_PASSWORD").unwrap_or("mysql".into()),
        };
        let backend = Arc::new(MysqlBackend::new(&cli));
        tpch::run(Arc::clone(&backend), cli.mode.clone()).await?;
        for table in tpch::ALL_TABLES {
            let n: Option<u64> = backend
                .connection(table)
                .await?
                .lock()
                .await
                .query_first(format!("SELECT COUNT(*) FROM {}", quote_ident(table)))
                .await?;
            assert_eq!(n, Some(0));
        }
        let load = Mode::Snapshot(tpch::SnapshotArgs {
            scale_factor: 0.01,
            tables: "all".into(),
            drop: false,
            create_only: false,
        });
        tpch::run(Arc::clone(&backend), load.clone()).await?;
        for table in tpch::ALL_TABLES {
            let source = tpch::TpchTable::new(table, 0.01)?;
            let schema = source.schema();
            let mut expected = Vec::new();
            for batch in source {
                let mut iter = batch_rows(&batch)?;
                let mut row = Vec::new();
                while iter.next_into(&mut row) {
                    expected.push(format!("{:?}", row.iter().map(value).collect::<Vec<_>>()));
                }
            }
            let cols = schema
                .fields()
                .iter()
                .map(|f| {
                    let col = quote_ident(f.name());
                    if f.data_type() == &DataType::Date32 {
                        format!("DATE_FORMAT({col}, '%Y-%m-%d')")
                    } else {
                        col
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let rows: Vec<mysql_async::Row> = backend
                .connection(table)
                .await?
                .lock()
                .await
                .exec(format!("SELECT {cols} FROM {}", quote_ident(table)), ())
                .await?;
            let mut actual = rows
                .into_iter()
                .map(|r| format!("{:?}", r.unwrap()))
                .collect::<Vec<_>>();
            expected.sort_unstable();
            actual.sort_unstable();
            assert_eq!(actual.len(), expected.len(), "{table} count");
            assert!(actual == expected, "{table} full value roundtrip");
            let count = actual.len();
            let temp = backend.create_temp_table(table, &schema).await?;
            let batch = tpch::TpchTable::new(table, 0.01)?
                .with_batch_size(3)
                .next()
                .unwrap();
            backend
                .copy_batches(&temp, &schema, Box::new(std::iter::once(batch.clone())))
                .await?;
            backend.upsert_from_temp(table, &temp, &schema).await?;
            let changed: Option<u64> = backend
                .connection(table)
                .await?
                .lock()
                .await
                .query_first(format!(
                    "SELECT COUNT(*) FROM {} WHERE {} LIKE '% CDC:%'",
                    quote_ident(table),
                    quote_ident(get_comment_column(table))
                ))
                .await?;
            assert_eq!(changed, Some(3), "{table} changed comments");
            let n: Option<u64> = backend
                .connection(table)
                .await?
                .lock()
                .await
                .query_first(format!("SELECT COUNT(*) FROM {}", quote_ident(table)))
                .await?;
            assert_eq!(n, Some(count as u64), "{table} no duplicate keys");
            // Exercise the insert branch of the same upsert and staging truncation.
            backend.truncate_table(table).await?;
            backend.upsert_from_temp(table, &temp, &schema).await?;
            let n: Option<u64> = backend
                .connection(table)
                .await?
                .lock()
                .await
                .query_first(format!("SELECT COUNT(*) FROM {}", quote_ident(table)))
                .await?;
            assert_eq!(n, Some(3));
            backend.truncate_table(&temp).await?;
            backend.drop_temp_table(&temp).await?;
        }
        // Existing primary keys must make a snapshot fail, not log-and-succeed.
        assert!(tpch::run(Arc::clone(&backend), load).await.is_err());
        backend
            .execute("region", "ALTER TABLE region DROP COLUMN r_comment".into())
            .await?;
        let cdc = Mode::Cdc(tpch::CdcArgs {
            tables: "region".into(),
            rate: 100,
            scale_factor: 0.01,
        });
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), tpch::run(backend, cdc))
                .await?;
        assert!(
            result.is_err(),
            "CDC errors must terminate instead of retrying a stale batch"
        );
        Ok(())
    }
}
