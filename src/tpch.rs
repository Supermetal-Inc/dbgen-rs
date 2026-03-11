use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arrow::array::{
    Array, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use clap::{Args, Subcommand};
use log::{error, info};
use tokio::task::JoinSet;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow,
    RecordBatchIterator, RegionArrow, SupplierArrow,
};

pub const UNIX_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(d) => d,
    None => panic!("invalid epoch"),
};

pub const ALL_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

pub fn parse_tables(tables: &str) -> Result<Vec<String>> {
    if tables == "all" {
        return Ok(ALL_TABLES.iter().map(|s| s.to_string()).collect());
    }
    let names: Vec<String> = tables.split(',').map(|s| s.trim().to_lowercase()).collect();
    for name in &names {
        if !ALL_TABLES.contains(&name.as_str()) {
            bail!("unknown table: '{}'. valid tables: {}", name, ALL_TABLES.join(", "));
        }
    }
    Ok(names)
}

pub fn get_pk_columns(table: &str) -> &'static [&'static str] {
    match table {
        "region" => &["r_regionkey"],
        "nation" => &["n_nationkey"],
        "supplier" => &["s_suppkey"],
        "customer" => &["c_custkey"],
        "part" => &["p_partkey"],
        "partsupp" => &["ps_partkey", "ps_suppkey"],
        "orders" => &["o_orderkey"],
        "lineitem" => &["l_orderkey", "l_linenumber"],
        _ => panic!("unknown table: {}", table),
    }
}

pub fn get_comment_column(table: &str) -> &'static str {
    match table {
        "region" => "r_comment",
        "nation" => "n_comment",
        "supplier" => "s_comment",
        "customer" => "c_comment",
        "part" => "p_comment",
        "partsupp" => "ps_comment",
        "orders" => "o_comment",
        "lineitem" => "l_comment",
        _ => panic!("unknown table: {}", table),
    }
}

#[derive(Subcommand, Clone)]
pub enum Mode {
    Snapshot(SnapshotArgs),
    Cdc(CdcArgs),
}

#[derive(Args, Clone)]
pub struct SnapshotArgs {
    #[arg(long, default_value_t = 0.01)]
    pub scale_factor: f64,
    #[arg(long, default_value = "all")]
    pub tables: String,
    #[arg(long, default_value_t = false)]
    pub drop: bool,
    #[arg(long, default_value_t = false)]
    pub create_only: bool,
}

#[derive(Args, Clone)]
pub struct CdcArgs {
    #[arg(long, default_value = "lineitem,orders")]
    pub tables: String,
    #[arg(long, default_value_t = 100)]
    pub rate: usize,
    #[arg(long, default_value_t = 1.0)]
    pub scale_factor: f64,
}

pub enum TpchTable {
    Region(RegionArrow),
    Nation(NationArrow),
    Supplier(SupplierArrow),
    Customer(CustomerArrow),
    Part(PartArrow),
    PartSupp(PartSuppArrow),
    Orders(OrderArrow),
    LineItem(LineItemArrow),
}

impl TpchTable {
    pub fn new(name: &str, scale_factor: f64) -> Result<Self> {
        Ok(match name {
            "region" => TpchTable::Region(RegionArrow::new(RegionGenerator::default())),
            "nation" => TpchTable::Nation(NationArrow::new(NationGenerator::default())),
            "supplier" => TpchTable::Supplier(SupplierArrow::new(SupplierGenerator::new(
                scale_factor, 1, 1,
            ))),
            "customer" => TpchTable::Customer(CustomerArrow::new(CustomerGenerator::new(
                scale_factor, 1, 1,
            ))),
            "part" => TpchTable::Part(PartArrow::new(PartGenerator::new(scale_factor, 1, 1))),
            "partsupp" => TpchTable::PartSupp(PartSuppArrow::new(PartSuppGenerator::new(
                scale_factor, 1, 1,
            ))),
            "orders" => {
                TpchTable::Orders(OrderArrow::new(OrderGenerator::new(scale_factor, 1, 1)))
            }
            "lineitem" => TpchTable::LineItem(LineItemArrow::new(LineItemGenerator::new(
                scale_factor, 1, 1,
            ))),
            _ => bail!("unknown table: {}", name),
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        match self {
            TpchTable::Region(g) => g.schema().clone(),
            TpchTable::Nation(g) => g.schema().clone(),
            TpchTable::Supplier(g) => g.schema().clone(),
            TpchTable::Customer(g) => g.schema().clone(),
            TpchTable::Part(g) => g.schema().clone(),
            TpchTable::PartSupp(g) => g.schema().clone(),
            TpchTable::Orders(g) => g.schema().clone(),
            TpchTable::LineItem(g) => g.schema().clone(),
        }
    }

    pub fn with_batch_size(self, batch_size: usize) -> Self {
        match self {
            TpchTable::Region(g) => TpchTable::Region(g.with_batch_size(batch_size)),
            TpchTable::Nation(g) => TpchTable::Nation(g.with_batch_size(batch_size)),
            TpchTable::Supplier(g) => TpchTable::Supplier(g.with_batch_size(batch_size)),
            TpchTable::Customer(g) => TpchTable::Customer(g.with_batch_size(batch_size)),
            TpchTable::Part(g) => TpchTable::Part(g.with_batch_size(batch_size)),
            TpchTable::PartSupp(g) => TpchTable::PartSupp(g.with_batch_size(batch_size)),
            TpchTable::Orders(g) => TpchTable::Orders(g.with_batch_size(batch_size)),
            TpchTable::LineItem(g) => TpchTable::LineItem(g.with_batch_size(batch_size)),
        }
    }
}

impl Iterator for TpchTable {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TpchTable::Region(g) => g.next(),
            TpchTable::Nation(g) => g.next(),
            TpchTable::Supplier(g) => g.next(),
            TpchTable::Customer(g) => g.next(),
            TpchTable::Part(g) => g.next(),
            TpchTable::PartSupp(g) => g.next(),
            TpchTable::Orders(g) => g.next(),
            TpchTable::LineItem(g) => g.next(),
        }
    }
}

#[derive(Debug)]
pub enum TypedValue {
    Int32(Option<i32>),
    Int64(Option<i64>),
    Utf8(Option<String>),
    Date32(Option<i32>),
    Decimal128(Option<i128>, i8),
}

enum TypedArray<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
    Date32(&'a Date32Array),
    Decimal128(&'a Decimal128Array, i8),
}

impl TypedArray<'_> {
    fn value_at(&self, idx: usize) -> TypedValue {
        match self {
            TypedArray::Int32(a) => TypedValue::Int32((!a.is_null(idx)).then(|| a.value(idx))),
            TypedArray::Int64(a) => TypedValue::Int64((!a.is_null(idx)).then(|| a.value(idx))),
            TypedArray::Utf8(a) => {
                TypedValue::Utf8((!a.is_null(idx)).then(|| a.value(idx).into()))
            }
            TypedArray::Utf8View(a) => {
                TypedValue::Utf8((!a.is_null(idx)).then(|| a.value(idx).into()))
            }
            TypedArray::Date32(a) => TypedValue::Date32((!a.is_null(idx)).then(|| a.value(idx))),
            TypedArray::Decimal128(a, s) => {
                TypedValue::Decimal128((!a.is_null(idx)).then(|| a.value(idx)), *s)
            }
        }
    }
}

pub struct BatchRowIter<'a> {
    arrays: Vec<TypedArray<'a>>,
    row_idx: usize,
    num_rows: usize,
}

impl BatchRowIter<'_> {
    pub fn next_into(&mut self, row: &mut Vec<TypedValue>) -> bool {
        if self.row_idx >= self.num_rows {
            return false;
        }
        row.clear();
        for arr in &self.arrays {
            row.push(arr.value_at(self.row_idx));
        }
        self.row_idx += 1;
        true
    }
}

pub fn batch_rows(batch: &RecordBatch) -> Result<BatchRowIter<'_>> {
    let mut arrays = Vec::with_capacity(batch.num_columns());

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(col_idx);
        let typed = match field.data_type() {
            DataType::Int32 => TypedArray::Int32(
                col.as_any()
                    .downcast_ref::<Int32Array>()
                    .context("expected Int32Array")?,
            ),
            DataType::Int64 => TypedArray::Int64(
                col.as_any()
                    .downcast_ref::<Int64Array>()
                    .context("expected Int64Array")?,
            ),
            DataType::Utf8 => TypedArray::Utf8(
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .context("expected StringArray")?,
            ),
            DataType::Utf8View => TypedArray::Utf8View(
                col.as_any()
                    .downcast_ref::<StringViewArray>()
                    .context("expected StringViewArray")?,
            ),
            DataType::Date32 => TypedArray::Date32(
                col.as_any()
                    .downcast_ref::<Date32Array>()
                    .context("expected Date32Array")?,
            ),
            DataType::Decimal128(_, scale) => TypedArray::Decimal128(
                col.as_any()
                    .downcast_ref::<Decimal128Array>()
                    .context("expected Decimal128Array")?,
                *scale,
            ),
            dt => bail!("unsupported Arrow type: {:?}", dt),
        };
        arrays.push(typed);
    }

    Ok(BatchRowIter {
        arrays,
        row_idx: 0,
        num_rows: batch.num_rows(),
    })
}

pub trait TpchBackend: Send + Sync + 'static {
    fn drop_table(&self, table: &str) -> impl Future<Output = Result<()>> + Send;
    fn create_table(&self, table: &str, schema: &Schema) -> impl Future<Output = Result<()>> + Send;
    fn create_temp_table(
        &self,
        table: &str,
        schema: &Schema,
    ) -> impl Future<Output = Result<String>> + Send;
    fn copy_batches(
        &self,
        table: &str,
        schema: &Schema,
        batches: Box<dyn Iterator<Item = RecordBatch> + Send>,
    ) -> impl Future<Output = Result<usize>> + Send;
    fn copy_temp_to_target(
        &self,
        target: &str,
        temp: &str,
        schema: &Schema,
    ) -> impl Future<Output = Result<()>> + Send;
    fn upsert_from_temp(
        &self,
        target: &str,
        temp: &str,
        schema: &Schema,
    ) -> impl Future<Output = Result<()>> + Send;
    fn truncate_table(&self, table: &str) -> impl Future<Output = Result<()>> + Send;
    fn drop_temp_table(&self, table: &str) -> impl Future<Output = Result<()>> + Send;
    fn needs_temp_for_snapshot() -> bool;
}

pub async fn run<B: TpchBackend>(backend: Arc<B>, mode: Mode) -> Result<()> {
    match mode {
        Mode::Snapshot(args) => run_snapshot(backend, args).await,
        Mode::Cdc(args) => run_cdc(backend, args).await,
    }
}

async fn run_snapshot<B: TpchBackend>(backend: Arc<B>, args: SnapshotArgs) -> Result<()> {
    let tables = parse_tables(&args.tables)?;
    info!(
        "Snapshot: sf={}, tables={:?}, drop={}",
        args.scale_factor, tables, args.drop
    );

    if args.drop {
        for table in &tables {
            info!("Dropping [{}]...", table);
            backend.drop_table(table).await?;
        }
    }

    for table in &tables {
        let schema = TpchTable::new(table, args.scale_factor)?.schema();
        info!("Creating [{}]...", table);
        backend.create_table(table, &schema).await?;
    }

    if args.create_only {
        info!("Tables created (--create-only). Exiting.");
        return Ok(());
    }

    let mut set: JoinSet<Result<(String, usize)>> = JoinSet::new();
    for table in tables {
        let backend = Arc::clone(&backend);
        let sf = args.scale_factor;
        set.spawn(async move {
            let n = load_table_snapshot::<B>(backend, table.clone(), sf).await?;
            Ok((table, n))
        });
    }

    while let Some(result) = set.join_next().await {
        match result? {
            Ok((table, rows)) => info!("[{}]: loaded {} rows", table, rows),
            Err(e) => error!("Error: {}", e),
        }
    }

    info!("Snapshot complete.");
    Ok(())
}

async fn load_table_snapshot<B: TpchBackend>(
    backend: Arc<B>,
    table: String,
    scale_factor: f64,
) -> Result<usize> {
    let tpch = TpchTable::new(&table, scale_factor)?.with_batch_size(100_000);
    let schema = tpch.schema();

    let target = if B::needs_temp_for_snapshot() {
        backend.create_temp_table(&table, &schema).await?
    } else {
        table.clone()
    };

    let total = backend
        .copy_batches(&target, &schema, Box::new(tpch))
        .await?;
    info!("[{}]: {} rows", table, total);

    if B::needs_temp_for_snapshot() {
        info!("[{}]: copying temp to target...", table);
        backend
            .copy_temp_to_target(&table, &target, &schema)
            .await?;
        backend.drop_temp_table(&target).await?;
    }

    Ok(total)
}

async fn run_cdc<B: TpchBackend>(backend: Arc<B>, args: CdcArgs) -> Result<()> {
    let tables = parse_tables(&args.tables)?;
    let n = tables.len().max(1);
    let rate_per_table = args.rate / n;
    info!(
        "CDC: tables={:?}, rate={} ({}/table)",
        tables, args.rate, rate_per_table
    );

    let total_ops = Arc::new(AtomicU64::new(0));
    let start = std::time::Instant::now();

    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for (i, table) in tables.iter().enumerate() {
        let backend = Arc::clone(&backend);
        let total_ops = Arc::clone(&total_ops);
        let sf = args.scale_factor;
        let table = table.clone();
        // Give the first table any remainder so total rate is preserved
        let rate = if i == 0 {
            args.rate - rate_per_table * (n - 1)
        } else {
            rate_per_table
        };
        set.spawn(async move {
            cdc_worker::<B>(backend, table, rate, sf, total_ops, start).await
        });
    }

    while let Some(result) = set.join_next().await {
        result??;
    }
    Ok(())
}

async fn cdc_worker<B: TpchBackend>(
    backend: Arc<B>,
    table: String,
    rate: usize,
    scale_factor: f64,
    total_ops: Arc<AtomicU64>,
    start: std::time::Instant,
) -> Result<()> {
    let batch_size = (rate / 10).clamp(100, 1000);
    let interval = Duration::from_secs_f64(batch_size as f64 / rate as f64);
    let mut timer = tokio::time::interval(interval);

    let mut source = TpchTable::new(&table, scale_factor)?.with_batch_size(batch_size);
    let schema = source.schema();
    let temp = backend.create_temp_table(&table, &schema).await?;

    info!(
        "[{}]: CDC started, {} ops/sec (batch={})",
        table, rate, batch_size
    );

    let log_interval = Duration::from_secs(5);
    let mut last_log = std::time::Instant::now();

    loop {
        timer.tick().await;

        let batch = match source.next() {
            Some(b) => b,
            None => {
                source = TpchTable::new(&table, scale_factor)?.with_batch_size(batch_size);
                match source.next() {
                    Some(b) => b,
                    None => bail!("[{}]: generator produced no batches at sf={}", table, scale_factor),
                }
            }
        };

        let rows = batch.num_rows();
        if let Err(e) = cdc_batch::<B>(&backend, &table, &temp, &schema, batch).await {
            error!("[{}]: CDC error: {}", table, e);
            continue;
        }

        let ops =
            total_ops.fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed) + rows as u64;
        if last_log.elapsed() >= log_interval {
            let actual_rate = ops as f64 / start.elapsed().as_secs_f64();
            info!("CDC: {} total ops, {:.1} ops/sec", ops, actual_rate);
            last_log = std::time::Instant::now();
        }
    }
}

async fn cdc_batch<B: TpchBackend>(
    backend: &Arc<B>,
    table: &str,
    temp: &str,
    schema: &Schema,
    batch: RecordBatch,
) -> Result<()> {
    backend
        .copy_batches(temp, schema, Box::new(std::iter::once(batch)))
        .await?;
    backend.upsert_from_temp(table, temp, schema).await?;
    backend.truncate_table(temp).await?;
    Ok(())
}