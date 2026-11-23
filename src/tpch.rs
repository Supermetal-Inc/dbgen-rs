use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crate::nested::NestedShape;
use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use chrono::NaiveDate;
use clap::{Args, Subcommand};
use log::info;
use tokio::task::JoinSet;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
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
    for (i, name) in names.iter().enumerate() {
        if !ALL_TABLES.contains(&name.as_str()) {
            bail!(
                "unknown table: '{}'. valid tables: {}",
                name,
                ALL_TABLES.join(", ")
            );
        }
        if names[..i].contains(name) {
            bail!("duplicate table: '{}'", name);
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

pub fn format_decimal128(val: i128, scale: i8) -> String {
    if scale == 0 {
        return val.to_string();
    }
    let sign = if val < 0 { "-" } else { "" };
    let abs = val.unsigned_abs();
    let divisor = 10_u128.pow(scale as u32);
    format!(
        "{}{}.{:0>width$}",
        sign,
        abs / divisor,
        abs % divisor,
        width = scale as usize
    )
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

impl Mode {
    /// Pools must hold at least this many connections, or extra workers only hold chunks.
    pub fn snapshot_workers(&self) -> usize {
        match self {
            Mode::Snapshot(args) => args.workers.get(),
            Mode::Cdc(_) => 0,
        }
    }
}

#[derive(Args, Clone)]
pub struct SnapshotArgs {
    /// Shared snapshot writers
    #[arg(long, default_value = "8")]
    pub workers: std::num::NonZeroUsize,
    /// Maximum rows per transaction
    #[arg(long, default_value = "100000")]
    pub commit_rows: std::num::NonZeroUsize,
    #[arg(long, default_value_t = 0.01)]
    pub scale_factor: f64,
    #[arg(long, default_value = "all")]
    pub tables: String,
    #[arg(long, default_value_t = false)]
    pub drop: bool,
    #[arg(long, default_value_t = false)]
    pub create_only: bool,
    /// Store most lineitem columns in one JSON column. Only PostgreSQL supports this
    #[arg(long, default_value_t = false)]
    pub nested: bool,
}

#[derive(Args, Clone)]
pub struct CdcArgs {
    #[arg(long, default_value = "lineitem,orders")]
    pub tables: String,
    #[arg(long, default_value_t = 100)]
    pub rate: usize,
    #[arg(long, default_value_t = 1.0)]
    pub scale_factor: f64,
    /// Must match the --nested value used for the snapshot
    #[arg(long, default_value_t = false)]
    pub nested: bool,
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
                scale_factor,
                1,
                1,
            ))),
            "customer" => TpchTable::Customer(CustomerArrow::new(CustomerGenerator::new(
                scale_factor,
                1,
                1,
            ))),
            "part" => TpchTable::Part(PartArrow::new(PartGenerator::new(scale_factor, 1, 1))),
            "partsupp" => TpchTable::PartSupp(PartSuppArrow::new(PartSuppGenerator::new(
                scale_factor,
                1,
                1,
            ))),
            "orders" => TpchTable::Orders(OrderArrow::new(OrderGenerator::new(scale_factor, 1, 1))),
            "lineitem" => TpchTable::LineItem(LineItemArrow::new(LineItemGenerator::new(
                scale_factor,
                1,
                1,
            ))),
            _ => bail!("unknown table: {}", name),
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        match self {
            TpchTable::Region(g) => g.schema(),
            TpchTable::Nation(g) => g.schema(),
            TpchTable::Supplier(g) => g.schema(),
            TpchTable::Customer(g) => g.schema(),
            TpchTable::Part(g) => g.schema(),
            TpchTable::PartSupp(g) => g.schema(),
            TpchTable::Orders(g) => g.schema(),
            TpchTable::LineItem(g) => g.schema(),
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
        let batch = match self {
            TpchTable::Region(g) => g.next(),
            TpchTable::Nation(g) => g.next(),
            TpchTable::Supplier(g) => g.next(),
            TpchTable::Customer(g) => g.next(),
            TpchTable::Part(g) => g.next(),
            TpchTable::PartSupp(g) => g.next(),
            TpchTable::Orders(g) => g.next(),
            TpchTable::LineItem(g) => g.next(),
        }?;
        // tpchgen-arrow builds both the schema and the columns, so try_new can only fail on a
        // tpchgen-arrow bug.
        Some(batch.expect("tpchgen-arrow batch matches its schema"))
    }
}

pub struct TableSource {
    table: TpchTable,
    nested: Option<NestedShape>,
}

impl TableSource {
    pub fn new(name: &str, scale_factor: f64, nested: bool) -> Result<Self> {
        let table = TpchTable::new(name, scale_factor)?;
        let nested = if nested {
            NestedShape::new(name, &table.schema())
        } else {
            None
        };
        Ok(Self { table, nested })
    }

    pub fn schema(&self) -> Arc<Schema> {
        match &self.nested {
            Some(shape) => shape.schema(),
            None => self.table.schema(),
        }
    }

    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self {
            table: self.table.with_batch_size(batch_size),
            nested: self.nested,
        }
    }
}

impl Iterator for TableSource {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.table.next()?;
        Some(match &self.nested {
            // The shape is fixed per table, so a failure here is a bug in NestedShape.
            Some(shape) => shape
                .apply(&batch)
                .expect("nested shape encodes TPC-H batch"),
            None => batch,
        })
    }
}

#[derive(Debug)]
pub enum TypedValue<'a> {
    Int32(Option<i32>),
    Int64(Option<i64>),
    Utf8(Option<&'a str>),
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

impl<'a> TypedArray<'a> {
    fn value_at(&self, idx: usize) -> TypedValue<'a> {
        match self {
            TypedArray::Int32(a) => TypedValue::Int32((!a.is_null(idx)).then(|| a.value(idx))),
            TypedArray::Int64(a) => TypedValue::Int64((!a.is_null(idx)).then(|| a.value(idx))),
            TypedArray::Utf8(a) => TypedValue::Utf8((!a.is_null(idx)).then(|| a.value(idx))),
            TypedArray::Utf8View(a) => TypedValue::Utf8((!a.is_null(idx)).then(|| a.value(idx))),
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

impl<'a> BatchRowIter<'a> {
    pub fn next_into(&mut self, row: &mut Vec<TypedValue<'a>>) -> bool {
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
    fn create_table(&self, table: &str, schema: &Schema)
    -> impl Future<Output = Result<()>> + Send;
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
    fn upsert_from_temp(
        &self,
        target: &str,
        temp: &str,
        schema: &Schema,
    ) -> impl Future<Output = Result<()>> + Send;
    fn truncate_table(&self, table: &str) -> impl Future<Output = Result<()>> + Send;
    fn drop_temp_table(&self, table: &str) -> impl Future<Output = Result<()>> + Send;

    /// Write and commit one independent chunk; check cancellation before commit.
    fn write_snapshot_chunk(
        &self,
        table: String,
        batch: RecordBatch,
        progress: Arc<crate::snapshot::Progress>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send;

    fn is_blocking() -> bool {
        false
    }

    fn supports_nested() -> bool {
        false
    }
}

pub async fn run<B: TpchBackend>(backend: Arc<B>, mode: Mode) -> Result<()> {
    let sf = match &mode {
        Mode::Snapshot(a) => a.scale_factor,
        Mode::Cdc(a) => a.scale_factor,
    };
    if !sf.is_finite() || sf <= 0.0 {
        bail!("scale factor must be finite and positive");
    }
    if matches!(&mode, Mode::Cdc(a) if a.rate == 0) {
        bail!("CDC rate must be positive");
    }
    let nested = match &mode {
        Mode::Snapshot(a) => a.nested,
        Mode::Cdc(a) => a.nested,
    };
    if nested && !B::supports_nested() {
        bail!("--nested is not supported for this database");
    }
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
        let schema = TableSource::new(table, args.scale_factor, args.nested)?.schema();
        info!("Creating [{}]...", table);
        backend.create_table(table, &schema).await?;
    }

    if args.create_only {
        info!("Tables created (--create-only). Exiting.");
        return Ok(());
    }

    use crate::snapshot::{self, Progress};
    let progress = Arc::new(Progress::default());
    let chunks = snapshot_chunks(
        tables,
        args.scale_factor,
        args.nested,
        args.commit_rows.get(),
        progress.clone(),
    )?;
    let cancel = tokio_util::sync::CancellationToken::new();
    let counters = progress.clone();
    let load = snapshot::load(
        chunks,
        args.workers,
        cancel.clone(),
        move |(table, batch), token| {
            let backend = backend.clone();
            let counters = counters.clone();
            async move {
                backend
                    .write_snapshot_chunk(table, batch, counters, token)
                    .await
            }
        },
    );
    tokio::pin!(load);
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    let result = loop {
        tokio::select! {
            result = &mut load => break result,
            _ = interval.tick() => progress.log(),
            // Tokio replaces SIGINT's default action, so a call stuck on a lock would
            // otherwise only die to SIGKILL. Open transactions roll back on disconnect.
            _ = tokio::signal::ctrl_c() => {
                if cancel.is_cancelled() {
                    progress.log();
                    std::process::exit(130);
                }
                cancel.cancel();
                info!(
                    "Stopping snapshot admission; waiting for in-flight database calls (Ctrl-C again to exit now)"
                );
            }
        }
    };
    progress.log();
    result.context("snapshot may be partially committed; recreate selected tables before retrying (no automatic resume)")?;
    info!("Snapshot complete.");
    Ok(())
}

// Round-robin sources give small tables a chance while allowing every worker to
// drain the final large table. Each iterator is advanced exactly once per chunk.
fn snapshot_chunks(
    tables: Vec<String>,
    sf: f64,
    nested: bool,
    rows: usize,
    progress: Arc<crate::snapshot::Progress>,
) -> Result<impl Iterator<Item = (String, RecordBatch)> + Send> {
    let mut sources = tables
        .into_iter()
        .map(|table| {
            let source = TableSource::new(&table, sf, nested)?.with_batch_size(rows);
            Ok((table, source))
        })
        .collect::<Result<std::collections::VecDeque<_>>>()?;
    Ok(std::iter::from_fn(move || {
        while let Some((table, mut source)) = sources.pop_front() {
            if let Some(batch) = source.next() {
                progress.add_generated(batch.num_rows());
                sources.push_back((table.clone(), source));
                return Some((table, batch));
            }
        }
        None
    }))
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
    let handle = tokio::runtime::Handle::current();
    for (i, table) in tables.iter().enumerate() {
        let backend = Arc::clone(&backend);
        let total_ops = Arc::clone(&total_ops);
        let sf = args.scale_factor;
        let nested = args.nested;
        let table = table.clone();
        let rate = if i == 0 {
            args.rate - rate_per_table * (n - 1)
        } else {
            rate_per_table
        };
        if rate == 0 {
            continue;
        }
        let task = async move {
            cdc_worker::<B>(backend, table, rate, sf, nested, total_ops, start).await
        };
        if B::is_blocking() {
            let handle = handle.clone();
            set.spawn_blocking(move || handle.block_on(task));
        } else {
            set.spawn(task);
        }
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
    nested: bool,
    total_ops: Arc<AtomicU64>,
    start: std::time::Instant,
) -> Result<()> {
    let batch_size = (rate / 10).clamp(100, 1000);
    let interval = Duration::from_secs_f64(batch_size as f64 / rate as f64);
    let mut timer = tokio::time::interval(interval);

    let mut source = TableSource::new(&table, scale_factor, nested)?.with_batch_size(batch_size);
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
                source =
                    TableSource::new(&table, scale_factor, nested)?.with_batch_size(batch_size);
                match source.next() {
                    Some(b) => b,
                    None => bail!(
                        "[{}]: generator produced no batches at sf={}",
                        table,
                        scale_factor
                    ),
                }
            }
        };

        let rows = batch.num_rows();
        if let Err(e) = cdc_batch::<B>(&backend, &table, &temp, &schema, batch).await {
            return Err(e).with_context(|| format!("[{table}]: CDC batch failed"));
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

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn chunks_preserve_every_table_and_bound_transaction_rows() {
        let progress = Arc::new(crate::snapshot::Progress::default());
        let mut actual = std::collections::HashMap::<String, Vec<RecordBatch>>::new();
        let mut order = Vec::new();
        for (table, batch) in snapshot_chunks(
            parse_tables("all").unwrap(),
            0.001,
            false,
            113,
            progress.clone(),
        )
        .unwrap()
        {
            assert!(batch.num_rows() <= 113);
            order.push(table.clone());
            actual.entry(table).or_default().push(batch);
        }
        assert_eq!(&order[..8], ALL_TABLES);
        let mut total = 0;
        for table in ALL_TABLES {
            let source = TableSource::new(table, 0.001, false).unwrap();
            let schema = source.schema();
            let expected =
                arrow::compute::concat_batches(&schema, source.collect::<Vec<_>>().iter()).unwrap();
            let actual = arrow::compute::concat_batches(&schema, &actual[*table]).unwrap();
            assert_eq!(actual, expected, "{table}");
            total += actual.num_rows() as u64;
        }
        assert_eq!(
            progress
                .generated
                .load(std::sync::atomic::Ordering::Relaxed),
            total
        );
    }

    #[test]
    fn duplicate_tables_are_rejected() {
        let err = parse_tables("region, Region").unwrap_err();
        assert_eq!(err.to_string(), "duplicate table: 'region'");
        assert_eq!(parse_tables("region,nation").unwrap(), ["region", "nation"]);
    }
}
