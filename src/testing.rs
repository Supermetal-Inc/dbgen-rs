//! Shared scenario for the ignored per-backend snapshot integration tests.
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Result;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;

use crate::snapshot::{self, Progress};
use crate::tpch::TpchBackend;

/// CLI arguments for a disposable localhost database, from DBGEN_TEST_PORT/PASSWORD.
pub fn cli_args(bin: &str, user: &str, database: &str) -> Result<Vec<String>> {
    let port = std::env::var("DBGEN_TEST_PORT")?;
    let password = std::env::var("DBGEN_TEST_PASSWORD")?;
    Ok([
        bin,
        "--host",
        "127.0.0.1",
        "--port",
        &port,
        "--user",
        user,
        "--password",
        &password,
        "--database",
        database,
        "snapshot",
    ]
    .map(String::from)
    .to_vec())
}

/// `table` must be empty, with a single integer primary key column named `id`.
pub async fn check_snapshot_chunks<B, C, Fut>(backend: Arc<B>, table: &str, count: C) -> Result<()>
where
    B: TpchBackend,
    C: Fn() -> Fut,
    Fut: Future<Output = Result<u64>>,
{
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        |ids: Vec<i32>| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]);
    let progress = Arc::new(Progress::default());
    let committed = || progress.committed.load(Ordering::Relaxed);

    // Concurrent chunks through the shared workers.
    let chunks = vec![
        batch((0..10_003).collect())?,
        batch((10_003..20_006).collect())?,
        batch(vec![20_006])?,
    ];
    let (writer, name, counters) = (backend.clone(), table.to_string(), progress.clone());
    snapshot::load(
        chunks.into_iter(),
        NonZeroUsize::new(3).unwrap(),
        CancellationToken::new(),
        move |chunk, cancel| {
            let (writer, name, counters) = (writer.clone(), name.clone(), counters.clone());
            async move {
                writer
                    .write_snapshot_chunk(name, chunk, counters, cancel)
                    .await
            }
        },
    )
    .await?;
    assert_eq!(count().await?, 20_007);
    assert_eq!(committed(), 20_007);

    // The duplicate key arrives last, after the driver has already sent 20,000 rows.
    let duplicate = batch((30_000..50_000).chain([0]).collect())?;
    let result = backend
        .write_snapshot_chunk(
            table.into(),
            duplicate,
            progress.clone(),
            CancellationToken::new(),
        )
        .await;
    assert!(result.is_err(), "duplicate key chunk committed");

    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = backend
        .write_snapshot_chunk(table.into(), batch(vec![60_000])?, progress.clone(), cancel)
        .await;
    assert_eq!(result.unwrap_err().to_string(), "snapshot cancelled");

    // Earlier commits survive both failures, and pooled connections stay usable.
    backend
        .write_snapshot_chunk(
            table.into(),
            batch(vec![70_000])?,
            progress.clone(),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(count().await?, 20_008);
    assert_eq!(committed(), 20_008);
    assert!(progress.written.load(Ordering::Relaxed) >= committed());
    Ok(())
}
