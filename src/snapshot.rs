//! Bounded shared snapshot workers. Any idle worker takes the next chunk, regardless
//! of table; there is no per-table writer allocation or unbounded batch queue.
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Monotonic observations, not a recovery checkpoint. Written includes rows later
/// rolled back; committed counts only acknowledged commits (a lost reply is unknown).
#[derive(Default)]
pub struct Progress {
    pub generated: AtomicU64,
    pub written: AtomicU64,
    pub committed: AtomicU64,
}

impl Progress {
    pub fn add_generated(&self, rows: usize) {
        self.generated.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn add_written(&self, rows: usize) {
        self.written.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn add_committed(&self, rows: usize) {
        self.committed.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn log(&self) {
        log::info!(
            "Snapshot rows: generated={}, written={}, committed={}",
            self.generated.load(Ordering::Relaxed),
            self.written.load(Ordering::Relaxed),
            self.committed.load(Ordering::Relaxed)
        );
    }
}

/// Pull work lazily with at most `workers` chunks resident. Generation runs on the
/// blocking executor, serialized through the iterator; database I/O overlaps it.
/// Errors stop admission and drain in-flight work. Dropping the future cancels
/// cooperatively: blocking database calls cannot be forcibly interrupted.
/// Writers must check cancellation between calls and roll back uncommitted work.
pub async fn load<I, F, Fut>(
    jobs: I,
    workers: NonZeroUsize,
    cancel: CancellationToken,
    write: F,
) -> Result<()>
where
    I: Iterator + Send + 'static,
    I::Item: Send + 'static,
    F: Fn(I::Item, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let _cancel_on_drop = cancel.clone().drop_guard();
    let jobs = Arc::new(Mutex::new(jobs));
    let write = Arc::new(write);
    let mut tasks = JoinSet::new();
    for _ in 0..workers.get() {
        let jobs = jobs.clone();
        let write = write.clone();
        let cancel = cancel.clone();
        tasks.spawn(async move {
            // Also stop siblings on panic, before the coordinator joins this task.
            let guard = cancel.clone().drop_guard();
            loop {
                let jobs = jobs.clone();
                let token = cancel.clone();
                let job = tokio::task::spawn_blocking(move || {
                    let mut jobs = jobs
                        .lock()
                        .map_err(|_| anyhow!("snapshot generator panicked"))?;
                    Ok::<_, anyhow::Error>(if token.is_cancelled() {
                        None
                    } else {
                        jobs.next()
                    })
                })
                .await??;
                let Some(job) = job else { break };
                if cancel.is_cancelled() {
                    break;
                }
                write(job, cancel.clone()).await?;
            }
            guard.disarm();
            Ok::<_, anyhow::Error>(())
        });
    }
    let mut error = None;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap_or_else(|e| Err(e.into()));
        if let Err(e) = result {
            error.get_or_insert(e);
        }
    }
    match error {
        Some(e) => Err(e),
        None if cancel.is_cancelled() => Err(anyhow!("snapshot cancelled")),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn shared_workers_are_bounded_and_assign_each_chunk_once() {
        for count in [1, 4] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let generated = Arc::new(AtomicUsize::new(0));
            let finished = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(Barrier::new(count));
            let jobs = (0..40).inspect({
                let generated = generated.clone();
                let finished = finished.clone();
                move |_| {
                    let n = generated.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!(n - finished.load(Ordering::SeqCst) <= count);
                }
            });
            let output = seen.clone();
            load(
                jobs,
                NonZeroUsize::new(count).unwrap(),
                CancellationToken::new(),
                move |job, _| {
                    let output = output.clone();
                    let barrier = barrier.clone();
                    let finished = finished.clone();
                    async move {
                        // This deadlocks if ready chunks for the same source are serialized.
                        barrier.wait().await;
                        output.lock().unwrap().push(job);
                        finished.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await
            .unwrap();
            let mut seen = seen.lock().unwrap();
            seen.sort();
            assert_eq!(*seen, (0..40).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn failure_stops_admission_and_drains_siblings() {
        let generated = Arc::new(AtomicUsize::new(0));
        let drained = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let jobs = (0..100).inspect({
            let generated = generated.clone();
            move |_| {
                generated.fetch_add(1, Ordering::SeqCst);
            }
        });
        let output = drained.clone();
        let result = load(
            jobs,
            NonZeroUsize::new(4).unwrap(),
            CancellationToken::new(),
            move |job, cancel| {
                let barrier = barrier.clone();
                let output = output.clone();
                async move {
                    barrier.wait().await;
                    if job == 0 {
                        return Err(anyhow!("injected write failure"));
                    }
                    cancel.cancelled().await;
                    output.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("injected"));
        assert_eq!(generated.load(Ordering::SeqCst), 4);
        assert_eq!(drained.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn cancellation_and_panics_are_errors() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            load(
                0..100,
                NonZeroUsize::new(1).unwrap(),
                cancel,
                |_, _| async {
                    panic!("cancelled work must not start");
                }
            )
            .await
            .is_err()
        );
        let jobs = std::iter::from_fn(|| -> Option<usize> { panic!("generator failure") });
        assert!(
            load(
                jobs,
                NonZeroUsize::new(2).unwrap(),
                CancellationToken::new(),
                |_, _| async { Ok(()) }
            )
            .await
            .is_err()
        );
        assert!(
            load(
                0..100,
                NonZeroUsize::new(2).unwrap(),
                CancellationToken::new(),
                |_, _| async { panic!("writer failure") }
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn dropping_coordinator_cancels_blocking_writer() {
        let cancel = CancellationToken::new();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(Mutex::new(Some(started_tx)));
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let done = Arc::new(Mutex::new(Some(done_tx)));
        let task = tokio::spawn(load(
            0..1,
            NonZeroUsize::new(1).unwrap(),
            cancel.clone(),
            move |_, token| {
                let started = started.clone();
                let done = done.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        started.lock().unwrap().take().unwrap().send(()).unwrap();
                        while !token.is_cancelled() {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        done.lock().unwrap().take().unwrap().send(()).unwrap();
                    })
                    .await?;
                    Ok(())
                }
            },
        ));
        started_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(cancel.is_cancelled());
    }
}
