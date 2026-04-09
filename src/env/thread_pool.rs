//! A concrete [`ThreadPool`] implementation backed by a pool of
//! `std::thread` workers and a multi-producer channel per priority level.
//!
//! Upstream RocksDB uses a custom thread pool in `util/threadpool_imp.cc`
//! with priority buckets (`LOW`, `HIGH`, `USER`, `BOTTOM`). We keep the
//! same four-bucket model but implement each bucket as an independent
//! `mpsc` channel + worker thread group. This is deliberately simple —
//! real production benchmarks would want work-stealing (rayon or a
//! custom crossbeam-based pool), but the Layer 2 shape exposes enough
//! for the engine's flush/compaction scheduling to work end-to-end.
//!
//! # Scope and limitations
//!
//! - Cancelling a task ([`ThreadPool::cancel`]) is a *best-effort*
//!   removal from the pending queue; tasks that have already started
//!   running always finish. Upstream behaves the same.
//! - Resizing shrinks the pool lazily — extra workers exit once they
//!   pop a poison pill from the channel. A follow-up layer can add a
//!   more precise shrink API.
//! - Panics inside tasks do not bring down the pool; they are caught
//!   via `std::panic::catch_unwind` and logged to stderr.

use crate::env::env_trait::{Priority, ScheduleHandle, ThreadPool};
use std::collections::HashSet;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// Message sent to a worker channel.
enum Message {
    /// Execute this task if `id` is still in the live-handle set.
    Task {
        id: ScheduleHandle,
        task: Box<dyn FnOnce() + Send + 'static>,
    },
    /// Worker should exit. One poison pill per worker to shrink.
    Shutdown,
}

/// One priority bucket: a sender, a set of live task IDs, and the
/// worker threads that drain the bucket.
struct PriorityBucket {
    sender: Sender<Message>,
    /// Handles that have been scheduled but not yet run. Workers
    /// consult this set before running a task so cancellation has a
    /// race-free check-and-run path.
    live: Arc<Mutex<HashSet<ScheduleHandle>>>,
    /// Worker join handles. Used by [`StdThreadPool::wait_for_jobs_and_join_all`]
    /// to wait for completion.
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl PriorityBucket {
    fn worker_count(&self) -> usize {
        self.workers.lock().unwrap().len()
    }

    fn set_worker_count(
        &self,
        new_count: usize,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    ) -> usize {
        let mut workers = self.workers.lock().unwrap();
        let old_count = workers.len();
        if new_count > old_count {
            for _ in old_count..new_count {
                workers.push(spawn_worker(Arc::clone(&receiver), Arc::clone(&self.live)));
            }
        } else {
            // Send one Shutdown per worker we want to stop. The
            // specific workers that pick them up don't matter.
            for _ in new_count..old_count {
                let _ = self.sender.send(Message::Shutdown);
            }
            // We do NOT join here — shrinking is lazy. Workers that
            // consume a Shutdown message exit on their own; their
            // join handles are cleaned up on the next full shutdown.
        }
        old_count
    }
}

fn spawn_worker(
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    live: Arc<Mutex<HashSet<ScheduleHandle>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        loop {
            let msg = {
                let rx = receiver.lock().expect("thread pool receiver poisoned");
                match rx.recv() {
                    Ok(m) => m,
                    Err(_) => return, // channel closed → exit
                }
            };
            match msg {
                Message::Shutdown => return,
                Message::Task { id, task } => {
                    // Check-and-remove: only run if the task is still
                    // considered live by the pool. Cancellation races
                    // are resolved by whichever side wins the lock.
                    let is_live = {
                        let mut set = live.lock().unwrap();
                        set.remove(&id)
                    };
                    if is_live {
                        // Catch panics so one bad task doesn't kill the worker.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task));
                    }
                }
            }
        }
    })
}

/// Concrete [`ThreadPool`] implementation.
///
/// One bucket per priority level. Each bucket has its own channel and
/// worker set, so high-priority and low-priority tasks don't contend
/// for the same queue.
pub struct StdThreadPool {
    low: PriorityBucket,
    high: PriorityBucket,
    user: PriorityBucket,
    bottom: PriorityBucket,
    /// Monotonic counter for task IDs.
    next_id: Mutex<ScheduleHandle>,
    /// Receivers for each bucket, stored so
    /// [`PriorityBucket::set_worker_count`] can spawn replacement
    /// workers without re-creating the channel.
    low_recv: Arc<Mutex<mpsc::Receiver<Message>>>,
    high_recv: Arc<Mutex<mpsc::Receiver<Message>>>,
    user_recv: Arc<Mutex<mpsc::Receiver<Message>>>,
    bottom_recv: Arc<Mutex<mpsc::Receiver<Message>>>,
}

impl StdThreadPool {
    /// Build a new pool. Each bucket starts with `initial_workers`
    /// threads. Pass `0` for a given bucket's `initial_workers` to
    /// defer worker creation.
    pub fn new(low: usize, high: usize, user: usize, bottom: usize) -> Self {
        // We need to own the receivers so we can pass clones into
        // new workers if the pool is resized. PriorityBucket::new
        // creates the channel internally, so we re-create the whole
        // bucket here to expose the receiver.
        fn make_bucket(initial: usize) -> (PriorityBucket, Arc<Mutex<mpsc::Receiver<Message>>>) {
            let (sender, receiver) = mpsc::channel::<Message>();
            let receiver = Arc::new(Mutex::new(receiver));
            let live = Arc::new(Mutex::new(HashSet::new()));
            let mut workers = Vec::with_capacity(initial);
            for _ in 0..initial {
                workers.push(spawn_worker(Arc::clone(&receiver), Arc::clone(&live)));
            }
            (
                PriorityBucket {
                    sender,
                    live,
                    workers: Mutex::new(workers),
                },
                receiver,
            )
        }

        let (low_b, low_recv) = make_bucket(low);
        let (high_b, high_recv) = make_bucket(high);
        let (user_b, user_recv) = make_bucket(user);
        let (bottom_b, bottom_recv) = make_bucket(bottom);

        Self {
            low: low_b,
            high: high_b,
            user: user_b,
            bottom: bottom_b,
            next_id: Mutex::new(1),
            low_recv,
            high_recv,
            user_recv,
            bottom_recv,
        }
    }

    fn bucket(&self, p: Priority) -> &PriorityBucket {
        match p {
            Priority::Low => &self.low,
            Priority::High => &self.high,
            Priority::User => &self.user,
            Priority::Bottom => &self.bottom,
        }
    }

    fn receiver(&self, p: Priority) -> Arc<Mutex<mpsc::Receiver<Message>>> {
        match p {
            Priority::Low => Arc::clone(&self.low_recv),
            Priority::High => Arc::clone(&self.high_recv),
            Priority::User => Arc::clone(&self.user_recv),
            Priority::Bottom => Arc::clone(&self.bottom_recv),
        }
    }
}

impl Default for StdThreadPool {
    /// Default pool: 1 worker per bucket. Good enough for tests; real
    /// engines should pick larger numbers based on cpu count.
    fn default() -> Self {
        Self::new(1, 1, 1, 1)
    }
}

impl ThreadPool for StdThreadPool {
    fn schedule(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        priority: Priority,
    ) -> ScheduleHandle {
        let id = {
            let mut counter = self.next_id.lock().unwrap();
            let id = *counter;
            *counter += 1;
            id
        };
        let bucket = self.bucket(priority);
        {
            let mut live = bucket.live.lock().unwrap();
            live.insert(id);
        }
        // Unwrap is fine: Receiver is held by workers and by our
        // own Arc<Mutex<>>, so the channel is never closed before
        // the pool is dropped.
        bucket
            .sender
            .send(Message::Task { id, task })
            .expect("failed to enqueue task; pool likely dropped");
        id
    }

    fn cancel(&self, handle: ScheduleHandle) -> bool {
        // Search every bucket — the caller didn't tell us which one.
        for bucket in [&self.low, &self.high, &self.user, &self.bottom] {
            let mut live = bucket.live.lock().unwrap();
            if live.remove(&handle) {
                return true;
            }
        }
        false
    }

    fn set_background_threads(&self, num: usize, priority: Priority) -> usize {
        self.bucket(priority)
            .set_worker_count(num, self.receiver(priority))
    }

    fn get_background_threads(&self, priority: Priority) -> usize {
        self.bucket(priority).worker_count()
    }

    fn wait_for_jobs_and_join_all(&self) {
        // Send one Shutdown per worker in every bucket, then drain
        // the join handles. This is irreversible — the pool cannot
        // be used after calling this.
        for bucket in [&self.low, &self.high, &self.user, &self.bottom] {
            let count = bucket.worker_count();
            for _ in 0..count {
                let _ = bucket.sender.send(Message::Shutdown);
            }
            let workers = std::mem::take(&mut *bucket.workers.lock().unwrap());
            for handle in workers {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    fn wait_until<F: Fn() -> bool>(f: F, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if f() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        f()
    }

    #[test]
    fn tasks_run_on_worker_threads() {
        let pool = StdThreadPool::new(2, 0, 0, 0);
        let counter = Arc::new(AtomicU32::new(0));
        for _ in 0..10 {
            let c = Arc::clone(&counter);
            pool.schedule(Box::new(move || {
                c.fetch_add(1, Ordering::Relaxed);
            }), Priority::Low);
        }
        assert!(wait_until(
            || counter.load(Ordering::Relaxed) == 10,
            Duration::from_secs(2)
        ));
        pool.wait_for_jobs_and_join_all();
    }

    #[test]
    fn cancel_prevents_execution() {
        // Single-threaded pool + a long-running first task so our
        // cancel beats the scheduled second task to the worker.
        let pool = StdThreadPool::new(1, 0, 0, 0);
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let gate2 = Arc::clone(&gate);
        pool.schedule(Box::new(move || {
            let (lock, cv) = &*gate2;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = cv.wait(started).unwrap();
            }
        }), Priority::Low);

        let ran = Arc::new(AtomicU32::new(0));
        let ran2 = Arc::clone(&ran);
        let handle = pool.schedule(Box::new(move || {
            ran2.store(1, Ordering::Relaxed);
        }), Priority::Low);

        assert!(pool.cancel(handle));

        // Release the blocker so the pool can shut down.
        {
            let (lock, cv) = &*gate;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        pool.wait_for_jobs_and_join_all();
        assert_eq!(ran.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn priorities_are_independent_queues() {
        let pool = StdThreadPool::new(0, 1, 0, 0);
        let hit = Arc::new(AtomicU32::new(0));
        let h = Arc::clone(&hit);
        pool.schedule(Box::new(move || {
            h.fetch_add(1, Ordering::Relaxed);
        }), Priority::High);
        assert!(wait_until(
            || hit.load(Ordering::Relaxed) == 1,
            Duration::from_secs(2)
        ));
        pool.wait_for_jobs_and_join_all();
    }

    #[test]
    fn worker_count_updates() {
        let pool = StdThreadPool::new(1, 0, 0, 0);
        assert_eq!(pool.get_background_threads(Priority::Low), 1);
        let prev = pool.set_background_threads(3, Priority::Low);
        assert_eq!(prev, 1);
        assert_eq!(pool.get_background_threads(Priority::Low), 3);
        pool.wait_for_jobs_and_join_all();
    }

    #[test]
    fn panicking_task_does_not_kill_worker() {
        let pool = StdThreadPool::new(1, 0, 0, 0);
        pool.schedule(Box::new(|| panic!("whoops")), Priority::Low);
        // After the panic, a follow-up task should still run.
        let ran = Arc::new(AtomicU32::new(0));
        let r = Arc::clone(&ran);
        pool.schedule(Box::new(move || {
            r.store(1, Ordering::Relaxed);
        }), Priority::Low);
        assert!(wait_until(
            || ran.load(Ordering::Relaxed) == 1,
            Duration::from_secs(2)
        ));
        pool.wait_for_jobs_and_join_all();
    }
}
