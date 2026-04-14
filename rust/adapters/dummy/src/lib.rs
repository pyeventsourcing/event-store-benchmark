use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::hint::spin_loop;
use anyhow::Result;
use async_trait::async_trait;
use bench_core::adapter::{
    EventData, EventStoreAdapter, ReadEvent, ReadRequest, StoreManager, StoreManagerFactory,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::mpsc;
use tokio::sync::oneshot;

struct ScheduledRequest {
    release_at: Instant,
    tx: oneshot::Sender<()>,
}

impl PartialEq for ScheduledRequest {
    fn eq(&self, other: &Self) -> bool {
        self.release_at == other.release_at
    }
}

impl Eq for ScheduledRequest {}

impl PartialOrd for ScheduledRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap
        other.release_at.cmp(&self.release_at)
    }
}

pub struct Scheduler {
    tx: mpsc::Sender<ScheduledRequest>,
    delay: Duration,
}

impl Scheduler {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<ScheduledRequest>();
        
        std::thread::Builder::new()
            .name("scheduler-thread".to_string())
            .spawn(move || {
                let mut heap = BinaryHeap::new();
                
                loop {
                    // Try to drain new requests into the heap
                    while let Ok(req) = rx.try_recv() {
                        heap.push(req);
                    }

                    if let Some(req) = heap.peek() {
                        let now = Instant::now();
                        if now >= req.release_at {
                            if let Some(req) = heap.pop() {
                                let _ = req.tx.send(());
                            }
                            // Continue immediately to check for next due request
                            continue;
                        } else {
                            // Spin tightly until the next request is due
                            // This is high accuracy and low jitter
                            spin_loop();
                            continue;
                        }
                    }

                    // Heap is empty, wait for a new request (blocking)
                    match rx.recv() {
                        Ok(req) => {
                            heap.push(req);
                        }
                        Err(_) => break, // Channel closed
                    }
                }
            })
            .expect("failed to spawn scheduler thread");

        Self { tx, delay: Duration::from_micros(1000) }
    }

    pub async fn wait(&self, release_at: Instant) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(ScheduledRequest { release_at, tx });
        let _ = rx.await;
    }
}

pub struct DummyStoreManager {
    scheduler: Arc<Scheduler>,
}

impl DummyStoreManager {
    pub fn new() -> Self {
        Self {
            scheduler: Arc::new(Scheduler::new()),
        }
    }
}

#[async_trait]
impl StoreManager for DummyStoreManager {
    fn use_docker(&self) -> bool { true }
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }
    async fn pull(&mut self) -> Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }
    fn container_id(&self) -> Option<String> {
        None
    }
    fn name(&self) -> &'static str {
        "dummy"
    }
    async fn create_adapter(&self) -> Result<Arc<dyn EventStoreAdapter>> {
        Ok(Arc::new(DummyAdapter {
            scheduler: self.scheduler.clone(),
        }))
    }
    async fn logs(&self) -> Result<String> {
        Ok(String::new())
    }
}

pub struct DummyAdapter {
    scheduler: Arc<Scheduler>,
}

#[async_trait]
impl EventStoreAdapter for DummyAdapter {
    fn as_any(&self) -> &dyn std::any::Any { self }
    async fn append(&self, _events: Vec<EventData>) -> Result<()> {
        self.scheduler.wait(Instant::now() + self.scheduler.delay).await;
        Ok(())
    }
    async fn read(&self, req: ReadRequest) -> Result<Vec<ReadEvent>> {
        self.scheduler.wait(Instant::now() + self.scheduler.delay).await;
        Ok((0..req.limit.unwrap_or(1))
            .map(|_| ReadEvent {
                offset: 0,
                event_type: String::from("DummyEvent"),
                payload: vec![],
                timestamp_ms: 0,
            })
            .collect())
    }
}

pub struct DummyFactory;

impl StoreManagerFactory for DummyFactory {
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn create_store_manager(
        &self,
        _data_dir: Option<String>,
        _use_docker: bool,
    ) -> Result<Box<dyn StoreManager>> {
        Ok(Box::new(DummyStoreManager::new()))
    }
}