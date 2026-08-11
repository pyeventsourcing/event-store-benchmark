use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::OnceLock;

/// Tag layout applied to prepopulated events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TagScheme {
    /// One tag per event: its stream name. The historical default.
    #[default]
    Single,
    /// The stream tag plus graded-selectivity tags `s1000:{g%1000}`, `s100:{g%100}`,
    /// `s10:{g%10}` (g = global event index), so a query for `s{d}:0` matches the `1/d`
    /// fraction of the log. Enables cardinality/selectivity read benchmarks.
    Graded,
}

/// Setup/prepopulation configuration for workloads that need data seeding
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SetupConfig {
    /// Number of events to prepopulate during setup phase
    pub prepopulate_events: u64,
    /// Number of streams to distribute prepopulated events across
    pub prepopulate_streams: u64,
    /// Tag layout for prepopulated events (default: one stream tag per event).
    #[serde(default)]
    pub tag_scheme: TagScheme,
    /// How many concurrent seeding connections to use during prepopulation. Default 1 keeps
    /// the historical single-threaded behaviour; a large cold-read corpus (many GB) needs a
    /// higher value to seed in reasonable time. Streams are partitioned across the tasks, and
    /// each event's global index is derived from its stream position, so the graded tag scheme
    /// stays deterministic regardless of concurrency.
    #[serde(default = "default_prepopulate_concurrency")]
    pub prepopulate_concurrency: usize,
}

fn default_prepopulate_concurrency() -> usize {
    1
}

fn pulled_images() -> &'static Mutex<HashSet<String>> {
    static PULLED_IMAGES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    PULLED_IMAGES.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Mark an image as pulled for the current session. Returns true if it was already pulled.
pub fn mark_image_pulled(image_name: &str) -> bool {
    let mut pulled = pulled_images().lock().unwrap();
    !pulled.insert(image_name.to_string())
}

/// Check if an image has been pulled in the current session.
pub fn is_image_pulled(image_name: &str) -> bool {
    let pulled = pulled_images().lock().unwrap();
    pulled.contains(image_name)
}
