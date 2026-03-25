use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::OnceLock;

/// Setup/prepopulation configuration for workloads that need data seeding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupConfig {
    /// Number of events to prepopulate during setup phase
    pub prepopulate_events: u64,
    /// Number of streams to distribute prepopulated events across
    #[serde(default)]
    pub prepopulate_streams: Option<u64>,
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
