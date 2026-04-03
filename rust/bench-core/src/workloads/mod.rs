// Workload architecture
pub mod consistency;
pub mod durability;
pub mod factory;
pub mod operational;
pub mod performance;

// Re-export main types
pub use factory::{Workload};
pub use performance::{PerformanceWorkload, PerformanceConfig};
