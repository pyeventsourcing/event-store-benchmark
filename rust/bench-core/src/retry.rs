use anyhow::{bail, Result};
use std::future::Future;
use std::time::Duration;

pub async fn wait_for_ready<F, Fut, T>(name: &str, mut f: F, max_duration: Duration) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut current_wait = Duration::from_millis(10);
    let start = std::time::Instant::now();
    let factor = 1.1; // 10% backoff

    loop {
        match f().await {
            Ok(res) => return Ok(res),
            Err(_) => {
                if start.elapsed() >= max_duration {
                    bail!("{} did not become ready within {:?}", name, max_duration);
                }
                tokio::time::sleep(current_wait).await;
                current_wait = Duration::from_secs_f64(current_wait.as_secs_f64() * factor);
                // Cap current_wait to the remaining time or 1s (to avoid very long gaps if 10% is large)
                if current_wait > Duration::from_secs(1) {
                    current_wait = Duration::from_secs(1);
                }
            }
        }
    }
}
