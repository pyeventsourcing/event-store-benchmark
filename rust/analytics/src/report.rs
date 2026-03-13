use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::aggregation::{compute_session_detail, compute_session_index};
use crate::scanner::SessionScanner;
use crate::templates::{generate_index_html, generate_session_html};

/// Report generator
pub struct ReportGenerator {
    sessions_path: PathBuf,
    output_path: PathBuf,
}

impl ReportGenerator {
    /// Create a new report generator
    pub fn new(sessions_path: impl AsRef<Path>, output_path: impl AsRef<Path>) -> Self {
        Self {
            sessions_path: sessions_path.as_ref().to_path_buf(),
            output_path: output_path.as_ref().to_path_buf(),
        }
    }

    /// Generate the complete report
    pub fn generate(&self) -> Result<()> {
        println!("Scanning sessions from {}...", self.sessions_path.display());

        // Load all sessions
        let scanner = SessionScanner::new(&self.sessions_path);
        let sessions = scanner
            .load_all()
            .context("Failed to load sessions")?;

        if sessions.is_empty() {
            anyhow::bail!("No sessions found in {}", self.sessions_path.display());
        }

        println!("Found {} session(s)", sessions.len());

        // Create output directory
        fs::create_dir_all(&self.output_path)
            .with_context(|| format!("Failed to create output directory: {}", self.output_path.display()))?;

        // Generate index
        println!("Generating session index...");
        let index = compute_session_index(&sessions);
        let index_html = generate_index_html(&index)?;

        let index_path = self.output_path.join("index.html");
        fs::write(&index_path, index_html)
            .with_context(|| format!("Failed to write {}", index_path.display()))?;

        println!("  → {}", index_path.display());

        // Generate individual session pages
        let sessions_dir = self.output_path.join("sessions");
        fs::create_dir_all(&sessions_dir)
            .with_context(|| format!("Failed to create sessions directory: {}", sessions_dir.display()))?;

        for session in &sessions {
            println!("Generating report for session: {}", session.metadata.session_id);

            let detail = compute_session_detail(session);
            let session_html = generate_session_html(&detail)?;

            let session_dir = sessions_dir.join(&session.metadata.session_id);
            fs::create_dir_all(&session_dir)
                .with_context(|| format!("Failed to create session directory: {}", session_dir.display()))?;

            let session_html_path = session_dir.join("index.html");
            fs::write(&session_html_path, session_html)
                .with_context(|| format!("Failed to write {}", session_html_path.display()))?;

            println!("  → {}", session_html_path.display());
        }

        println!("\n✓ Report generated successfully!");
        println!("  Open: {}", self.output_path.join("index.html").display());

        Ok(())
    }
}
