use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::session::Session;

/// Scans a results directory for all sessions
pub struct SessionScanner {
    sessions_root: PathBuf,
}

impl SessionScanner {
    /// Create a new scanner for the given sessions root directory
    pub fn new(sessions_root: impl AsRef<Path>) -> Self {
        Self {
            sessions_root: sessions_root.as_ref().to_path_buf(),
        }
    }

    /// Find all session directories
    pub fn find_sessions(&self) -> Result<Vec<PathBuf>> {
        let mut session_dirs = Vec::new();

        if !self.sessions_root.exists() {
            return Ok(session_dirs);
        }

        // Look for directories containing session.json
        for entry in WalkDir::new(&self.sessions_root)
            .max_depth(2) // sessions/{timestamp}/
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                let session_json = entry.path().join("session.json");
                if session_json.exists() {
                    session_dirs.push(entry.path().to_path_buf());
                }
            }
        }

        // Sort by directory name (timestamp) in descending order (newest first)
        session_dirs.sort_by(|a, b| b.cmp(a));

        Ok(session_dirs)
    }

    /// Load all sessions
    pub fn load_all(&self) -> Result<Vec<Session>> {
        let paths = self.find_sessions()?;
        let mut sessions = Vec::new();

        for path in paths {
            match Session::load(&path) {
                Ok(session) => sessions.push(session),
                Err(e) => {
                    eprintln!("Warning: Failed to load session at {}: {}", path.display(), e);
                }
            }
        }

        Ok(sessions)
    }

    /// Load a specific session by ID
    pub fn load_by_id(&self, session_id: &str) -> Result<Session> {
        let session_path = self.sessions_root.join(session_id);
        Session::load(&session_path)
            .with_context(|| format!("Failed to load session {}", session_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scanner_creation() {
        let scanner = SessionScanner::new("results/raw/sessions");
        assert_eq!(
            scanner.sessions_root,
            PathBuf::from("results/raw/sessions")
        );
    }
}
