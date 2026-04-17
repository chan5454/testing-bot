use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::log_retention::{RetentionOutcome, enforce_jsonl_retention};

#[derive(Clone)]
pub struct RollingJsonlLogger {
    path: PathBuf,
    max_lines: usize,
    state: Arc<Mutex<RollingJsonlState>>,
}

#[derive(Default)]
struct RollingJsonlState {
    initialized: bool,
    line_count: usize,
}

impl RollingJsonlLogger {
    pub fn new(path: PathBuf, max_lines: usize) -> Self {
        Self {
            path,
            max_lines: max_lines.max(1),
            state: Arc::new(Mutex::new(RollingJsonlState::default())),
        }
    }

    pub async fn append<T: Serialize>(&self, record: &T) -> Result<()> {
        let mut state = self.state.lock().await;
        self.initialize_if_needed(&mut state).await?;
        if state.line_count >= self.max_lines {
            self.rotate_current_file().await?;
            state.line_count = 0;
        }

        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .with_context(|| format!("opening {}", self.path.display()))?;
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        file.write_all(&line)
            .await
            .with_context(|| format!("writing {}", self.path.display()))?;
        state.line_count += 1;
        Ok(())
    }

    async fn initialize_if_needed(&self, state: &mut RollingJsonlState) -> Result<()> {
        if state.initialized {
            return Ok(());
        }

        state.line_count = count_lines(&self.path).await?;
        state.initialized = true;
        Ok(())
    }

    async fn rotate_current_file(&self) -> Result<()> {
        if !path_exists(&self.path).await? {
            return Ok(());
        }

        let rotated_path = rotated_path(&self.path, Utc::now().timestamp_millis());
        tokio::fs::rename(&self.path, &rotated_path)
            .await
            .with_context(|| {
                format!(
                    "rotating {} to {}",
                    self.path.display(),
                    rotated_path.display()
                )
            })?;
        Ok(())
    }

    pub async fn cull_old_entries(&self, retention: Duration) -> Result<RetentionOutcome> {
        let mut state = self.state.lock().await;
        self.initialize_if_needed(&mut state).await?;

        let mut outcome = RetentionOutcome::default();
        for path in self.managed_paths().await? {
            let file_outcome = enforce_jsonl_retention(&path, retention).await?;
            outcome.kept_lines += file_outcome.kept_lines;
            outcome.removed_lines += file_outcome.removed_lines;
        }

        state.line_count = count_lines(&self.path).await?;
        Ok(outcome)
    }

    async fn managed_paths(&self) -> Result<Vec<PathBuf>> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let mut directory = tokio::fs::read_dir(parent)
            .await
            .with_context(|| format!("reading {}", parent.display()))?;
        let mut paths = Vec::new();

        while let Some(entry) = directory.next_entry().await? {
            let path = entry.path();
            if self.manages_path(&path) {
                paths.push(path);
            }
        }

        if !paths.iter().any(|candidate| candidate == &self.path) && path_exists(&self.path).await? {
            paths.push(self.path.clone());
        }

        Ok(paths)
    }

    fn manages_path(&self, candidate: &Path) -> bool {
        if candidate == self.path {
            return true;
        }

        let Some(file_name) = candidate.file_name().and_then(|value| value.to_str()) else {
            return false;
        };
        let stem = self
            .path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("log");
        let rotated_prefix = format!("{stem}-");
        let extension = self.path.extension().and_then(|value| value.to_str());

        match extension {
            Some(extension) if !extension.is_empty() => {
                file_name.starts_with(&rotated_prefix)
                    && file_name.ends_with(&format!(".{extension}"))
            }
            _ => file_name.starts_with(&rotated_prefix),
        }
    }
}

async fn count_lines(path: &Path) -> Result<usize> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => Ok(contents.lines().count()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

async fn path_exists(path: &Path) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("checking {}", path.display())),
    }
}

fn rotated_path(path: &Path, suffix: i64) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("log");
    let extension = path.extension().and_then(|value| value.to_str());
    match extension {
        Some(extension) if !extension.is_empty() => {
            parent.join(format!("{stem}-{suffix}.{extension}"))
        }
        _ => parent.join(format!("{stem}-{suffix}")),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};
    use serde::Serialize;
    use tempfile::tempdir;

    use super::*;

    #[derive(Serialize)]
    struct TestEntry {
        logged_at: chrono::DateTime<Utc>,
        value: &'static str,
    }

    #[tokio::test]
    async fn culls_rotated_files_and_deletes_empty_shells() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wallet-activity.jsonl");
        let logger = RollingJsonlLogger::new(path.clone(), 2);
        let stale_time = Utc::now() - ChronoDuration::hours(3);

        logger
            .append(&TestEntry {
                logged_at: stale_time,
                value: "first",
            })
            .await
            .expect("append first");
        logger
            .append(&TestEntry {
                logged_at: stale_time,
                value: "second",
            })
            .await
            .expect("append second");
        logger
            .append(&TestEntry {
                logged_at: stale_time,
                value: "third",
            })
            .await
            .expect("append third");

        let outcome = logger
            .cull_old_entries(Duration::from_secs(60 * 60))
            .await
            .expect("cull stale entries");

        assert_eq!(outcome.kept_lines, 0);
        assert_eq!(outcome.removed_lines, 3);

        let names = std::fs::read_dir(temp_dir.path())
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(names.is_empty(), "expected no managed files, found {names:?}");
        assert!(!path.exists());
    }
}
