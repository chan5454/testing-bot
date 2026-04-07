use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

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
