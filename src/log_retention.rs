use std::fs::File;
use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const TIMESTAMP_FIELDS: &[&str] = &[
    "logged_at",
    "submission_completed_at",
    "skipped_at",
    "confirmation_received_at",
    "trade_detected_at",
    "websocket_event_received_at",
    "market_event_at",
    "source_trade_at",
    "confirmed_at",
    "updated_at",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionOutcome {
    pub kept_lines: usize,
    pub removed_lines: usize,
}

pub async fn enforce_jsonl_retention(path: &Path, retention: Duration) -> Result<RetentionOutcome> {
    let source = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RetentionOutcome::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("opening {}", path.display()));
        }
    };

    let retention_window = chrono::Duration::from_std(retention)
        .map_err(|error| anyhow!("invalid log retention duration: {error}"))?;
    let cutoff = Utc::now() - retention_window;
    let temp_path = temp_path_for(path)?;
    let mut destination = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .await
        .with_context(|| format!("opening {}", temp_path.display()))?;
    let mut outcome = RetentionOutcome::default();
    let mut lines = BufReader::new(source).lines();

    while let Some(line) = lines
        .next_line()
        .await
        .with_context(|| format!("reading {}", path.display()))?
    {
        if line.trim().is_empty() {
            continue;
        }

        if should_keep_line(&line, cutoff) {
            destination
                .write_all(line.as_bytes())
                .await
                .with_context(|| format!("writing {}", temp_path.display()))?;
            destination
                .write_all(b"\n")
                .await
                .with_context(|| format!("writing newline to {}", temp_path.display()))?;
            outcome.kept_lines += 1;
        } else {
            outcome.removed_lines += 1;
        }
    }

    destination
        .flush()
        .await
        .with_context(|| format!("flushing {}", temp_path.display()))?;
    drop(destination);

    if outcome.kept_lines == 0 {
        remove_if_exists_async(path).await?;
        remove_if_exists_async(&temp_path).await?;
        return Ok(outcome);
    }

    remove_if_exists_async(path).await?;
    tokio::fs::rename(&temp_path, path)
        .await
        .with_context(|| format!("replacing {}", path.display()))?;

    Ok(outcome)
}

pub fn enforce_jsonl_retention_blocking(
    path: &Path,
    retention: Duration,
) -> Result<RetentionOutcome> {
    let source = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RetentionOutcome::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("opening {}", path.display()));
        }
    };

    let retention_window = chrono::Duration::from_std(retention)
        .map_err(|error| anyhow!("invalid log retention duration: {error}"))?;
    let cutoff = Utc::now() - retention_window;
    let temp_path = temp_path_for(path)?;
    let mut destination = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .with_context(|| format!("opening {}", temp_path.display()))?;
    let mut outcome = RetentionOutcome::default();
    let lines = StdBufReader::new(source).lines();

    for line in lines {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }

        if should_keep_line(&line, cutoff) {
            destination
                .write_all(line.as_bytes())
                .with_context(|| format!("writing {}", temp_path.display()))?;
            destination
                .write_all(b"\n")
                .with_context(|| format!("writing newline to {}", temp_path.display()))?;
            outcome.kept_lines += 1;
        } else {
            outcome.removed_lines += 1;
        }
    }

    destination
        .flush()
        .with_context(|| format!("flushing {}", temp_path.display()))?;
    drop(destination);

    if outcome.kept_lines == 0 {
        remove_if_exists_blocking(path)?;
        remove_if_exists_blocking(&temp_path)?;
        return Ok(outcome);
    }

    remove_if_exists_blocking(path)?;
    std::fs::rename(&temp_path, path).with_context(|| format!("replacing {}", path.display()))?;

    Ok(outcome)
}

fn should_keep_line(line: &str, cutoff: DateTime<Utc>) -> bool {
    extract_timestamp(line)
        .map(|timestamp| timestamp >= cutoff)
        .unwrap_or(true)
}

fn extract_timestamp(line: &str) -> Option<DateTime<Utc>> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    for field in TIMESTAMP_FIELDS {
        let Some(raw) = value.get(*field).and_then(Value::as_str) else {
            continue;
        };
        if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
            return Some(parsed.with_timezone(&Utc));
        }
    }
    None
}

fn temp_path_for(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("path {} has no file name", path.display()))?;
    let mut temp_name = file_name.to_os_string();
    temp_name.push(".tmp");
    Ok(path.with_file_name(temp_name))
}

async fn remove_if_exists_async(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

fn remove_if_exists_blocking(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn keeps_recent_timestamped_lines() {
        let recent = format!(
            r#"{{"logged_at":"{}","event_type":"recent"}}"#,
            Utc::now().to_rfc3339()
        );

        assert!(should_keep_line(
            &recent,
            Utc::now() - chrono::Duration::hours(6)
        ));
    }

    #[test]
    fn drops_old_timestamped_lines() {
        let stale = format!(
            r#"{{"logged_at":"{}","event_type":"stale"}}"#,
            (Utc::now() - chrono::Duration::hours(7)).to_rfc3339()
        );

        assert!(!should_keep_line(
            &stale,
            Utc::now() - chrono::Duration::hours(6)
        ));
    }

    #[test]
    fn keeps_lines_without_known_timestamp_fields() {
        assert!(should_keep_line(
            r#"{"event_type":"unknown"}"#,
            Utc::now() - chrono::Duration::hours(6)
        ));
    }

    #[tokio::test]
    async fn deletes_file_when_all_entries_are_expired() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("retention.jsonl");
        let stale = format!(
            r#"{{"logged_at":"{}","event_type":"stale"}}"#,
            (Utc::now() - chrono::Duration::hours(8)).to_rfc3339()
        );
        tokio::fs::write(&path, format!("{stale}\n"))
            .await
            .expect("write stale line");

        let outcome = enforce_jsonl_retention(&path, Duration::from_secs(60 * 60))
            .await
            .expect("enforce retention");

        assert_eq!(outcome.kept_lines, 0);
        assert_eq!(outcome.removed_lines, 1);
        assert!(!path.exists());
    }

    #[test]
    fn deletes_file_when_all_entries_are_expired_blocking() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("retention-blocking.jsonl");
        let stale = format!(
            r#"{{"logged_at":"{}","event_type":"stale"}}"#,
            (Utc::now() - chrono::Duration::hours(8)).to_rfc3339()
        );
        std::fs::write(&path, format!("{stale}\n")).expect("write stale line");

        let outcome = enforce_jsonl_retention_blocking(&path, Duration::from_secs(60 * 60))
            .expect("enforce blocking retention");

        assert_eq!(outcome.kept_lines, 0);
        assert_eq!(outcome.removed_lines, 1);
        assert!(!path.exists());
    }
}
