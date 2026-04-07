use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use tracing::info;

use crate::log_retention::{RetentionOutcome, enforce_jsonl_retention_blocking};

pub struct RotatingLogger {
    base_path: String,
    file_index: u32,
    line_count: u32,
    max_lines: u32,
    writer: Option<BufWriter<File>>,
    created_at: Instant,
    max_duration: Duration,
    clear_on_duration: bool,
    writer_active: bool,
}

impl RotatingLogger {
    pub fn new(base_path: String, max_lines: u32, max_duration: Duration) -> Result<Self> {
        let max_lines = max_lines.max(1);
        let (file_index, line_count) = Self::restore_state(&base_path, max_lines)?;
        let file = Self::open_file(&base_path, file_index)?;
        let created_at = restored_created_at(&Self::path_for(&base_path, file_index), max_duration);

        Ok(Self {
            base_path,
            file_index,
            line_count,
            max_lines,
            writer: Some(BufWriter::new(file)),
            created_at,
            max_duration,
            clear_on_duration: false,
            writer_active: true,
        })
    }

    pub fn with_time_clearing(mut self, enabled: bool) -> Self {
        self.clear_on_duration = enabled;
        self
    }

    pub fn write_line(&mut self, line: &str) -> Result<()> {
        if self.writer_is_closed() {
            self.reopen_file()?;
        }

        let path = self.current_path();
        writeln!(
            self.writer
                .as_mut()
                .expect("writer must be reopened before writing"),
            "{line}"
        )
        .with_context(|| format!("writing {}", path.display()))?;
        self.writer
            .as_mut()
            .expect("writer must be active before flushing")
            .flush()
            .with_context(|| format!("flushing {}", path.display()))?;
        self.line_count = self.line_count.saturating_add(1);

        if self.line_count >= self.max_lines {
            self.rotate("lines")?;
        } else if self.created_at.elapsed() >= self.max_duration {
            if self.clear_on_duration {
                self.clear_logs()?;
            } else {
                self.rotate("time")?;
            }
        }

        Ok(())
    }

    pub fn current_path(&self) -> PathBuf {
        Self::path_for(&self.base_path, self.file_index)
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer
                .flush()
                .with_context(|| format!("flushing {}", self.current_path().display()))?;
        }
        Ok(())
    }

    pub fn cull_old_entries(&mut self, retention: Duration) -> Result<RetentionOutcome> {
        self.flush()?;
        let writer_was_active = self.writer_active;
        let previous_writer = self.writer.take();
        drop(previous_writer);
        self.writer_active = false;

        let result = self.cull_all_files(retention);
        self.line_count = count_lines(&self.current_path())?;

        if writer_was_active {
            self.reopen_file()?;
        }
        result
    }

    fn cull_all_files(&self, retention: Duration) -> Result<RetentionOutcome> {
        let mut outcome = RetentionOutcome::default();
        for index in 0..=self.file_index {
            let file_outcome = enforce_jsonl_retention_blocking(
                &Self::path_for(&self.base_path, index),
                retention,
            )?;
            outcome.kept_lines += file_outcome.kept_lines;
            outcome.removed_lines += file_outcome.removed_lines;
        }
        Ok(outcome)
    }

    fn rotate(&mut self, trigger: &'static str) -> Result<()> {
        let previous_path = self.current_path();
        self.flush()?;
        self.file_index = self.file_index.saturating_add(1);
        self.line_count = 0;
        self.created_at = Instant::now();

        let new_file = Self::open_file(&self.base_path, self.file_index)?;
        self.writer = Some(BufWriter::new(new_file));
        self.writer_active = true;
        info!(
            log_rotation_triggered_by = trigger,
            previous_path = %previous_path.display(),
            current_path = %self.current_path().display(),
            "rotating log file"
        );
        Ok(())
    }

    fn clear_logs(&mut self) -> Result<()> {
        let current_path = self.current_path();
        self.flush()?;
        let previous_writer = self.writer.take();
        drop(previous_writer);
        remove_if_exists(&current_path)?;
        self.line_count = 0;
        self.created_at = Instant::now();
        self.writer_active = false;
        info!(
            log_rotation_triggered_by = "time",
            cleared_path = %current_path.display(),
            "clearing log file after max duration"
        );
        Ok(())
    }

    fn reopen_file(&mut self) -> Result<()> {
        let file = Self::open_file(&self.base_path, self.file_index)?;
        self.writer = Some(BufWriter::new(file));
        self.writer_active = true;
        Ok(())
    }

    fn writer_is_closed(&self) -> bool {
        !self.writer_active
    }

    fn restore_state(base_path: &str, max_lines: u32) -> Result<(u32, u32)> {
        let mut next_index = 0_u32;
        while Self::path_for(base_path, next_index).exists() {
            match next_index.checked_add(1) {
                Some(value) => next_index = value,
                None => break,
            }
        }

        if next_index == 0 {
            return Ok((0, 0));
        }

        let last_index = next_index - 1;
        let line_count = count_lines(&Self::path_for(base_path, last_index))?;
        if line_count >= max_lines {
            Ok((next_index, 0))
        } else {
            Ok((last_index, line_count))
        }
    }

    fn open_file(base: &str, index: u32) -> Result<File> {
        let path = Self::path_for(base, index);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))
    }

    fn path_for(base: &str, index: u32) -> PathBuf {
        PathBuf::from(format!("{base}_{index}.jsonl"))
    }
}

fn count_lines(path: &Path) -> Result<u32> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error).with_context(|| format!("opening {}", path.display())),
    };

    let mut count = 0_u32;
    for line in BufReader::new(file).lines() {
        line.with_context(|| format!("reading {}", path.display()))?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotates_after_max_lines() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_path = temp_dir
            .path()
            .join("logs")
            .join("latency-events")
            .display()
            .to_string();
        let mut logger =
            RotatingLogger::new(base_path.clone(), 2, Duration::from_secs(6 * 60 * 60))
                .expect("logger");

        logger.write_line(r#"{"line":1}"#).expect("line one");
        logger.write_line(r#"{"line":2}"#).expect("line two");
        logger.write_line(r#"{"line":3}"#).expect("line three");
        logger.flush().expect("flush");

        let first = std::fs::read_to_string(format!("{base_path}_0.jsonl")).expect("first file");
        let second = std::fs::read_to_string(format!("{base_path}_1.jsonl")).expect("second file");

        assert_eq!(first.lines().count(), 2);
        assert_eq!(second.lines().count(), 1);
    }

    #[test]
    fn resumes_last_non_full_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_path = temp_dir.path().join("logs").join("raw-activity");
        let first_path = format!("{}_0.jsonl", base_path.display());
        if let Some(parent) = Path::new(&first_path).parent() {
            std::fs::create_dir_all(parent).expect("create dir");
        }
        std::fs::write(&first_path, "{\"line\":1}\n").expect("seed file");

        let logger = RotatingLogger::new(
            base_path.display().to_string(),
            3,
            Duration::from_secs(6 * 60 * 60),
        )
        .expect("logger");

        assert_eq!(logger.file_index, 0);
        assert_eq!(logger.line_count, 1);
    }

    #[test]
    fn rotates_after_max_duration() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_path = temp_dir.path().join("logs").join("raw-activity");
        let mut logger = RotatingLogger::new(
            base_path.display().to_string(),
            30_000,
            Duration::from_millis(20),
        )
        .expect("logger");

        std::thread::sleep(Duration::from_millis(30));
        logger.write_line(r#"{"line":1}"#).expect("line one");

        assert!(Path::new(&format!("{}_1.jsonl", base_path.display())).exists());
    }

    #[test]
    fn clears_current_file_when_time_clearing_is_enabled() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_path = temp_dir.path().join("logs").join("attribution-events");
        let mut logger = RotatingLogger::new(
            base_path.display().to_string(),
            30_000,
            Duration::from_millis(20),
        )
        .expect("logger")
        .with_time_clearing(true);

        logger.write_line(r#"{"line":1}"#).expect("line one");
        std::thread::sleep(Duration::from_millis(30));
        logger.write_line(r#"{"line":2}"#).expect("line two");
        assert!(!Path::new(&format!("{}_0.jsonl", base_path.display())).exists());

        logger.write_line(r#"{"line":3}"#).expect("line three");
        logger.flush().expect("flush");

        let current = std::fs::read_to_string(format!("{}_0.jsonl", base_path.display()))
            .expect("current file");
        assert_eq!(current.lines().count(), 1);
        assert!(current.contains(r#""line":3"#));
    }
}

fn restored_created_at(path: &Path, max_duration: Duration) -> Instant {
    let Some(age) = file_age(path) else {
        return Instant::now();
    };
    let bounded_age = age.min(max_duration);
    Instant::now()
        .checked_sub(bounded_age)
        .unwrap_or_else(Instant::now)
}

fn file_age(path: &Path) -> Option<Duration> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}
