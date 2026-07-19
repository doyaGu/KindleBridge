use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);

pub fn start_from_environment() -> Result<(), String> {
    let heartbeat = std::env::var_os("KINDLEBRIDGE_HEARTBEAT");
    let instance = std::env::var("KINDLEBRIDGE_INSTANCE").ok();
    match (heartbeat, instance) {
        (None, None) => Ok(()),
        (Some(path), Some(instance)) => {
            let path = PathBuf::from(path);
            validate(&path, &instance)?;
            write_heartbeat(&path, &instance)?;
            std::thread::Builder::new()
                .name("kindlebridge-heartbeat".to_owned())
                .spawn(move || {
                    let mut failure_reported = false;
                    loop {
                        std::thread::sleep(HEARTBEAT_INTERVAL);
                        match write_heartbeat(&path, &instance) {
                            Ok(()) => {
                                if failure_reported {
                                    eprintln!("kindlebridged: heartbeat write recovered");
                                    failure_reported = false;
                                }
                            }
                            Err(error) => {
                                if !failure_reported {
                                    eprintln!("kindlebridged: heartbeat write delayed: {error}");
                                    failure_reported = true;
                                }
                            }
                        }
                    }
                })
                .map_err(|error| format!("could not start heartbeat: {error}"))?;
            Ok(())
        }
        _ => Err("launcher heartbeat environment is incomplete".to_owned()),
    }
}

fn validate(path: &Path, instance: &str) -> Result<(), String> {
    if !path.is_absolute()
        || path.file_name().and_then(|name| name.to_str()) != Some("heartbeat")
        || instance.is_empty()
        || instance.len() > 128
        || !instance
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("launcher heartbeat configuration is invalid".to_owned());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "heartbeat path has no parent".to_owned())?;
    if !parent.is_dir() {
        return Err("heartbeat parent is not a directory".to_owned());
    }
    Ok(())
}

fn write_heartbeat(path: &Path, instance: &str) -> Result<(), String> {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let timestamp_ms = u64::try_from(timestamp_ms).unwrap_or(u64::MAX);
    let bytes = kindlebridge_launcher::encode_heartbeat(instance, timestamp_ms);
    let temporary = path.with_file_name(format!(".heartbeat.{}", std::process::id()));
    fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_write_uses_the_launcher_instance_format() {
        let root = std::env::temp_dir().join(format!(
            "kindlebridge-heartbeat-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        let path = root.join("heartbeat");
        validate(&path, "abc-123").unwrap();
        write_heartbeat(&path, "abc-123").unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.starts_with("KINDLEBRIDGE_HEARTBEAT_V1\ninstance=abc-123\n"));
        fs::remove_dir_all(root).unwrap();
    }
}
