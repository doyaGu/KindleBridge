use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::fs_safe::entry_exists;
use crate::watchdog::{ChildRunner, ChildStatus, Clock, DisableFlag, SpawnRequest};
use crate::{Error, ErrorKind, Result, CHILD_PID_FILE, PRODUCTION_DISABLE_FLAG};

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }

    fn sleep_ms(&mut self, duration_ms: u64) {
        std::thread::sleep(Duration::from_millis(duration_ms));
    }
}

#[derive(Clone, Debug)]
pub struct FilesystemDisableFlag {
    path: PathBuf,
}

impl Default for FilesystemDisableFlag {
    fn default() -> Self {
        Self {
            path: PathBuf::from(PRODUCTION_DISABLE_FLAG),
        }
    }
}

impl FilesystemDisableFlag {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DisableFlag for FilesystemDisableFlag {
    fn is_disabled(&self) -> Result<bool> {
        if !entry_exists(&self.path)? {
            return Ok(false);
        }
        if !std::fs::metadata(&self.path)?.is_file() {
            return Err(Error::new(
                ErrorKind::UnsafePath,
                "disable flag is not a regular file",
            ));
        }
        Ok(true)
    }
}

#[derive(Debug, Default)]
pub struct SystemChildRunner {
    next_id: u64,
    arguments: Vec<OsString>,
    children: BTreeMap<u64, ManagedChild>,
}

#[derive(Debug)]
struct ManagedChild {
    process: Child,
    pid_file: PathBuf,
}

impl SystemChildRunner {
    #[must_use]
    pub fn with_arguments(arguments: Vec<OsString>) -> Self {
        Self {
            next_id: 0,
            arguments,
            children: BTreeMap::new(),
        }
    }
}

impl Drop for SystemChildRunner {
    fn drop(&mut self) {
        for (_, mut child) in std::mem::take(&mut self.children) {
            let _ = child.process.kill();
            let _ = child.process.wait();
            let _ = std::fs::remove_file(child.pid_file);
        }
    }
}

impl ChildRunner for SystemChildRunner {
    fn spawn(&mut self, request: &SpawnRequest) -> Result<u64> {
        let root = request
            .heartbeat
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| Error::new(ErrorKind::Child, "heartbeat path has no root"))?;
        let mut command = Command::new(&request.executable);
        command
            .current_dir(
                request
                    .executable
                    .parent()
                    .ok_or_else(|| Error::new(ErrorKind::Child, "executable has no parent"))?,
            )
            .env("KINDLEBRIDGE_ROOT", root)
            .env("KINDLEBRIDGE_SLOT", request.slot.as_str())
            .env("KINDLEBRIDGE_HEARTBEAT", &request.heartbeat)
            .env("KINDLEBRIDGE_INSTANCE", &request.instance);
        command.args(&self.arguments);
        let child = command
            .spawn()
            .map_err(|error| Error::new(ErrorKind::Child, format!("spawn failed: {error}")))?;
        let pid_file = root.join(CHILD_PID_FILE);
        std::fs::write(&pid_file, format!("{}\n", child.id()))?;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| Error::new(ErrorKind::Child, "launcher child identifier exhausted"))?;
        let id = self.next_id;
        self.children.insert(
            id,
            ManagedChild {
                process: child,
                pid_file,
            },
        );
        Ok(id)
    }

    fn poll(&mut self, child_id: u64) -> Result<ChildStatus> {
        let status = self
            .children
            .get_mut(&child_id)
            .ok_or_else(|| Error::new(ErrorKind::Child, "unknown child identifier"))?
            .process
            .try_wait()
            .map_err(|error| Error::new(ErrorKind::Child, format!("child poll failed: {error}")))?;
        let Some(status) = status else {
            return Ok(ChildStatus::Running);
        };
        if let Some(child) = self.children.remove(&child_id) {
            let _ = std::fs::remove_file(child.pid_file);
        }
        Ok(ChildStatus::Exited {
            code: status.code(),
        })
    }

    fn terminate(&mut self, child_id: u64) -> Result<()> {
        let mut child = self
            .children
            .remove(&child_id)
            .ok_or_else(|| Error::new(ErrorKind::Child, "unknown child identifier"))?;
        child
            .process
            .kill()
            .map_err(|error| Error::new(ErrorKind::Child, format!("child kill failed: {error}")))?;
        child
            .process
            .wait()
            .map_err(|error| Error::new(ErrorKind::Child, format!("child wait failed: {error}")))?;
        let _ = std::fs::remove_file(child.pid_file);
        Ok(())
    }
}
