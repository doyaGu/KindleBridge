//! Small safe boundary around Linux native AIO.
//!
//! Buffers passed to [`Context::write_all`] remain borrowed until every
//! submitted IOCB has completed or the context has been destroyed. Keeping the
//! syscall ABI here lets the rest of KindleBridge continue to forbid unsafe
//! code.

#![cfg(target_os = "linux")]

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::ptr;

const IOCB_CMD_PWRITE: u16 = 1;

type AioContext = libc::c_ulong;

#[repr(C)]
#[derive(Clone, Copy)]
struct IoEvent {
    data: u64,
    object: u64,
    result: i64,
    result2: i64,
}

/// A failed batch write, including whether the kernel accepted any IOCB.
#[derive(Debug)]
pub struct WriteError {
    source: io::Error,
    submitted: bool,
}

impl WriteError {
    #[must_use]
    pub const fn submitted(&self) -> bool {
        self.submitted
    }

    #[must_use]
    pub fn into_io_error(self) -> io::Error {
        self.source
    }
}

/// One Linux native-AIO context with no outstanding operations between calls.
#[derive(Debug)]
pub struct Context {
    raw: Option<AioContext>,
}

impl Context {
    pub fn new(entries: usize) -> io::Result<Self> {
        let entries = u32::try_from(entries)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "AIO depth exceeds u32"))?;
        if entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AIO depth must be positive",
            ));
        }
        let mut raw: AioContext = 0;
        // SAFETY: `raw` points to initialized writable storage of the kernel's
        // `aio_context_t` width, and remains valid for the syscall.
        let result = unsafe { libc::syscall(libc::SYS_io_setup, entries, &mut raw) };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { raw: Some(raw) })
    }

    /// Submit ordered writes in bounded batches and wait for every completion.
    ///
    /// The caller may retry synchronously only when [`WriteError::submitted`]
    /// is false; otherwise the byte boundary is deliberately unknown.
    pub fn write_all(
        &mut self,
        fd: BorrowedFd<'_>,
        buffer: &[u8],
        chunk_size: usize,
        depth: usize,
    ) -> Result<(), WriteError> {
        if chunk_size == 0 || depth == 0 {
            return Err(WriteError {
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AIO chunk size and depth must be positive",
                ),
                submitted: false,
            });
        }
        let Some(raw) = self.raw else {
            return Err(WriteError {
                source: io::Error::new(io::ErrorKind::NotConnected, "AIO context is closed"),
                submitted: false,
            });
        };

        let mut submitted_any = false;
        for batch in buffer.chunks(chunk_size.saturating_mul(depth)) {
            let chunks = batch.chunks(chunk_size).collect::<Vec<_>>();
            let mut controls = Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                // SAFETY: the all-zero representation is the kernel-prescribed
                // initialization for `iocb`; public fields are filled below.
                let mut control = unsafe { std::mem::zeroed::<libc::iocb>() };
                control.aio_lio_opcode = IOCB_CMD_PWRITE;
                control.aio_fildes = fd.as_raw_fd() as u32;
                control.aio_buf = chunk.as_ptr() as u64;
                control.aio_nbytes = chunk.len() as u64;
                control.aio_offset = 0;
                controls.push(control);
            }
            let mut pointers = controls
                .iter_mut()
                .map(|control| control as *mut libc::iocb)
                .collect::<Vec<_>>();
            let mut next = 0_usize;
            while next < pointers.len() {
                // SAFETY: the context is live; every IOCB and borrowed payload
                // remains pinned in its vector until all completions return.
                let submitted = unsafe {
                    libc::syscall(
                        libc::SYS_io_submit,
                        raw,
                        pointers.len() - next,
                        pointers[next..].as_mut_ptr(),
                    )
                };
                if submitted == -1 {
                    let error = WriteError {
                        source: io::Error::last_os_error(),
                        submitted: submitted_any,
                    };
                    if submitted_any {
                        self.destroy();
                    }
                    return Err(error);
                }
                let submitted = usize::try_from(submitted).unwrap_or(0);
                if submitted == 0 {
                    let error = WriteError {
                        source: io::Error::new(
                            io::ErrorKind::WriteZero,
                            "io_submit accepted no requests",
                        ),
                        submitted: submitted_any,
                    };
                    if submitted_any {
                        self.destroy();
                    }
                    return Err(error);
                }
                submitted_any = true;
                let mut events = vec![
                    IoEvent {
                        data: 0,
                        object: 0,
                        result: 0,
                        result2: 0,
                    };
                    submitted
                ];
                let received = loop {
                    // SAFETY: `events` has capacity for `submitted` initialized
                    // entries and the null timeout requests an unbounded wait.
                    let result = unsafe {
                        libc::syscall(
                            libc::SYS_io_getevents,
                            raw,
                            submitted,
                            submitted,
                            events.as_mut_ptr(),
                            ptr::null_mut::<libc::timespec>(),
                        )
                    };
                    if result == -1
                        && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted
                    {
                        continue;
                    }
                    break result;
                };
                if received != submitted as libc::c_long {
                    let source = if received == -1 {
                        io::Error::last_os_error()
                    } else {
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete AIO completion set",
                        )
                    };
                    self.destroy();
                    return Err(WriteError {
                        source,
                        submitted: true,
                    });
                }
                let expected = chunks[next..next + submitted]
                    .iter()
                    .map(|chunk| chunk.len() as i64)
                    .sum::<i64>();
                let completed = events.iter().map(|event| event.result).sum::<i64>();
                if events
                    .iter()
                    .any(|event| event.result < 0 || event.result2 != 0)
                    || completed != expected
                {
                    self.destroy();
                    return Err(WriteError {
                        source: io::Error::new(
                            io::ErrorKind::WriteZero,
                            "AIO write completed short or with an endpoint error",
                        ),
                        submitted: true,
                    });
                }
                next += submitted;
            }
        }
        Ok(())
    }

    fn destroy(&mut self) {
        if let Some(raw) = self.raw.take() {
            // SAFETY: `raw` was returned by `io_setup` and is destroyed once.
            // The syscall waits for requests it cannot cancel before returning.
            let _ = unsafe { libc::syscall(libc::SYS_io_destroy, raw) };
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.destroy();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::fd::AsFd;

    #[test]
    fn rejects_unbounded_shapes_before_submission() {
        let mut context = Context::new(1).unwrap();
        let file = std::fs::File::open("/dev/null").unwrap();
        let error = context.write_all(file.as_fd(), b"data", 0, 1).unwrap_err();
        assert!(!error.submitted());
    }

    #[test]
    fn writes_one_buffer_through_the_kernel_aio_abi() {
        let path = std::env::temp_dir().join(format!(
            "kindlebridge-linux-aio-test-{}",
            std::process::id()
        ));
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut context = Context::new(1).unwrap();
        context
            .write_all(file.as_fd(), b"kindlebridge-aio", 16 * 1024, 1)
            .unwrap();
        let mut written = Vec::new();
        file.read_to_end(&mut written).unwrap();
        assert_eq!(written, b"kindlebridge-aio");
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
