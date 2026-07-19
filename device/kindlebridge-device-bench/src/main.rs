//! Destructive-but-self-cleaning hardware benchmark for an explicitly selected
//! Kindle writable partition. This program never writes the root filesystem.

use std::env;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Serialize;

const MIB: u64 = 1024 * 1024;
const BLOCK_SIZE: usize = 1024 * 1024;
const DEFAULT_SIZE_MIB: u64 = 128;
const MAX_SIZE_MIB: u64 = 4096;

#[derive(Debug)]
struct Arguments {
    output: PathBuf,
    size_bytes: u64,
}

#[derive(Debug, Serialize)]
struct Report {
    output_class: &'static str,
    size_bytes: u64,
    block_size: usize,
    write_mib_per_second: f64,
    read_mib_per_second: f64,
    blake3_mib_per_second: f64,
    memory_copy_mib_per_second: f64,
    write_elapsed_millis: u64,
    read_elapsed_millis: u64,
    blake3_elapsed_millis: u64,
    memory_copy_elapsed_millis: u64,
    digest_hex: String,
}

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("kindlebridge-device-bench: {error}");
        std::process::exit(1);
    }
}

fn run(raw_arguments: Vec<String>) -> Result<(), Box<dyn Error>> {
    let arguments = parse_arguments(&raw_arguments)?;
    let output_class = validate_output_path(&arguments.output)?;
    let block = test_block();

    let mut expected = blake3::Hasher::new();
    for_each_chunk(arguments.size_bytes, |length| {
        expected.update(&block[..length]);
        Ok(())
    })?;
    let expected_digest = expected.finalize();

    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&arguments.output)?;
    let mut guard = TemporaryFile::new(arguments.output.clone(), file);

    let write_started = Instant::now();
    for_each_chunk(arguments.size_bytes, |length| {
        guard.file.write_all(&block[..length])?;
        Ok(())
    })?;
    guard.file.sync_all()?;
    let write_elapsed = write_started.elapsed();

    drop(guard.file.try_clone()?);
    let mut reader = File::open(&arguments.output)?;
    let mut read_buffer = vec![0_u8; BLOCK_SIZE];
    let mut actual = blake3::Hasher::new();
    let read_started = Instant::now();
    let mut remaining = arguments.size_bytes;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(BLOCK_SIZE as u64))?;
        reader.read_exact(&mut read_buffer[..wanted])?;
        actual.update(&read_buffer[..wanted]);
        remaining -= u64::try_from(wanted)?;
    }
    let read_elapsed = read_started.elapsed();
    let actual_digest = actual.finalize();
    if actual_digest != expected_digest {
        return Err("readback digest does not match written data".into());
    }

    let hash_started = Instant::now();
    let mut hasher = blake3::Hasher::new();
    for_each_chunk(arguments.size_bytes, |length| {
        hasher.update(black_box(&block[..length]));
        Ok(())
    })?;
    let benchmark_digest = hasher.finalize();
    black_box(benchmark_digest);
    let hash_elapsed = hash_started.elapsed();

    let mut copy_target = vec![0_u8; BLOCK_SIZE];
    let copy_started = Instant::now();
    for_each_chunk(arguments.size_bytes, |length| {
        copy_target[..length].copy_from_slice(black_box(&block[..length]));
        black_box(&copy_target[..length]);
        Ok(())
    })?;
    let copy_elapsed = copy_started.elapsed();

    let report = Report {
        output_class,
        size_bytes: arguments.size_bytes,
        block_size: BLOCK_SIZE,
        write_mib_per_second: rate(arguments.size_bytes, write_elapsed),
        read_mib_per_second: rate(arguments.size_bytes, read_elapsed),
        blake3_mib_per_second: rate(arguments.size_bytes, hash_elapsed),
        memory_copy_mib_per_second: rate(arguments.size_bytes, copy_elapsed),
        write_elapsed_millis: millis(write_elapsed),
        read_elapsed_millis: millis(read_elapsed),
        blake3_elapsed_millis: millis(hash_elapsed),
        memory_copy_elapsed_millis: millis(copy_elapsed),
        digest_hex: expected_digest.to_hex().to_string(),
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    guard.remove()?;
    Ok(())
}

fn test_block() -> Vec<u8> {
    (0..BLOCK_SIZE)
        .map(|index| u8::try_from(index % 251).expect("modulo result fits in u8"))
        .collect()
}

fn for_each_chunk(
    total: u64,
    mut operation: impl FnMut(usize) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut remaining = total;
    while remaining > 0 {
        let length = usize::try_from(remaining.min(BLOCK_SIZE as u64))?;
        operation(length)?;
        remaining -= u64::try_from(length)?;
    }
    Ok(())
}

fn rate(bytes: u64, elapsed: Duration) -> f64 {
    (bytes as f64 / MIB as f64) / elapsed.as_secs_f64().max(f64::EPSILON)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, Box<dyn Error>> {
    let output = option_value(arguments, "--output")
        .ok_or("usage: kindlebridge-device-bench --output PATH [--size-mib N]")?;
    let size_mib = option_value(arguments, "--size-mib")
        .map(str::parse::<u64>)
        .transpose()?
        .unwrap_or(DEFAULT_SIZE_MIB);
    if size_mib == 0 || size_mib > MAX_SIZE_MIB {
        return Err(format!("--size-mib must be in 1..={MAX_SIZE_MIB}").into());
    }
    Ok(Arguments {
        output: PathBuf::from(output),
        size_bytes: size_mib.checked_mul(MIB).ok_or("benchmark size overflow")?,
    })
}

fn option_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

fn validate_output_path(path: &Path) -> Result<&'static str, Box<dyn Error>> {
    if path.file_name().and_then(|name| name.to_str()) != Some("kindlebridge-device-bench.tmp") {
        return Err("output filename must be kindlebridge-device-bench.tmp".into());
    }
    let parent = path.parent().ok_or("output path has no parent")?;
    let canonical_parent = fs::canonicalize(parent)?;
    for (allowed, class) in [
        (Path::new("/var/local"), "var-local"),
        (Path::new("/mnt/us"), "user-storage"),
    ] {
        let Ok(canonical_allowed) = fs::canonicalize(allowed) else {
            continue;
        };
        if canonical_parent.starts_with(canonical_allowed) {
            return Ok(class);
        }
    }
    Err("output must be below /var/local or /mnt/us".into())
}

struct TemporaryFile {
    path: PathBuf,
    file: File,
    removed: bool,
}

impl TemporaryFile {
    fn new(path: PathBuf, file: File) -> Self {
        Self {
            path,
            file,
            removed: false,
        }
    }

    fn remove(&mut self) -> Result<(), std::io::Error> {
        fs::remove_file(&self.path)?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if !self.removed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argument_size_is_bounded() {
        let arguments = vec![
            "--output".to_owned(),
            "/mnt/us/kindlebridge-device-bench.tmp".to_owned(),
            "--size-mib".to_owned(),
            "4097".to_owned(),
        ];
        assert!(parse_arguments(&arguments).is_err());
    }

    #[test]
    fn fixed_filename_prevents_arbitrary_deletion() {
        assert!(validate_output_path(Path::new("/mnt/us/book.azw3")).is_err());
    }
}
