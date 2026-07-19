use std::{env, ffi::OsString, fs, path::PathBuf, process::ExitCode};

use kindlebridge_functionfs::{run, SessionOutcome};

const DEFAULT_FUNCTIONFS_DIR: &str = "/dev/usb-ffs/kbp";

#[derive(Debug, Eq, PartialEq)]
struct Arguments {
    directory: PathBuf,
    completion_file: Option<PathBuf>,
}

fn main() -> ExitCode {
    let arguments = match parse_arguments(env::args_os().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!("kindlebridge-ffs-probe: {error}");
            eprintln!(
                "usage: kindlebridge-ffs-probe [FUNCTIONFS_DIRECTORY] [--completion-file PATH]"
            );
            return ExitCode::from(2);
        }
    };

    let (exit_code, completion) = match run(&arguments.directory) {
        Ok(SessionOutcome::Completed) => {
            eprintln!("KindleBridge FunctionFS probe completed");
            (ExitCode::SUCCESS, "completed\n")
        }
        Ok(SessionOutcome::Disconnected) => {
            eprintln!("KindleBridge FunctionFS peer disconnected");
            (ExitCode::SUCCESS, "disconnected\n")
        }
        Err(error) => {
            eprintln!("KindleBridge FunctionFS probe failed: {error}");
            (ExitCode::FAILURE, "failed\n")
        }
    };
    if let Some(path) = arguments.completion_file {
        if let Err(error) = fs::write(&path, completion) {
            eprintln!(
                "KindleBridge FunctionFS probe could not write completion file {}: {error}",
                path.display()
            );
        }
    }
    exit_code
}

fn parse_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Arguments, String> {
    let mut arguments = arguments.into_iter();
    let mut directory = None;
    let mut completion_file = None;
    while let Some(argument) = arguments.next() {
        if argument == "--completion-file" {
            let value = arguments
                .next()
                .ok_or_else(|| "--completion-file requires a path".to_owned())?;
            if completion_file.replace(PathBuf::from(value)).is_some() {
                return Err("--completion-file was specified more than once".to_owned());
            }
        } else if argument.to_string_lossy().starts_with('-') {
            return Err(format!("unknown option {argument:?}"));
        } else if directory.replace(PathBuf::from(argument)).is_some() {
            return Err("more than one FunctionFS directory was specified".to_owned());
        }
    }
    Ok(Arguments {
        directory: directory.unwrap_or_else(|| PathBuf::from(DEFAULT_FUNCTIONFS_DIR)),
        completion_file,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_file_is_optional_and_order_independent() {
        assert_eq!(
            parse_arguments(Vec::<OsString>::new()).unwrap(),
            Arguments {
                directory: PathBuf::from(DEFAULT_FUNCTIONFS_DIR),
                completion_file: None,
            }
        );
        assert_eq!(
            parse_arguments([
                OsString::from("--completion-file"),
                OsString::from("/tmp/done"),
                OsString::from("/tmp/ffs"),
            ])
            .unwrap(),
            Arguments {
                directory: PathBuf::from("/tmp/ffs"),
                completion_file: Some(PathBuf::from("/tmp/done")),
            }
        );
    }
}
