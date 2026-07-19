use std::io::{self, BufReader, BufWriter};
use std::process::ExitCode;

fn main() -> ExitCode {
    if !std::env::args()
        .skip(1)
        .any(|argument| argument == "--stdio")
    {
        eprintln!("kindlebridge-fake-device: pass --stdio");
        return ExitCode::FAILURE;
    }
    let stdin = io::stdin();
    let stdout = io::stdout();
    match kindlebridge_server::serve(
        &mut BufReader::new(stdin.lock()),
        &mut BufWriter::new(stdout.lock()),
        &kindlebridge_fake_device::provider(),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kindlebridge-fake-device: {error}");
            ExitCode::FAILURE
        }
    }
}
