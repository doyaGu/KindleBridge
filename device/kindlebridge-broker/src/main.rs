use std::io::{self, Read};

use kindlebridge_broker::{BrokerPolicy, BrokerRequest};
use serde::Serialize;

#[derive(Serialize)]
struct Response<'a> {
    allowed: bool,
    error: Option<&'a str>,
}

fn main() {
    let mut input = String::new();
    if let Err(error) = io::stdin().read_to_string(&mut input) {
        emit(false, Some(&format!("failed to read request: {error}")));
        std::process::exit(2);
    }

    let request: BrokerRequest = match serde_json::from_str(&input) {
        Ok(request) => request,
        Err(error) => {
            emit(false, Some(&format!("invalid broker request: {error}")));
            std::process::exit(2);
        }
    };

    match BrokerPolicy::default().authorize(&request) {
        Ok(()) => emit(true, None),
        Err(error) => {
            emit(false, Some(&error.to_string()));
            std::process::exit(1);
        }
    }
}

fn emit(allowed: bool, error: Option<&str>) {
    let response = Response { allowed, error };
    println!(
        "{}",
        serde_json::to_string(&response).expect("response serialization cannot fail")
    );
}
