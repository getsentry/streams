use std::error::Error;

use rust_streams::run::{run, Args};

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    run(args)
}
