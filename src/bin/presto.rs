use clap::Parser;
use presto::{Dataset, describe, render_tui};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about = "Presto accelerates preprocessing with precision.", long_about = None)]
struct Args {
    #[arg(short = 'p', long = "path", required = true)]
    path: PathBuf,
}

fn main() -> Result<(), presto::PrestoError> {
    let args = Args::parse();
    let dataset = Dataset::from_csv(args.path.to_str().ok_or_else(|| {
        presto::PrestoError::InvalidNumeric("Invalid path provided".to_string())
    })?)?;
    let description = describe(&dataset)?;
    render_tui(&dataset, &description)?;
    Ok(())
}
