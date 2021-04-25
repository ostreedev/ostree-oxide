use std::io;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {}

fn main() -> io::Result<()> {
    let _opt = Opt::from_args();
    Ok(())
}
