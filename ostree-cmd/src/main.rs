use std::io;

use io::ErrorKind;
use ostree_repo::{prune, Repo};

use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "prune")]
    Prune(Prune),
}

#[derive(Debug, StructOpt)]
pub struct Prune {
    /// Path to OSTree repository
    #[structopt(long, parse(from_os_str))]
    repo: PathBuf,

    /// Only traverse DEPTH parents for each commit (default: infinite)
    #[structopt(long)]
    pub depth: Option<usize>,

    /// Only compute reachability via refs
    #[structopt(long)]
    refs_only: bool,

    /// Only display unreachable objects; don't delete
    #[structopt(long)]
    no_prune: bool,
}

fn main() -> io::Result<()> {
    let opt = Opt::from_args();

    match opt.command {
        Command::Prune(p) => {
            if !p.refs_only {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "--refs-only is manditory for now",
                ));
            }
            if !p.no_prune {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "--no-prune is manditory for now",
                ));
            }
            let repo = Repo::open(p.repo)?;
            prune(&repo, p.depth)
        }
    }
}
