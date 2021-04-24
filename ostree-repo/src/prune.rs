use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};

use crate::{CommitId, ContentId, DirMetaId, DirTreeId, OwnedCommit};

use super::{ObjId, Repo};
use std::sync::atomic::AtomicBool;
use std::{collections::HashMap, sync::atomic::Ordering};
use std::{io, path::Path, time::Instant};

#[derive(Debug)]
struct PruneState<'a> {
    repo: &'a Repo,
    commits: HashMap<CommitId, AtomicBool>,
    dirtrees: HashMap<DirTreeId, AtomicBool>,
    dirmetas: HashMap<DirMetaId, AtomicBool>,
    files: HashMap<ContentId, AtomicBool>,
}

impl<'a> PruneState<'a> {
    fn new(repo: &'a Repo) -> Self {
        Self {
            repo,
            commits: Default::default(),
            dirtrees: Default::default(),
            dirmetas: Default::default(),
            files: Default::default(),
        }
    }
    fn mark_ref(&self, refname: &Path, limit: usize) -> io::Result<()> {
        let cid = self.repo.read_ref(refname)?;
        self.mark_commit(cid, limit).map_err(|err| {
            eprintln!("Failed processing ref {:?}", refname);
            err
        })
    }
    fn mark_commit<'b>(&self, cid: CommitId, limit: usize) -> io::Result<()> {
        self.repo
            .iter_commit_history(cid)
            .take(limit)
            .par_bridge()
            .try_for_each(|commit| match commit {
                Ok(c) => self._mark_commit(c),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            })
    }
    fn _mark_commit<'b>(&self, (cid, commit): (CommitId, OwnedCommit)) -> io::Result<()> {
        self.commits[&cid].fetch_or(true, Ordering::Relaxed);
        let c = commit.as_commit();
        self.mark_dirmeta(&c.root_dirmeta());
        self.mark_dirtree(&c.root_dirtree())?;
        Ok(())
    }
    fn mark_dirmeta(&self, oid: &DirMetaId) {
        self.dirmetas[oid].fetch_or(true, Ordering::Relaxed);
    }
    fn mark_file(&self, oid: &ContentId) {
        self.files[oid].fetch_or(true, Ordering::Relaxed);
    }
    fn mark_dirtree(&self, oid: &DirTreeId) -> io::Result<()> {
        let already_seen = self.dirtrees[oid].fetch_or(true, Ordering::SeqCst);
        if !already_seen {
            let owned = self.repo.read_dirtree(oid)?;
            let dt = owned.as_dirtree();
            // TODO: Make this more efficient using rayon::IndexedParallelIterator
            dt.iter_dirs().par_bridge().try_for_each(|d| {
                self.mark_dirmeta(d.dirmeta_id);
                self.mark_dirtree(d.dirtree_id)
            })?;
            // These will be fast enough that it's not worth doing it in parallel:
            for f in dt.iter_files() {
                self.mark_file(f.oid)
            }
        }
        Ok(())
    }
}

pub fn prune(repo: &Repo, depth: Option<usize>) -> io::Result<()> {
    let mut state = PruneState::new(repo);

    let depth = match depth {
        // depth = 0 means just those commits directly referenced by a ref, so limit is 1:
        Some(d) => d + 1,
        None => usize::MAX,
    };

    println!("Enumerating objects");
    let start = Instant::now();
    // TODO: We could add additional parallelism here by enumerating from the different
    // subdirectories of objects/ in parallel.  See rayon ParallelExtend
    for obj in repo.iter_objects() {
        match obj? {
            ObjId::Commit(x) => state.commits.insert(x, AtomicBool::new(false)),
            ObjId::DirTree(x) => state.dirtrees.insert(x, AtomicBool::new(false)),
            ObjId::DirMeta(x) => state.dirmetas.insert(x, AtomicBool::new(false)),
            ObjId::Content(x) => state.files.insert(x, AtomicBool::new(false)),
        };
    }
    let end = Instant::now();
    println!(
        "Found {} commits, {} dirtrees, {} dirmetas and {} content objects in {:?}",
        state.commits.len(),
        state.dirtrees.len(),
        state.dirmetas.len(),
        state.files.len(),
        end - start
    );

    println!("Marking");
    let start = Instant::now();
    repo.list_refs("refs".into())?
        .par_bridge()
        .try_for_each(|r| state.mark_ref(&r?, depth))?;
    let end = Instant::now();
    println!("Done in {:?}", end - start);

    println!("Sweeping");
    let (mut tn, mut tt) = (0, 0);
    let commits_for_deletion = state
        .commits
        .par_iter()
        .filter(|x| !x.1.load(Ordering::Relaxed));
    let n = commits_for_deletion.count();
    let t = state.commits.len();
    tn += n;
    tt += t;
    println!("Would delete {}/{} commits", n, t);

    let dirtrees_for_deletion = state
        .dirtrees
        .par_iter()
        .filter(|x| !x.1.load(Ordering::Relaxed));
    let n = dirtrees_for_deletion.count();
    let t = state.dirtrees.len();
    tn += n;
    tt += t;
    println!("Would delete {}/{} dirtrees", n, t);

    let dirmetas_for_deletion = state
        .dirmetas
        .par_iter()
        .filter(|x| !x.1.load(Ordering::Relaxed));
    let n = dirmetas_for_deletion.count();
    let t = state.dirmetas.len();
    tn += n;
    tt += t;
    println!("Would delete {}/{} dirmetas", n, t);

    let files_for_deletion = state
        .files
        .par_iter()
        .filter(|x| !x.1.load(Ordering::Relaxed));
    let n = files_for_deletion.count();
    let t = state.files.len();
    tn += n;
    tt += t;
    println!("Would delete {}/{} files", n, t);

    println!("Would delete {}/{} objects total", tn, tt);
    Ok(())
}
