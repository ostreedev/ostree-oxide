use std::{
    io::{self, ErrorKind, Read},
    path::{Path, PathBuf},
};

use hex::FromHex;
use openat::{Dir, DirIter};

use crate::{CommitId, Oid};

pub(crate) fn read_ref(repo: &Dir, name: &Path) -> io::Result<CommitId> {
    let mut file = repo.open_file(name)?;
    let mut buf = [0u8; 64];
    file.read_exact(&mut buf)?;
    Ok(CommitId(Oid::from_hex(buf).map_err(|_| {
        io::Error::new(ErrorKind::InvalidData, "Ref contains non-hex content")
    })?))
}

pub(crate) fn check_ref_prefix(path: &Path) -> Result<(), io::Error> {
    if path != Path::new("refs") && !path.starts_with("refs/") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "prefix must start with \"refs\"",
        ));
    }
    for c in path.components() {
        match c {
            std::path::Component::Normal(_) => (),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "prefix may not contain \"..\"",
                ))
            }
        }
    }
    Ok(())
}

pub struct ListRefs<'a> {
    dir: &'a Dir,
    dir_stack: Vec<DirIter>,
    name_stack: PathBuf,
}

impl<'a> ListRefs<'a> {
    pub fn new(repo: &'a Dir, path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            dir: repo,
            dir_stack: vec![repo.list_dir(&path)?],
            name_stack: path,
        })
    }
}

impl<'a> Iterator for ListRefs<'a> {
    type Item = io::Result<PathBuf>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let it = self.dir_stack.last_mut()?;
            match it.next() {
                Some(x) => {
                    let ent = match x {
                        Ok(x) => x,
                        Err(err) => return Some(Err(err)),
                    };
                    let ty = match ent.simple_type() {
                        Some(t) => t,
                        None => todo!(
                            "Handle this, apparently some filesystems don't provide type info"
                        ),
                    };
                    match ty {
                        openat::SimpleType::Dir => {
                            self.name_stack.push(ent.file_name());
                            self.dir_stack
                                .push(match self.dir.list_dir(&self.name_stack) {
                                    Ok(x) => x,
                                    Err(err) => return Some(Err(err)),
                                });
                        }
                        openat::SimpleType::File => {
                            return Some(Ok(self.name_stack.join(ent.file_name())));
                        }
                        _ => (),
                    }
                }
                None => {
                    self.dir_stack.pop();
                    self.name_stack.pop();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Repo;

    #[test]
    fn test_refs() {
        let repo = Repo::open("testdata/fancy-refs").unwrap();
        let cid = repo.read_ref(Path::new("refs/heads/myref")).unwrap();
        assert_eq!(
            cid,
            CommitId(
                Oid::from_hex("0000000000000000000000000000000000000000000000000000000000000003")
                    .unwrap()
            )
        );

        let r: Result<Vec<_>, _> = repo
            .list_refs("refs/heads/ref-prefix".into())
            .unwrap()
            .collect();
        let mut r = r.unwrap();
        r.sort();
        assert_eq!(
            r,
            [
                Path::new("refs/heads/ref-prefix/another-ref"),
                Path::new("refs/heads/ref-prefix/subprefix/more-refs")
            ]
        );
    }
}
