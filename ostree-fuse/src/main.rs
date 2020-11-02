use fuse::{mount, FileAttr, FileType, Filesystem};
use hex::FromHex;
use libc::{c_int, EIO, EISDIR, ENOENT, ENOSYS, ENOTDIR};
use ostree_repo::{CommitId, ContentId, DirMetaId, DirTreeId, Oid, Repo};
use std::os::unix::ffi::OsStrExt;
use std::{
    collections::HashMap,
    convert::TryInto,
    env,
    error::Error,
    ffi::OsStr,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};
use time::Timespec;

const UNIX_EPOCH: Timespec = Timespec { sec: 0, nsec: 0 };
const FOREVER: Timespec = Timespec {
    sec: i64::MAX,
    nsec: 0,
};
const TRACING: bool = false;

#[derive(Copy, Clone)]
struct Errno(c_int);
impl From<Errno> for i32 {
    fn from(x: Errno) -> Self {
        x.0
    }
}
impl From<Errno> for std::io::Error {
    fn from(x: Errno) -> Self {
        std::io::Error::from_raw_os_error(x.0)
    }
}

struct StaticDir {
    attr: FileAttr,
    entries: &'static [(InodeNo, i64, FileType, &'static str)],
}

const STATIC_DIRS: &[StaticDir] = &[
    // /
    StaticDir {
        attr: dir_attr(InodeNo::ROOT),
        entries: &[
            (InodeNo::ROOT, 1, FileType::Directory, "."),
            (InodeNo::ROOT, 2, FileType::Directory, ".."),
            (InodeNo::BY_COMMIT, 3, FileType::Directory, "by-commit"),
            (InodeNo::REFS, 4, FileType::Directory, "refs"),
        ],
    },
    // /by-commit
    StaticDir {
        attr: dir_attr(InodeNo::BY_COMMIT),
        entries: &[
            (InodeNo::BY_COMMIT, 1, FileType::Directory, "."),
            (InodeNo::ROOT, 2, FileType::Directory, ".."),
        ],
    },
    // /refs
    StaticDir {
        attr: dir_attr(InodeNo::REFS),
        entries: &[
            (InodeNo::REFS, 1, FileType::Directory, "."),
            (InodeNo::ROOT, 2, FileType::Directory, ".."),
        ],
    },
];

fn static_dir(inode: InodeNo) -> Option<&'static StaticDir> {
    if inode.0 > 0 {
        STATIC_DIRS.get((inode.0 - 1) as usize)
    } else {
        None
    }
}

impl INode for &'static StaticDir {
    fn readdir(
        &mut self,
        _fs: &OstreeFs,
        offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), i32> {
        for entry in self.entries.iter().skip(offset as usize) {
            reply.add((entry.0).0, entry.1, entry.2, entry.3);
        }
        Ok(())
    }

    fn getattr(&mut self, _fs: &OstreeFs) -> Result<FileAttr, i32> {
        Ok(self.attr)
    }

    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, i32> {
        for attr in self.entries.iter() {
            if name == attr.3 {
                return Ok(dir_attr(attr.0));
            }
        }
        if self.attr.ino == InodeNo::BY_COMMIT.as_u64() {
            if let Ok(oid) = Oid::from_hex(name.as_bytes()) {
                return Commit { oid: CommitId(oid) }.getattr(fs);
            } else {
                eprintln!("Invalid commit oid: {:?}", name);
            }
        }
        Err(ENOENT)
    }

    fn read(
        &mut self,
        _fs: &OstreeFs,
        _offset: i64,
        _size: u32,
        _reply: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        Err(Errno(EISDIR).into())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct InodeNo(u64);

impl InodeNo {
    const ROOT: InodeNo = InodeNo(1);
    const BY_COMMIT: InodeNo = InodeNo(2);
    const REFS: InodeNo = InodeNo(3);

    const DIRTREE_MASK: u64 = 0x0000_0fff_ffff_ffff;
    const DIRMETA_MASK: u64 = 0xffff_f000_0000_0000;

    fn from_u64(inode: u64) -> Self {
        Self(inode)
    }
    fn from_commit_id(oid: &CommitId) -> Self {
        Self(u64::from_be_bytes((oid.0).0[..8].try_into().unwrap()))
    }
    fn from_file_id(oid: &ContentId) -> Self {
        Self(u64::from_be_bytes((oid.0).0[..8].try_into().unwrap()))
    }
    fn from_dir_oid(dirmeta: &DirMetaId, dirtree: &DirTreeId) -> Self {
        let high_bytes: [u8; 8] = (dirmeta.0).0[..8].try_into().unwrap();
        let dirmeta_high = u64::from_be_bytes(high_bytes);

        let high_bytes: [u8; 8] = (dirtree.0).0[..8].try_into().unwrap();
        let dirtree_high = u64::from_be_bytes(high_bytes);

        InodeNo((dirmeta_high & Self::DIRMETA_MASK) | (dirtree_high & Self::DIRTREE_MASK))
    }
    fn to_dir_sid(self) -> (u64, u64) {
        (self.0 & Self::DIRMETA_MASK, self.0 & Self::DIRTREE_MASK)
    }
    const fn as_u64(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod test {
    use crate::InodeNo;
    use ostree_repo::{DirMetaId, DirTreeId, Oid};
    use rand::Rng;

    fn random_oid() -> Oid {
        Oid(rand::thread_rng().gen::<[u8; 32]>())
    }

    #[test]
    fn test_inodeno_dir_ids() {
        let dm = DirMetaId(random_oid());
        let dt = DirTreeId(random_oid());
        let ino = InodeNo::from_dir_oid(&dm, &dt);
        let (_dmsid, _dtsid) = ino.to_dir_sid();
        todo!("What goes here?")
    }
}

const fn dir_attr(ino: InodeNo) -> FileAttr {
    FileAttr {
        ino: ino.as_u64(),
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

enum FileRef {
    Static(&'static StaticDir),
    Commit(Commit),
    Tree(Tree),
    File(Content),
}

impl FileRef {
    fn as_inode_mut(&mut self) -> Result<&mut dyn INode, Errno> {
        Ok(match self {
            FileRef::Static(x) => x,
            FileRef::Commit(x) => x,
            FileRef::Tree(x) => x,
            FileRef::File(x) => x,
        })
    }
}

trait INode {
    fn readdir(
        &mut self,
        fs: &OstreeFs,
        offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), i32>;
    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, i32>;
    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, i32>;
    fn read(
        &mut self,
        fs: &OstreeFs,
        offset: i64,
        size: u32,
        reply: &mut Vec<u8>,
    ) -> Result<(), std::io::Error>;
}

struct Commit {
    oid: CommitId,
}
impl Commit {
    fn to_tree(&self, repo: &Repo) -> Result<Tree, Errno> {
        let buf = match repo.read_commit(&self.oid) {
            Ok(x) => x,
            Err(err) => {
                eprintln!("Error loading commit {:?}: {:?}", self.oid, err);
                return Err(Errno(ENOENT));
            }
        };
        let commit = buf.as_commit();
        Ok(Tree {
            meta: *commit.dirmeta,
            tree: *commit.dirtree,
        })
    }
}
impl INode for Commit {
    fn readdir(
        &mut self,
        fs: &OstreeFs,
        offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), i32> {
        self.to_tree(&fs.repo)?.readdir(fs, offset, reply)
    }

    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, i32> {
        self.to_tree(&fs.repo)?.getattr(fs)
    }

    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, i32> {
        self.to_tree(&fs.repo)?.lookup(fs, name)
    }

    fn read(
        &mut self,
        fs: &OstreeFs,
        offset: i64,
        size: u32,
        reply: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        self.to_tree(&fs.repo)?.read(fs, offset, size, reply)
    }
}

struct Tree {
    meta: DirMetaId,
    tree: DirTreeId,
}

impl INode for Tree {
    fn readdir(
        &mut self,
        fs: &OstreeFs,
        mut offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), i32> {
        let dt_data = fs.repo.read_dirtree(&self.tree).map_err(|_| EIO)?;
        let dt = dt_data.as_dirtree();
        let mut next_offset: i64 = offset as i64 + 1;
        let dirs = dt.iter_dirs();
        if offset >= dirs.len() {
            offset -= dirs.len();
        } else {
            for dir in dirs.skip(offset) {
                let sub_ino = InodeNo::from_dir_oid(dir.dirmeta_id, dir.dirtree_id);
                offset = offset.saturating_sub(1);
                if reply.add(sub_ino.as_u64(), next_offset, FileType::Directory, dir.name) {
                    return Ok(());
                }
                next_offset += 1;
            }
        }
        for file in dt.iter_files().skip(offset) {
            let sub_ino = InodeNo::from_file_id(file.oid);
            if reply.add(
                sub_ino.as_u64(),
                next_offset,
                FileType::RegularFile,
                file.name,
            ) {
                return Ok(());
            }
            next_offset += 1;
        }
        Ok(())
    }

    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, i32> {
        let meta = fs.repo.read_dirmeta(&self.meta).map_err(|_| EIO)?;
        Ok(FileAttr {
            ino: InodeNo::from_dir_oid(&self.meta, &self.tree).as_u64(),
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: (meta.mode & 0o7777) as u16,
            nlink: 1,
            uid: meta.uid,
            gid: meta.gid,
            rdev: 0,
            flags: 0,
        })
    }

    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, i32> {
        let dt_data = fs.repo.read_dirtree(&self.tree).map_err(|_| ENOENT)?;
        let dt = dt_data.as_dirtree();
        for de in dt.iter_dirs() {
            if de.name == name {
                return Tree {
                    meta: *de.dirmeta_id,
                    tree: *de.dirtree_id,
                }
                .getattr(fs);
            }
        }
        for fe in dt.iter_files() {
            if fe.name == name {
                return Content { oid: *fe.oid }.getattr(fs);
            }
        }
        Err(ENOENT)
    }
    fn read(
        &mut self,
        _fs: &OstreeFs,
        _offset: i64,
        _size: u32,
        _reply: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        Err(std::io::Error::from_raw_os_error(EISDIR))
    }
}

struct Content {
    oid: ContentId,
}
impl INode for Content {
    fn readdir(
        &mut self,
        _fs: &OstreeFs,
        _offset: usize,
        _reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), i32> {
        Err(ENOTDIR)
    }

    fn lookup(&mut self, _fs: &OstreeFs, _name: &OsStr) -> Result<FileAttr, i32> {
        Err(ENOTDIR)
    }

    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, i32> {
        let meta = fs.repo.read_meta(&self.oid).map_err(|_| EIO)?;
        let kind = match meta.mode & libc::S_IFMT {
            libc::S_IFBLK => FileType::BlockDevice,
            libc::S_IFCHR => FileType::CharDevice,
            libc::S_IFDIR => FileType::Directory,
            libc::S_IFIFO => FileType::NamedPipe,
            libc::S_IFREG => FileType::RegularFile,
            libc::S_IFSOCK => FileType::Socket,
            libc::S_IFLNK => FileType::Symlink,
            _ => {
                eprintln!("Invalid metadata on file");
                return Err(EIO);
            }
        };
        Ok(FileAttr {
            ino: InodeNo::from_file_id(&self.oid).as_u64(),
            size: meta.size,
            blocks: (meta.size + 511) / 512,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind,
            perm: (meta.mode & 0o7777) as u16,
            nlink: 1,
            uid: meta.uid,
            gid: meta.gid,
            rdev: 0,
            flags: 0,
        })
    }

    fn read(
        &mut self,
        fs: &OstreeFs,
        offset: i64,
        size: u32,
        reply: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let mut f = fs.repo.open_object(&self.oid.into())?;
        f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
        reply.reserve(size as usize);
        f.take(size as u64).read_to_end(reply)?;
        Ok(())
    }
}

struct OstreeFs {
    repo: Repo,
    commits: HashMap<InodeNo, CommitId>,
    files: HashMap<InodeNo, ContentId>,
    // TODO: Store this as u32:
    dirmetas: HashMap<u64, DirMetaId>,
    dirtrees: HashMap<u64, DirTreeId>,
}

impl OstreeFs {
    fn new_from_repo(repo: Repo) -> Result<OstreeFs, Box<dyn Error>> {
        let mut commits = HashMap::new();
        let mut files = HashMap::new();
        let mut dirmetas = HashMap::new();
        let mut dirtrees = HashMap::new();

        repo.for_each_object(|obj_id| match obj_id {
            ostree_repo::ObjId::Commit(cid) => {
                commits.insert(InodeNo::from_commit_id(cid), *cid);
            }
            ostree_repo::ObjId::DirTree(id) => {
                let ino = InodeNo::from_dir_oid(&DirMetaId(Oid::ZERO), id);
                let (_, sid) = ino.to_dir_sid();
                dirtrees.insert(sid, *id);
            }
            ostree_repo::ObjId::DirMeta(id) => {
                let ino = InodeNo::from_dir_oid(id, &DirTreeId(Oid::ZERO));
                let (sid, _) = ino.to_dir_sid();
                dirmetas.insert(sid, *id);
            }
            ostree_repo::ObjId::Content(id) => {
                files.insert(InodeNo::from_file_id(id), *id);
            }
        })?;
        Ok(OstreeFs {
            repo,
            commits,
            files,
            dirtrees,
            dirmetas,
        })
    }
    fn get_by_inode(&self, ino: u64) -> Result<FileRef, Errno> {
        let inode = InodeNo::from_u64(ino);
        if let Some(sd) = static_dir(inode) {
            return Ok(FileRef::Static(sd));
        }
        if let Some(oid) = self.files.get(&inode) {
            return Ok(FileRef::File(Content { oid: *oid }));
        }
        let (dm, dt) = inode.to_dir_sid();
        if let (Some(meta), Some(tree)) = (self.dirmetas.get(&dm), self.dirtrees.get(&dt)) {
            return Ok(FileRef::Tree(Tree {
                meta: *meta,
                tree: *tree,
            }));
        }
        if let Some(commit_id) = self.commits.get(&inode) {
            return Ok(FileRef::Commit(Commit { oid: *commit_id }));
        }
        Err(Errno(ENOENT))
    }
}

impl Filesystem for OstreeFs {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), c_int> {
        if TRACING {
            eprintln!("init()");
        }
        Ok(())
    }
    fn destroy(&mut self, _req: &fuse::Request) {}
    fn forget(&mut self, _req: &fuse::Request, _ino: u64, _nlookup: u64) {}
    fn getattr(&mut self, _req: &fuse::Request, ino: u64, reply: fuse::ReplyAttr) {
        if TRACING {
            eprintln!("getattr(ino: {:?})", ino);
        }
        match (|| self.get_by_inode(ino)?.as_inode_mut()?.getattr(self))() {
            Ok(x) => reply.attr(&FOREVER, &x),
            Err(errno) => reply.error(errno),
        }
    }
    fn lookup(&mut self, _req: &fuse::Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        match (|| {
            self.get_by_inode(parent)?
                .as_inode_mut()?
                .lookup(self, name)
        })() {
            Ok(attr) => reply.entry(&FOREVER, &attr, 0),
            Err(errno) => reply.error(errno),
        };
        if TRACING {
            eprintln!("lookup(parent: {:?}, name: {:?})", parent, name);
        }
    }
    fn readlink(&mut self, _req: &fuse::Request, ino: u64, reply: fuse::ReplyData) {
        if TRACING {
            eprintln!("readlink(ino: {:?})", ino);
        }
        let ino = InodeNo::from_u64(ino);
        if let Some(oid) = self.files.get(&ino) {
            match self.repo.read_content(oid) {
                Ok(obj) => reply.data(&*obj),
                Err(_) => reply.error(ENOENT),
            };
        } else {
            reply.error(ENOENT)
        }
    }
    fn open(&mut self, _req: &fuse::Request, ino: u64, flags: u32, reply: fuse::ReplyOpen) {
        if TRACING {
            eprintln!("open(ino: {:?}, flags: {:?})", ino, flags);
        }
        reply.opened(0, 0);
    }
    fn read(
        &mut self,
        _req: &fuse::Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: fuse::ReplyData,
    ) {
        if TRACING {
            eprintln!(
                "read(ino: {:?}, offset: {:?}, size: {:?})",
                ino, offset, size
            );
        }
        let mut out = Vec::new();
        match (|| {
            self.get_by_inode(ino)?
                .as_inode_mut()?
                .read(self, offset, size, &mut out)
        })() {
            Ok(()) => reply.data(out.as_ref()),
            Err(err) => reply.error(err.raw_os_error().unwrap_or(EIO)),
        }
    }
    fn release(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: fuse::ReplyEmpty,
    ) {
        reply.ok();
    }
    fn opendir(&mut self, _req: &fuse::Request, ino: u64, flags: u32, reply: fuse::ReplyOpen) {
        if TRACING {
            eprintln!("opendir(ino: {:?}, flags: {:?})", ino, flags);
        }
        reply.opened(0, 0);
    }
    fn readdir(
        &mut self,
        _req: &fuse::Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuse::ReplyDirectory,
    ) {
        if TRACING {
            eprintln!("readdir(ino: {:?}, offset: {:?})", ino, offset);
        }
        assert!(offset >= 0);
        let offset: usize = offset as usize;
        match (|| {
            self.get_by_inode(ino)?
                .as_inode_mut()?
                .readdir(self, offset, &mut reply)
        })() {
            Ok(_) => reply.ok(),
            Err(x) => reply.error(x),
        };
    }
    fn releasedir(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        reply: fuse::ReplyEmpty,
    ) {
        reply.ok();
    }
    fn getxattr(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _name: &OsStr,
        _size: u32,
        reply: fuse::ReplyXattr,
    ) {
        reply.error(ENOSYS);
    }
    fn listxattr(&mut self, _req: &fuse::Request, _ino: u64, _size: u32, reply: fuse::ReplyXattr) {
        reply.error(ENOSYS);
    }
    fn access(&mut self, _req: &fuse::Request, _ino: u64, _mask: u32, reply: fuse::ReplyEmpty) {
        reply.error(ENOSYS);
    }
    fn bmap(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: fuse::ReplyBmap,
    ) {
        reply.error(ENOSYS);
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs::create_dir, fs::File, io::Write, process::Stdio};

    use ostree_repo::Repo;
    use tempfile::tempdir;

    use crate::*;

    const FS_TAR: &[u8] = include_bytes!("../../ostree-repo/testdata/fs.tar");
    const BARE_USER_REPO_TAR: &[u8] = include_bytes!("../../ostree-repo/testdata/bare-user.tar");
    const COMMIT_OID: &str = "247b95a821f9cca301513b1ed57224b906d7e8fe117936db82afac43acee024a";

    #[test]
    fn test_retar() {
        // Setup
        let tmp = tempdir().unwrap();
        let mountpoint = tmp.path().join("repo");
        let repo = tmp.path().join("mnt");
        create_dir(&mountpoint).unwrap();
        create_dir(&repo).unwrap();

        let mut bare_user_repo = tar::Archive::new(BARE_USER_REPO_TAR);
        bare_user_repo.set_unpack_xattrs(true);
        bare_user_repo.set_preserve_permissions(true);
        bare_user_repo.unpack(&repo).unwrap();

        let repo = Repo::open(&repo).unwrap();
        let options = ["-o", "ro", "-o", "fsname=hello"]
            .iter()
            .map(|o| o.as_ref())
            .collect::<Vec<&OsStr>>();

        // The invariants to uphold for this unsafe aren't made explicit in the
        // fuse-rs documentations, but this is only tests.  See also
        // https://github.com/zargony/fuse-rs/commit/babbfd5ec216b9a7fb43a75872d69c3dfd01879e
        let _m = unsafe {
            fuse::spawn_mount(
                OstreeFs::new_from_repo(repo).unwrap(),
                &mountpoint,
                &options,
            )
            .unwrap()
        };

        let fs_path = mountpoint.join("by-commit").join(COMMIT_OID);
        let retar = std::process::Command::new("tar")
            .args(&[
                "-c",
                "-C",
                &fs_path.to_str().unwrap(),
                "--sort=name",
                "--numeric-owner",
                "--xattrs",
                "--format=posix",
                "--mtime=1970-01-01 00:00Z",
                "-v",
                "--pax-option=exthdr.name=%d/PaxHeaders/%f,atime:=0,ctime:=0",
                ".",
            ])
            .stderr(Stdio::inherit())
            .output()
            .unwrap()
            .stdout;
        if retar != FS_TAR {
            File::create(tmp.path().join("expected.tar"))
                .unwrap()
                .write_all(FS_TAR)
                .unwrap();
            File::create(tmp.path().join("actual.tar"))
                .unwrap()
                .write_all(&retar)
                .unwrap();
            let o = std::process::Command::new("diffoscope")
                .args(&[
                    tmp.path().join("expected.tar"),
                    tmp.path().join("actual.tar"),
                ])
                .output()
                .unwrap();
            println!("{}", std::str::from_utf8(&*o.stdout).unwrap());
            assert!(false);
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mountpoint = env::args_os().nth(2).unwrap();
    let repo: PathBuf = env::args_os().nth(1).unwrap().try_into()?;
    let repo = Repo::open(repo.as_path())?;
    let options = ["-o", "ro", "-o", "fsname=hello"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    mount(OstreeFs::new_from_repo(repo)?, &mountpoint, &options).unwrap();
    Ok(())
}
