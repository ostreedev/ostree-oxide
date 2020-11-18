use fuse::{mount, FileAttr, FileType, Filesystem};
use hex::FromHex;
use ostree_repo::{CommitId, ContentId, DirMetaId, DirTreeId, ObjId, Oid, Repo};
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

#[derive(Copy, Clone, Debug)]
struct Errno(libc::c_int);
impl Errno {
    const EFBIG: Errno = Errno(libc::EFBIG);
    const EIO: Errno = Errno(libc::EIO);
    const ENOENT: Errno = Errno(libc::ENOENT);
    const ENOTDIR: Errno = Errno(libc::ENOTDIR);
    const EISDIR: Errno = Errno(libc::EISDIR);
    const ENODATA: Errno = Errno(libc::ENODATA);
}
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
impl From<std::io::Error> for Errno {
    fn from(x: std::io::Error) -> Self {
        match x.raw_os_error() {
            Some(errno) => Errno(errno),
            None => Errno::EIO,
        }
    }
}

/* This is like the old try! macro, but for the reply pattern that fuse-rs
 * employs. */
macro_rules! try_reply {
    ($reply:expr, $stmt:expr) => {
        match $stmt {
            Ok(x) => x,
            Err(err) => {
                let err: Errno = err.into();
                $reply.error(err.0);
                return;
            }
        }
    };
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct InodeNo(u64);

impl InodeNo {
    const ROOT: InodeNo = InodeNo(1);
    const BY_COMMIT: InodeNo = InodeNo(2);

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
    Root(Root),
    ByCommit(ByCommit),
    Commit(Commit),
    Tree(Tree),
    File(Content),
}

impl FileRef {
    fn as_inode_mut(&mut self) -> &mut dyn INode {
        match self {
            FileRef::Root(x) => x,
            FileRef::ByCommit(x) => x,
            FileRef::Commit(x) => x,
            FileRef::Tree(x) => x,
            FileRef::File(x) => x,
        }
    }
}

fn iter_readdir<'a, It: Iterator<Item = Result<(InodeNo, FileType, &'a OsStr), Errno>>>(
    ino: InodeNo,
    parent_ino: InodeNo,
    it: It,
    mut offset: usize,
    reply: &mut fuse::ReplyDirectory,
) -> Result<(), Errno> {
    if write_special_dirents(ino, parent_ino, &mut offset, reply) {
        return Ok(());
    }
    for x in it.skip(offset - 2) {
        let (ino, kind, name) = x?;
        if reply.add(ino.0, offset as i64 + 1, kind, name) {
            break;
        }
        offset += 1
    }
    Ok(())
}

fn write_special_dirents(
    ino: InodeNo,
    parent_ino: InodeNo,
    offset: &mut usize,
    reply: &mut fuse::ReplyDirectory,
) -> bool {
    if *offset == 0 {
        *offset += 1;
        if reply.add(ino.as_u64(), *offset as i64, FileType::Directory, ".") {
            return true;
        }
    }
    if *offset == 1 {
        *offset += 1;
        if reply.add(
            parent_ino.as_u64(),
            *offset as i64,
            FileType::Directory,
            "..",
        ) {
            return true;
        }
    }
    false
}

trait INode {
    fn readdir(
        &mut self,
        fs: &OstreeFs,
        offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), Errno>;
    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, Errno>;
    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, Errno>;
    fn read(
        &mut self,
        fs: &OstreeFs,
        offset: i64,
        size: u32,
        reply: &mut Vec<u8>,
    ) -> Result<(), Errno>;
    fn getxattr(&mut self, _fs: &OstreeFs, _name: &OsStr) -> Result<Vec<u8>, Errno> {
        Err(Errno::ENODATA)
    }
    fn listxattr(&mut self, _fs: &OstreeFs) -> Result<Vec<Vec<u8>>, Errno> {
        Ok(vec![])
    }
}

struct Root {}
impl INode for Root {
    fn readdir(
        &mut self,
        _fs: &OstreeFs,
        offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), Errno> {
        iter_readdir(
            InodeNo::ROOT,
            InodeNo::from_u64(0),
            [Ok((
                InodeNo::BY_COMMIT,
                FileType::Directory,
                OsStr::new("by-commit"),
            ))]
            .iter()
            .copied(),
            offset,
            reply,
        )
    }

    fn getattr(&mut self, _fs: &OstreeFs) -> Result<FileAttr, Errno> {
        Ok(dir_attr(InodeNo::ROOT))
    }

    fn lookup(&mut self, _fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, Errno> {
        match name.to_str() {
            Some("by-commit") => Ok(dir_attr(InodeNo::BY_COMMIT)),
            _ => Err(Errno::ENOENT),
        }
    }

    fn read(
        &mut self,
        _fs: &OstreeFs,
        _offset: i64,
        _size: u32,
        _reply: &mut Vec<u8>,
    ) -> Result<(), Errno> {
        Err(Errno::EISDIR)
    }
}

struct ByCommit {}
impl INode for ByCommit {
    fn readdir(
        &mut self,
        fs: &OstreeFs,
        mut offset: usize,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), Errno> {
        if write_special_dirents(InodeNo::BY_COMMIT, InodeNo::ROOT, &mut offset, reply) {
            return Ok(());
        }
        for x in fs.repo.iter_objects().skip(offset - 2) {
            offset += 1;
            if let ObjId::Commit(cid) = x? {
                if reply.add(
                    InodeNo::from_commit_id(&cid).as_u64(),
                    offset as i64,
                    FileType::Directory,
                    cid.0.to_hex(),
                ) {
                    break;
                }
            }
        }
        Ok(())
    }

    fn getattr(&mut self, _fs: &OstreeFs) -> Result<FileAttr, Errno> {
        Ok(dir_attr(InodeNo::BY_COMMIT))
    }

    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, Errno> {
        if let Ok(oid) = Oid::from_hex(name.as_bytes()) {
            Commit { oid: CommitId(oid) }.getattr(fs)
        } else {
            eprintln!("Invalid commit oid: {:?}", name);
            Err(Errno::ENOENT)
        }
    }

    fn read(
        &mut self,
        _fs: &OstreeFs,
        _offset: i64,
        _size: u32,
        _reply: &mut Vec<u8>,
    ) -> Result<(), Errno> {
        Err(Errno::EISDIR)
    }
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
                return Err(Errno::ENOENT);
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
    ) -> Result<(), Errno> {
        self.to_tree(&fs.repo)?.readdir(fs, offset, reply)
    }

    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, Errno> {
        self.to_tree(&fs.repo)?.getattr(fs)
    }

    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, Errno> {
        self.to_tree(&fs.repo)?.lookup(fs, name)
    }

    fn read(
        &mut self,
        fs: &OstreeFs,
        offset: i64,
        size: u32,
        reply: &mut Vec<u8>,
    ) -> Result<(), Errno> {
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
    ) -> Result<(), Errno> {
        let dt_data = fs.repo.read_dirtree(&self.tree).map_err(|_| Errno::EIO)?;
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

    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, Errno> {
        let meta = fs.repo.read_dirmeta(&self.meta).map_err(|_| Errno::EIO)?;
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

    fn lookup(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<FileAttr, Errno> {
        let dt_data = fs
            .repo
            .read_dirtree(&self.tree)
            .map_err(|_| Errno::ENOENT)?;
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
        Err(Errno::ENOENT)
    }
    fn read(
        &mut self,
        _fs: &OstreeFs,
        _offset: i64,
        _size: u32,
        _reply: &mut Vec<u8>,
    ) -> Result<(), Errno> {
        Err(Errno::EISDIR)
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
    ) -> Result<(), Errno> {
        Err(Errno::ENOTDIR)
    }

    fn lookup(&mut self, _fs: &OstreeFs, _name: &OsStr) -> Result<FileAttr, Errno> {
        Err(Errno::ENOTDIR)
    }

    fn getattr(&mut self, fs: &OstreeFs) -> Result<FileAttr, Errno> {
        let meta = fs.repo.read_meta(&self.oid).map_err(|_| Errno::EIO)?;
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
                return Err(Errno::EIO);
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
    ) -> Result<(), Errno> {
        let mut f = fs.repo.open_object(&self.oid.into())?;
        f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
        reply.reserve(size as usize);
        f.take(size as u64).read_to_end(reply)?;
        Ok(())
    }
    fn getxattr(&mut self, fs: &OstreeFs, name: &OsStr) -> Result<Vec<u8>, Errno> {
        let xattrs = fs.repo.read_content_xattrs(&self.oid)?;
        Ok(xattrs
            .get(name.as_bytes())
            .ok_or(Errno::ENODATA)?
            .to_owned())
    }
    fn listxattr(&mut self, fs: &OstreeFs) -> Result<Vec<Vec<u8>>, Errno> {
        let xattrs = fs.repo.read_content_xattrs(&self.oid)?;
        Ok(xattrs
            .iter_keys()
            .map(|x| x.to_bytes().to_owned())
            .collect())
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

        for r in repo.iter_objects() {
            match &r? {
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
            }
        }
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
        match inode {
            InodeNo::ROOT => return Ok(FileRef::Root(Root {})),
            InodeNo::BY_COMMIT => return Ok(FileRef::ByCommit(ByCommit {})),
            _ => {}
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
        Err(Errno::ENOENT)
    }
}

impl Filesystem for OstreeFs {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), libc::c_int> {
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
        let mut ino = try_reply!(reply, self.get_by_inode(ino));
        let attr = try_reply!(reply, ino.as_inode_mut().getattr(self));
        reply.attr(&FOREVER, &attr);
    }
    fn lookup(&mut self, _req: &fuse::Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        let mut ino = try_reply!(reply, self.get_by_inode(parent));
        let attr = try_reply!(reply, ino.as_inode_mut().lookup(self, name));
        reply.entry(&FOREVER, &attr, 0);
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
            let obj = try_reply!(reply, self.repo.read_content(oid));
            reply.data(&*obj);
        } else {
            reply.error(Errno::ENOENT.into())
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
        let mut ino = try_reply!(reply, self.get_by_inode(ino));
        let ino = ino.as_inode_mut();
        try_reply!(reply, ino.read(self, offset, size, &mut out));
        reply.data(out.as_ref());
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
        let mut ino = try_reply!(reply, self.get_by_inode(ino));
        try_reply!(reply, ino.as_inode_mut().readdir(self, offset, &mut reply));
        reply.ok();
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
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: fuse::ReplyXattr,
    ) {
        let mut ino = try_reply!(reply, self.get_by_inode(ino));
        let xattr = try_reply!(reply, ino.as_inode_mut().getxattr(self, name));
        let needed: u32 = try_reply!(reply, xattr.len().try_into().map_err(|_| Errno::EFBIG));
        if needed == 0 {
            reply.data(&[]);
        } else if size == 0 {
            reply.size(needed);
        } else if size < needed {
            reply.error(libc::ERANGE)
        } else {
            reply.data(&xattr);
        }
    }
    fn listxattr(&mut self, _req: &fuse::Request, ino: u64, size: u32, reply: fuse::ReplyXattr) {
        if TRACING {
            eprintln!("listxattr({}, {})", ino, size);
        }
        let mut ino = try_reply!(reply, self.get_by_inode(ino));
        let xattrs = try_reply!(reply, ino.as_inode_mut().listxattr(self));
        let needed: u32 = try_reply!(
            reply,
            xattrs
                .iter()
                .map(|x| x.len() + 1)
                .sum::<usize>()
                .try_into()
                .map_err(|_| Errno::EFBIG)
        );
        if needed == 0 {
            reply.data(&[])
        } else if size == 0 {
            reply.size(needed)
        } else if size < needed {
            reply.error(libc::ERANGE)
        } else {
            let mut buf = vec![];
            buf.reserve_exact(needed as usize);
            for mut x in xattrs {
                buf.append(&mut x);
                buf.push(b'\0');
            }
            reply.data(&buf);
        }
    }
    fn access(&mut self, _req: &fuse::Request, _ino: u64, _mask: u32, reply: fuse::ReplyEmpty) {
        reply.error(libc::ENOSYS)
    }
    fn bmap(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: fuse::ReplyBmap,
    ) {
        reply.error(libc::ENOSYS);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{CStr, OsStr},
        fs::create_dir,
        fs::metadata,
        fs::File,
        io::Write,
        os::linux::fs::MetadataExt,
        path::Path,
        process::Stdio,
    };

    use nix::dir::Entry;
    use ostree_repo::Repo;
    use tempfile::tempdir;

    use crate::*;

    const FS_TAR: &[u8] = include_bytes!("../../ostree-repo/testdata/fs.tar");
    const BARE_USER_REPO_TAR: &[u8] = include_bytes!("../../ostree-repo/testdata/bare-user.tar");
    const COMMIT_OID: &str = "36ed56008e2f4f53012f4e3f5235767821c24adb0e14cdd364c420b624f1b323";

    fn with_fusemnt(f: impl FnOnce(&Path, &Path)) {
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
        f(mountpoint.as_ref(), tmp.path())
    }

    #[test]
    fn test_retar() {
        with_fusemnt(|mountpoint, tmp| {
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
                File::create(tmp.join("expected.tar"))
                    .unwrap()
                    .write_all(FS_TAR)
                    .unwrap();
                File::create(tmp.join("actual.tar"))
                    .unwrap()
                    .write_all(&retar)
                    .unwrap();
                let o = std::process::Command::new("diffoscope")
                    .args(&[tmp.join("expected.tar"), tmp.join("actual.tar")])
                    .output()
                    .unwrap();
                println!("{}", std::str::from_utf8(&*o.stdout).unwrap());
                assert!(false);
            }
        })
    }
    fn check_dir_specials(path: &Path, parent: &Path) {
        let ino = metadata(path).unwrap().st_ino();
        let parent_ino = metadata(parent).unwrap().st_ino();
        assert_eq!(metadata(path.join(".")).unwrap().st_ino(), ino);
        assert_eq!(metadata(path.join("..")).unwrap().st_ino(), parent_ino);

        let mut d = nix::dir::Dir::open(
            path,
            nix::fcntl::OFlag::O_DIRECTORY,
            nix::sys::stat::Mode::S_IXUSR,
        )
        .unwrap();
        let l: Result<Vec<Entry>, _> = d.iter().collect();
        let l = l.unwrap();
        let s_dot = CStr::from_bytes_with_nul(b".\0").unwrap();
        let s_dotdot = CStr::from_bytes_with_nul(b"..\0").unwrap();
        let dot = l
            .iter()
            .filter(|x| x.file_name() == s_dot)
            .next()
            .expect("Expected . entry in directory");
        let dotdot = l
            .iter()
            .filter(|x| x.file_name() == s_dotdot)
            .next()
            .expect("Expected .. entry in directory");
        assert_eq!(dot.ino(), ino);
        assert_eq!(dotdot.ino(), parent_ino);
    }

    #[test]
    fn test_specials_dirs() {
        with_fusemnt(|mountpoint, _tmp| {
            // TODO: Figure out how to refer to the inode of the parent of the root directory
            //check_dir_specials(mountpoint, _tmp);
            //check_dir_specials(&PathBuf::from("/run"), &PathBuf::from("/"));
            check_dir_specials(&mountpoint.join("by-commit"), mountpoint);
            check_dir_specials(
                &mountpoint.join("by-commit").join(COMMIT_OID),
                &mountpoint.join("by-commit"),
            );
        })
    }

    fn ls(path: impl AsRef<Path>) -> Vec<String> {
        std::fs::read_dir(path.as_ref())
            .unwrap()
            .map(|d| d.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    #[test]
    fn test_by_commit() {
        with_fusemnt(|mountpoint, _tmp| {
            assert_eq!(ls(mountpoint), &["by-commit"]);
            assert_eq!(ls(mountpoint.join("by-commit")), &[COMMIT_OID])
        });
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
