use fuse::{mount, FileAttr, FileType, Filesystem};
use hex::FromHex;
use libc::{c_int, EINVAL, EIO, ENOENT, ENOSYS};
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

struct StaticDir {
    attr: FileAttr,
    entries: &'static [(InodeNo, i64, FileType, &'static str)],
}

const STATIC_DIRS: &'static [StaticDir] = &[
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct InodeNo(u64);

impl InodeNo {
    const ROOT: InodeNo = InodeNo(1);
    const BY_COMMIT: InodeNo = InodeNo(2);
    const REFS: InodeNo = InodeNo(3);

    const DIRTREE_MASK: u64 = 0x00000fffffffffff;
    const DIRMETA_MASK: u64 = 0xfffff00000000000;

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
    fn to_dir_sid(&self) -> (u64, u64) {
        (self.0 & Self::DIRMETA_MASK, self.0 & Self::DIRTREE_MASK)
    }
    const fn as_u64(self) -> u64 {
        self.0
    }
}

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
        let (dmsid, dtsid) = ino.to_dir_sid();
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
    fn get_dir(&self, ino: InodeNo) -> Option<(DirMetaId, DirTreeId)> {
        let (dm, dt) = ino.to_dir_sid();
        //eprintln!("dm: {:x} dt: {:x}", dm, dt);
        if let (Some(dirmeta_oid), Some(dirtree_oid)) =
            (self.dirmetas.get(&dm), self.dirtrees.get(&dt))
        {
            Some((*dirmeta_oid, *dirtree_oid))
        } else {
            None
        }
    }
    fn readdir(
        &mut self,
        _req: &fuse::Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        reply: &mut fuse::ReplyDirectory,
    ) -> Result<(), i32> {
        let ino = InodeNo::from_u64(ino);
        assert!(offset >= 0);
        let mut offset: usize = offset as usize;
        if let Some(d) = static_dir(ino) {
            for entry in d.entries.iter().skip(offset as usize) {
                reply.add((entry.0).0, entry.1, entry.2, entry.3);
                //eprintln!("Added entry {}", entry.3);
            }
            return Ok(());
        }
        let (_, dt) = self.get_dir(ino).ok_or(EINVAL)?;
        let dt_data = self.repo.read_dirtree(&dt).map_err(|_| EIO)?;
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
    fn getattr(&mut self, _req: &fuse::Request, ino: u64) -> Result<FileAttr, i32> {
        let ino = InodeNo::from_u64(ino);
        if let Some(sd) = static_dir(ino) {
            return Ok(sd.attr);
        }
        if let Some(oid) = self.files.get(&ino) {
            return self.file_getattr(oid);
        }

        if let Some((dirmeta_oid, _)) = self.get_dir(ino) {
            return Ok(FileAttr {
                ino: ino.as_u64(),
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            });
        }
        if self.commits.contains_key(&ino) {
            return Ok(dir_attr(ino));
        }
        Err(ENOENT)
    }
    fn file_getattr(&self, oid: &ContentId) -> Result<FileAttr, i32> {
        let meta = self.repo.read_meta(oid).map_err(|_| EIO)?;
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
        return Ok(FileAttr {
            ino: InodeNo::from_file_id(oid).as_u64(),
            size: meta.size,
            blocks: (meta.size + 511) / 512,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: kind,
            perm: (meta.mode & 0o777) as u16,
            nlink: 1,
            uid: meta.uid,
            gid: meta.gid,
            rdev: 0,
            flags: 0,
        });
    }
    fn lookup(&mut self, req: &fuse::Request, parent: u64, name: &OsStr) -> Result<FileAttr, i32> {
        let parent = InodeNo::from_u64(parent);
        if let Some(sd) = static_dir(parent) {
            for attr in sd.entries.iter() {
                if name == attr.3 {
                    return Ok(dir_attr(attr.0));
                }
            }
        }
        if parent == InodeNo::BY_COMMIT {
            if let Ok(oid) = Oid::from_hex(name.as_bytes()) {
                let buf = match self.repo.read_commit(&CommitId(oid)) {
                    Ok(x) => x,
                    Err(err) => {
                        eprintln!("Error loading commit {:?}: {:?}", oid, err);
                        return Err(ENOENT);
                    }
                };
                let commit = buf.as_commit();
                // TODO: Lookup the attrs from the dirmeta
                return Ok(dir_attr(InodeNo::from_dir_oid(
                    commit.dirmeta,
                    commit.dirtree,
                )));
            } else {
                eprintln!("Invalid commit oid: {:?}", name);
            }
        }
        if let Some((dm, dtid)) = self.get_dir(parent) {
            let dt_data = self.repo.read_dirtree(&dtid).map_err(|_| ENOENT)?;
            let dt = dt_data.as_dirtree();
            for de in dt.iter_dirs() {
                if de.name == name {
                    return Ok(FileAttr {
                        ino: InodeNo::from_dir_oid(de.dirmeta_id, de.dirtree_id).as_u64(),
                        size: 0,
                        blocks: 0,
                        atime: UNIX_EPOCH,
                        mtime: UNIX_EPOCH,
                        ctime: UNIX_EPOCH,
                        crtime: UNIX_EPOCH,
                        kind: FileType::Directory,
                        // TODO: Get permissions from dirmeta
                        perm: 0o755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        flags: 0,
                    });
                }
            }
            for fe in dt.iter_files() {
                if fe.name == name {
                    return self.file_getattr(&fe.oid);
                }
            }
            //let dm_data = self.repo.read_object(&dm.0, ObjType::DIRMETA).map_err(|_| ENOENT)?;
            //let (uid, gid, mode, xattrs) = gv!("(uuua(ayay))").cast(&dm_data.as_aligned()).to_tuple();
        }
        Err(ENOENT)
    }
    fn read(
        &mut self,
        _req: &fuse::Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let ino = InodeNo::from_u64(ino);
        let oid = self.files.get(&ino).ok_or(std::io::ErrorKind::NotFound)?;
        let mut f = self.repo.open_object(&(*oid).into())?;
        f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
        reply.reserve(size as usize);
        f.take(size as u64).read_to_end(reply)?;
        Ok(())
    }
}

impl Filesystem for OstreeFs {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), c_int> {
        //eprintln!("init()");
        Ok(())
    }
    fn destroy(&mut self, _req: &fuse::Request) {}
    fn forget(&mut self, _req: &fuse::Request, _ino: u64, _nlookup: u64) {}
    fn getattr(&mut self, req: &fuse::Request, ino: u64, reply: fuse::ReplyAttr) {
        //eprintln!("getattr(ino: {:?})", ino);
        match self.getattr(req, ino) {
            Ok(x) => reply.attr(&FOREVER, &x),
            Err(errno) => reply.error(errno),
        }
    }
    fn lookup(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        let out = match self.lookup(req, parent, name) {
            Ok(attr) => reply.entry(&FOREVER, &attr, 0),
            Err(errno) => reply.error(errno),
        };
        //eprintln!(
        //   "lookup(parent: {:?}, name: {:?}) -> {:?}",
        //    parent, name, out
        //);
        out
    }
    fn readlink(&mut self, _req: &fuse::Request, ino: u64, mut reply: fuse::ReplyData) {
        //eprintln!("readlink(ino: {:?})", ino);
        let ino = InodeNo::from_u64(ino);
        if let Some(oid) = self.files.get(&ino) {
            match self.repo.read_object(&(*oid).into()) {
                Ok(obj) => reply.data(&*obj),
                Err(_) => reply.error(ENOENT),
            };
        } else {
            reply.error(ENOENT)
        }
    }
    fn open(&mut self, _req: &fuse::Request, ino: u64, flags: u32, reply: fuse::ReplyOpen) {
        //eprintln!("open(ino: {:?}, flags: {:?})", ino, flags);
        reply.opened(0, 0);
    }
    fn read(
        &mut self,
        req: &fuse::Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: fuse::ReplyData,
    ) {
        //eprintln!(
        //    "read(ino: {:?}, offset: {:?}, size: {:?})",
        //    ino, offset, size
        //);
        let mut out = Vec::new();
        match self.read(req, ino, fh, offset, size, &mut out) {
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
        //eprintln!("opendir(ino: {:?}, flags: {:?})", ino, flags);
        reply.opened(0, 0);
    }
    fn readdir(
        &mut self,
        req: &fuse::Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuse::ReplyDirectory,
    ) {
        //eprintln!("readdir(ino: {:?}, offset: {:?})", ino, offset);
        match self.readdir(req, ino, fh, offset, &mut reply) {
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
