use gvariant::{
    aligned_bytes::{copy_to_align, read_to_slice, AlignedSlice, Alignment, AsAligned, A1, A4, A8},
    gv, Marker, Structure,
};
use hex::{FromHex, ToHex};
use nix::{dir::Dir, fcntl::OFlag, sys::stat::Mode};
use ref_cast::RefCast;
use std::{
    convert::TryInto,
    error::Error,
    fmt::Display,
    fs::File,
    io::{Read, Write},
    os::unix::io::{AsRawFd, FromRawFd},
    path::{Path, PathBuf},
};
use xattr::FileExt;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
#[repr(transparent)]
pub struct Oid(pub [u8; 32]);
impl Oid {
    pub const ZERO: Oid = Oid([
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]);
    pub fn try_from_slice(s: &[u8]) -> Option<&Self> {
        let x: &[u8; 32] = s.try_into().ok()?;
        Some(Self::ref_cast(x))
    }
    fn from_prefix_suffix(prefix: u8, suffix: &[u8; 31]) -> Oid {
        let mut out = Oid::ZERO;
        out.0[0] = prefix;
        out.0[1..].copy_from_slice(suffix);
        out
    }
}
impl Display for Oid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Oid::from_hex(\"{}\")", hex::encode(self.0))
    }
}

impl FromHex for Oid {
    type Error = hex::FromHexError;
    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        Ok(Self(<[u8; 32]>::from_hex(hex)?))
    }
}

impl ToHex for Oid {
    fn encode_hex<T: std::iter::FromIterator<char>>(&self) -> T {
        self.0.encode_hex()
    }
    fn encode_hex_upper<T: std::iter::FromIterator<char>>(&self) -> T {
        self.0.encode_hex_upper()
    }
}

impl AsRef<Oid> for [u8] {
    fn as_ref(&self) -> &Oid {
        match self.try_into() {
            Ok(a) => Oid::ref_cast(a),
            Err(_) => &Oid::ZERO,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
#[repr(transparent)]
pub struct CommitId(pub Oid);
impl CommitId {
    pub fn try_from_slice(s: &[u8]) -> Option<&Self> {
        Some(Self::ref_cast(Oid::try_from_slice(s)?))
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
#[repr(transparent)]
pub struct DirTreeId(pub Oid);
impl DirTreeId {
    pub fn try_from_slice(s: &[u8]) -> Option<&Self> {
        Some(Self::ref_cast(Oid::try_from_slice(s)?))
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
#[repr(transparent)]
pub struct DirMetaId(pub Oid);

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
#[repr(transparent)]
pub struct ContentId(pub Oid);
impl ContentId {
    pub fn try_from_slice(s: &[u8]) -> Option<&Self> {
        Some(Self::ref_cast(Oid::try_from_slice(s)?))
    }
}

fn open_file_at(d: &Dir, path: &impl AsRef<Path>) -> nix::Result<File> {
    let fd = nix::fcntl::openat(
        d.as_raw_fd(),
        path.as_ref(),
        OFlag::O_CLOEXEC | OFlag::O_RDONLY,
        Mode::empty(),
    )?;
    // The invariant that we need to uphold for this unsafe block is that the fd
    // must be owned by only one object.  As we've just created this fd this is
    // safe:
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[derive(Debug, Eq, PartialEq)]
pub enum ObjType {
    COMMIT,
    DIRTREE,
    DIRMETA,
    FILE,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash, Ord, PartialOrd)]
pub enum ObjId {
    Commit(CommitId),
    DirTree(DirTreeId),
    DirMeta(DirMetaId),
    Content(ContentId),
}
impl From<CommitId> for ObjId {
    fn from(x: CommitId) -> Self {
        ObjId::Commit(x)
    }
}
impl From<DirTreeId> for ObjId {
    fn from(x: DirTreeId) -> Self {
        ObjId::DirTree(x)
    }
}
impl From<DirMetaId> for ObjId {
    fn from(x: DirMetaId) -> Self {
        ObjId::DirMeta(x)
    }
}
impl From<ContentId> for ObjId {
    fn from(x: ContentId) -> Self {
        ObjId::Content(x)
    }
}

pub struct Repo {
    repo: Dir,
}

impl Repo {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Box<dyn Error>> {
        let repo = Dir::open(
            path.as_ref(),
            OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_RDONLY,
            Mode::empty(),
        )?;
        Ok(Self { repo })
    }
    pub fn open_object(&self, oid: &ObjId) -> Result<File, std::io::Error> {
        let (extension, sha) = match oid {
            ObjId::Commit(oid) => ("commit", oid.0),
            ObjId::DirTree(oid) => ("dirtree", oid.0),
            ObjId::DirMeta(oid) => ("dirmeta", oid.0),
            ObjId::Content(oid) => ("file", oid.0),
        };
        // TODO: Avoid this allocation
        let path = format!(
            "objects/{:02x}/{}.{}",
            sha.0[0],
            hex::encode(&sha.0[1..]),
            extension
        );
        Ok(open_file_at(&self.repo, &path).map_err(|x| x.as_errno().unwrap())?)
    }
    pub fn read_meta(&self, oid: &ContentId) -> Result<Meta, Box<dyn Error>> {
        let file = self.open_object(&ObjId::Content(*oid))?;
        let meta = file
            .get_xattr("user.ostreemeta")?
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
        let meta = copy_to_align(&meta);
        let m = file.metadata()?;
        Ok(Meta::from_data(meta.as_ref(), m.len()))
    }
    fn read_object<A: Alignment>(
        &self,
        oid: &ObjId,
    ) -> Result<Box<AlignedSlice<A>>, Box<dyn Error>> {
        let f = self.open_object(oid)?;
        Ok(read_to_slice(f, None)?)
    }
    pub fn read_content(&self, oid: &ContentId) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut f = self.open_object(&(*oid).into())?;
        let mut out = vec![];
        f.read_to_end(&mut out)?;
        Ok(out)
    }
    pub fn read_dirtree(&self, oid: &DirTreeId) -> Result<OwnedDirTree, Box<dyn Error>> {
        Ok(OwnedDirTree(self.read_object(&(*oid).into())?))
    }
    pub fn read_commit(&self, oid: &CommitId) -> Result<OwnedCommit, Box<dyn Error>> {
        Ok(OwnedCommit(self.read_object(&(*oid).into())?))
    }
    pub fn read_dirmeta(&self, oid: &DirMetaId) -> Result<Meta, Box<dyn Error>> {
        let data = self.read_object(&(*oid).into())?;
        Ok(Meta::from_data(&*data, 0))
    }
    pub fn read_content_xattrs(&self, oid: &ContentId) -> Result<Xattrs, Box<dyn Error>> {
        let file = self.open_object(&ObjId::Content(*oid))?;
        let meta = file
            .get_xattr("user.ostreemeta")?
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
        let meta = copy_to_align(&meta).into_owned();
        Ok(Xattrs::from_data(meta))
    }
    pub fn read_dirmeta_xattrs(&self, oid: &DirMetaId) -> Result<Xattrs, Box<dyn Error>> {
        let data = self.read_object(&(*oid).into())?;
        Ok(Xattrs::from_data(data))
    }
    pub fn for_each_object(&self, mut cb: impl FnMut(&ObjId)) -> Result<(), Box<dyn Error>> {
        for x in 0u8..=255 {
            let path: PathBuf = ["objects", &format!("{:02x}", x)].iter().collect();
            let d = Dir::openat(
                self.repo.as_raw_fd(),
                &path,
                OFlag::O_CLOEXEC | OFlag::O_DIRECTORY | OFlag::O_RDONLY,
                Mode::empty(),
            );
            let mut d = match d {
                Ok(d) => d,
                Err(nix::Error::Sys(nix::errno::Errno::ENOENT)) => continue,
                Err(x) => return Err(x.into()),
            };
            for y in d.iter() {
                let e = y?;
                let filename = e.file_name().to_bytes();
                match filename {
                    b"." | b".." => continue,
                    _ => {}
                }
                let (h, ext) = match memchr::memchr(b'.', filename) {
                    Some(mid) => filename.split_at(mid),
                    None => {
                        eprintln!(
                            "No . in object filename {:?}",
                            String::from_utf8_lossy(filename)
                        );
                        continue;
                    }
                };
                if h.len() != 62 {
                    eprintln!(
                        "Wrong length SHA ({}) in {:?}",
                        h.len(),
                        String::from_utf8_lossy(filename)
                    );
                    continue;
                }
                let oid_suffix = match <[u8;31]>::from_hex(h) {
                    Ok(x) => x,
                    Err(err) => {
                        eprintln!(
                            "Error converting {:?} from hex: {:?}",
                            String::from_utf8_lossy(&tmp),
                            err
                        );
                        continue;
                    }
                };
                let oid = Oid::from_prefix_suffix(x, &oid_suffix);
                let objid = match ext {
                    b".commit" => ObjId::Commit(CommitId(oid)),
                    b".dirmeta" => ObjId::DirMeta(DirMetaId(oid)),
                    b".dirtree" => ObjId::DirTree(DirTreeId(oid)),
                    b".file" => ObjId::Content(ContentId(oid)),
                    _ => {
                        eprintln!(
                            "Incorrect extension {:?} for file: {:?}",
                            ext,
                            String::from_utf8_lossy(filename)
                        );
                        continue;
                    }
                };
                cb(&objid);
            }
        }
        Ok(())
    }
}

fn buf_as_commit(buf: &AlignedSlice<A8>) -> Commit<'_> {
    let c = gv!("(a{sv}aya(say)sstayay)").cast(&buf);
    let (_, parent, _, _, _, _, dirtree, dirmeta) = c.to_tuple();

    Commit {
        parent: CommitId::ref_cast(parent.as_ref()),
        dirtree: DirTreeId::ref_cast(dirtree.as_ref()),
        dirmeta: DirMetaId::ref_cast(dirmeta.as_ref()),
    }
}

pub struct Commit<'a> {
    pub parent: &'a CommitId,
    pub dirtree: &'a DirTreeId,
    pub dirmeta: &'a DirMetaId,
}

pub struct OwnedCommit(Box<AlignedSlice<A8>>);
impl OwnedCommit {
    pub fn as_commit(&self) -> Commit<'_> {
        self.into()
    }
}
impl<'a> From<&'a OwnedCommit> for Commit<'a> {
    fn from(x: &'a OwnedCommit) -> Self {
        buf_as_commit(&x.0)
    }
}

// (a(say)a(sayay))
#[derive(Debug, RefCast)]
#[repr(transparent)]
pub struct DirTree<'a>(&'a AlignedSlice<A1>);

pub struct OwnedDirTree(Box<AlignedSlice<A1>>);
impl OwnedDirTree {
    pub fn as_dirtree(&self) -> DirTree<'_> {
        self.into()
    }
}
impl<'a> From<&'a OwnedDirTree> for DirTree<'a> {
    fn from(x: &'a OwnedDirTree) -> Self {
        DirTree(&x.0)
    }
}

impl<'a> DirTree<'a> {
    pub fn from_bytes(b: &'a [u8]) -> Self {
        DirTree(b.as_aligned())
    }
    pub fn iter_files(&'a self) -> impl Iterator<Item = FileEntry<'a>> + ExactSizeIterator {
        let files = gv!("(a(say)a(sayay))").cast(&self.0).to_tuple().0;
        files.into_iter().map(|x| x.to_tuple().into())
    }
    pub fn iter_dirs(&'a self) -> impl Iterator<Item = DirEntry<'a>> + ExactSizeIterator {
        let files = gv!("(a(say)a(sayay))").cast(&self.0).to_tuple().1;
        files.into_iter().map(|x| x.to_tuple().into())
    }
}

pub struct DirEntry<'a> {
    pub name: &'a str,
    pub dirtree_id: &'a DirTreeId,
    pub dirmeta_id: &'a DirMetaId,
}

impl<'a> DirEntry<'a> {
    pub fn from_tuple(t: (&'a gvariant::Str, &'a [u8], &'a [u8])) -> Option<Self> {
        Some(Self {
            name: t.0.to_str(),
            dirtree_id: DirTreeId::ref_cast(Oid::try_from_slice(t.1)?),
            dirmeta_id: DirMetaId::ref_cast(Oid::try_from_slice(t.2)?),
        })
    }
}

impl<'a> From<(&'a gvariant::Str, &'a [u8], &'a [u8])> for DirEntry<'a> {
    fn from(t: (&'a gvariant::Str, &'a [u8], &'a [u8])) -> Self {
        Self {
            name: t.0.to_str(),
            dirtree_id: DirTreeId::ref_cast(t.1.as_ref()),
            dirmeta_id: DirMetaId::ref_cast(t.2.as_ref()),
        }
    }
}

pub struct FileEntry<'a> {
    pub name: &'a str,
    pub oid: &'a ContentId,
}

impl<'a> FileEntry<'a> {
    pub fn from_tuple(t: (&'a gvariant::Str, &'a [u8])) -> Option<Self> {
        Some(t.into())
    }
}

impl<'a> From<(&'a gvariant::Str, &'a [u8])> for FileEntry<'a> {
    fn from(x: (&'a gvariant::Str, &'a [u8])) -> Self {
        FileEntry {
            name: x.0.to_str(),
            oid: ContentId::ref_cast(x.1.as_ref()),
        }
    }
}

// (uuua(ayay))
pub struct Meta {
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub size: u64,
}
pub struct Xattrs(Box<AlignedSlice<A4>>);
impl Xattrs {
    fn from_data(data: Box<AlignedSlice<A4>>) -> Xattrs {
        Xattrs(data)
    }
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let (_, _, _, xattrs) = gv!("(uuua(ayay))").cast(&self.0).to_tuple();
        for x in xattrs {
            let (k, v) = x.to_tuple();
            if k == key {
                return Some(v);
            }
        }
        None
    }
    pub fn iter_keys(&self) -> impl Iterator<Item = &[u8]> {
        let (_, _, _, xattrs) = gv!("(uuua(ayay))").cast(&self.0).to_tuple();
        xattrs.iter().map(|x| x.to_tuple().0)
    }
}

impl Meta {
    pub fn from_data(data: &AlignedSlice<A4>, size: u64) -> Self {
        let (uid, gid, mode, _xattrs) = gv!("(uuua(ayay))").cast(data).to_tuple();
        Self {
            uid: u32::from_be(*uid),
            gid: u32::from_be(*gid),
            mode: u32::from_be(*mode),
            size,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{CommitId, ObjId, Oid, Repo};
    use hex::FromHex;
    use std::path::PathBuf;

    #[test]
    fn oid_from_hex() {
        assert_eq!(
            Oid::from_hex("750574081316f0c9078674bbe599bb79902a9d1b8020a92c56b81a69179452e5")
                .unwrap(),
            Oid([
                0x75, 0x05, 0x74, 0x08, 0x13, 0x16, 0xf0, 0xc9, 0x07, 0x86, 0x74, 0xbb, 0xe5, 0x99,
                0xbb, 0x79, 0x90, 0x2a, 0x9d, 0x1b, 0x80, 0x20, 0xa9, 0x2c, 0x56, 0xb8, 0x1a, 0x69,
                0x17, 0x94, 0x52, 0xe5
            ])
        );
    }

    #[test]
    fn test_open_object() {
        todo!("Open object of all different types, make sure there's no error");
        todo!("Open object that doesn't exist, check for ENOENT");
    }

    #[test]
    fn test_for_each_object() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("testdata/repo");
        let r = Repo::open(d.as_path()).unwrap();
        let mut v = vec![];
        r.for_each_object(|id| v.push(*id)).unwrap();
        let oid = Oid([
            0x00u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
            0x28, 0x29, 0x30, 0x31,
        ]);
        assert_eq!(v[0], ObjId::Commit(CommitId(oid)));
    }
}
