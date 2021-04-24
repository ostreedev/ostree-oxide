use gvariant::{
    aligned_bytes::{copy_to_align, read_to_slice, AlignedSlice, Alignment, AsAligned, A1, A4, A8},
    gv, Marker, Structure,
};
use hex::{FromHex, ToHex};
use itertools::Either;
use openat::Dir;
use ref_cast::RefCast;
use std::{
    convert::TryInto,
    error::Error,
    ffi::{CStr, OsStr},
    fmt::Display,
    fs::File,
    io::{self, ErrorKind, Read},
    iter::empty,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use xattr::FileExt;

mod refs;

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
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
    pub fn to_hex(&self) -> OidHex {
        let mut out = [0u8; 65];
        hex::encode_to_slice(&self.0, &mut out[..64]).unwrap();
        OidHex(out)
    }
}
impl Display for Oid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Oid::from_hex(\"{}\")", hex::encode(self.0))
    }
}
impl std::fmt::Debug for Oid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
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

pub struct OidHex([u8; 65]);
impl From<Oid> for OidHex {
    fn from(oid: Oid) -> Self {
        oid.to_hex()
    }
}
impl AsRef<CStr> for OidHex {
    fn as_ref(&self) -> &CStr {
        CStr::from_bytes_with_nul(&self.0).unwrap()
    }
}
impl AsRef<str> for OidHex {
    fn as_ref(&self) -> &str {
        std::str::from_utf8(&self.0[..64]).unwrap()
    }
}
impl AsRef<OsStr> for OidHex {
    fn as_ref(&self) -> &OsStr {
        let x: &str = self.as_ref();
        x.as_ref()
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
impl DirMetaId {
    pub fn try_from_slice(s: &[u8]) -> Option<&Self> {
        Some(Self::ref_cast(Oid::try_from_slice(s)?))
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, RefCast, Copy, Clone)]
#[repr(transparent)]
pub struct ContentId(pub Oid);
impl ContentId {
    pub fn try_from_slice(s: &[u8]) -> Option<&Self> {
        Some(Self::ref_cast(Oid::try_from_slice(s)?))
    }
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

#[derive(Debug)]
pub struct Repo {
    repo: Dir,
}

impl Repo {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Box<dyn Error>> {
        let repo = Dir::open(path.as_ref())?;
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
        self.repo.open_file(path)
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
    ) -> Result<Box<AlignedSlice<A>>, std::io::Error> {
        let f = self.open_object(oid)?;
        Ok(read_to_slice(f, None)?)
    }
    pub fn read_content(&self, oid: &ContentId) -> Result<Vec<u8>, std::io::Error> {
        let mut f = self.open_object(&(*oid).into())?;
        let mut out = vec![];
        f.read_to_end(&mut out)?;
        Ok(out)
    }
    pub fn read_dirtree(&self, oid: &DirTreeId) -> Result<OwnedDirTree, std::io::Error> {
        Ok(OwnedDirTree(self.read_object(&(*oid).into())?))
    }
    pub fn read_commit(&self, oid: &CommitId) -> Result<OwnedCommit, std::io::Error> {
        Ok(OwnedCommit(self.read_object(&(*oid).into())?))
    }
    pub fn read_dirmeta(&self, oid: &DirMetaId) -> Result<Meta, std::io::Error> {
        let data = self.read_object(&(*oid).into())?;
        Ok(Meta::from_data(&*data, 0))
    }
    pub fn read_content_xattrs(&self, oid: &ContentId) -> Result<Xattrs, std::io::Error> {
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
    pub fn iter_objects(&self) -> impl Iterator<Item = Result<ObjId, std::io::Error>> + '_ {
        (0..=255)
            .into_iter()
            .map(move |prefix| self.iter_objects_with_prefix(prefix))
            .flatten()
    }
    fn iter_objects_with_prefix(
        &self,
        prefix: u8,
    ) -> impl Iterator<Item = Result<ObjId, std::io::Error>> {
        let path: PathBuf = ["objects", &format!("{:02x}", prefix)].iter().collect();
        let d = self.repo.list_dir(&path);
        let d = match d {
            Ok(d) => d,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Either::Right(Either::Left(empty()))
            }
            Err(x) => return Either::Right(Either::Right(std::iter::once(Err(x)))),
        };
        Either::Left(d.into_iter().filter_map(move |entry| match entry {
            Ok(entry) => dirent_to_objid(prefix, entry).map(Ok),
            Err(x) => Some(Err(x)),
        }))
    }
    /// Pass prefix = "refs/heads" for heads. Prefix must start with "refs/"
    pub fn read_ref(&self, name: &Path) -> io::Result<CommitId> {
        refs::check_ref_prefix(name)?;
        refs::read_ref(&self.repo, name)
    }
    pub fn list_refs(&self, path: PathBuf) -> io::Result<refs::ListRefs> {
        refs::ListRefs::new(&self.repo, path)
    }
}

fn dirent_to_objid(prefix: u8, entry: openat::Entry) -> Option<ObjId> {
    let filename = entry.file_name().as_bytes();
    match filename {
        b"." | b".." => return None,
        _ => {}
    }
    if filename.len() < 62 {
        eprintln!(
            "Not a valid object filename: {:?}.  Filename too short",
            String::from_utf8_lossy(filename)
        );
        return None;
    }
    let (str_sha, ext) = filename.split_at(62);
    let oid_suffix = match <[u8; 31]>::from_hex(str_sha) {
        Ok(x) => x,
        Err(err) => {
            eprintln!(
                "Error converting {:?} from hex: {:?}",
                String::from_utf8_lossy(&str_sha),
                err
            );
            return None;
        }
    };
    let oid = Oid::from_prefix_suffix(prefix, &oid_suffix);
    Some(match ext {
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
            return None;
        }
    })
}

fn buf_as_commit(buf: &AlignedSlice<A8>) -> Commit<'_> {
    let c = gv!("(a{sv}aya(say)sstayay)").cast(&buf);
    let (_metadata, parent, _related, subject, body, timestamp, root_contents, root_metadata) =
        c.to_tuple();
    Commit {
        parent,
        subject,
        body,
        timestamp,
        root_contents,
        root_metadata,
    }
}

pub struct Commit<'a> {
    //metadata: &'a <unnameable>,
    parent: &'a [u8],
    //related: &'a <unnameable>,
    subject: &'a gvariant::Str,
    body: &'a gvariant::Str,
    timestamp: &'a u64,
    root_contents: &'a [u8],
    root_metadata: &'a [u8],
}

impl<'a> Commit<'a> {
    /// Parent commit
    ///
    /// This will be `None` if the commit was created with `ostree commit --orphan`.
    pub fn parent(&self) -> Option<CommitId> {
        CommitId::try_from_slice(self.parent).copied()
    }
    /// One line subject
    ///
    /// Note: this isn't guaranteed to be a single line, it's just a convention.
    ///
    /// This can also be empty if a subject wasn't specified at commit creation time.
    pub fn subject(&self) -> &str {
        self.subject.into()
    }
    /// Full description
    ///
    /// This can be empty if it wasn't specified at commit creation time.
    pub fn body(&self) -> &str {
        self.body.into()
    }
    pub fn root_dirtree(&self) -> DirTreeId {
        match DirTreeId::try_from_slice(self.root_contents) {
            Some(x) => *x,
            None => {
                //warn!("DirTree invalid in commit");
                *DirTreeId::ref_cast(&Oid::ZERO)
            }
        }
    }
    pub fn root_dirmeta(&self) -> DirMetaId {
        match DirMetaId::try_from_slice(self.root_metadata) {
            Some(x) => *x,
            None => {
                //warn!("DirTree invalid in commit");
                *DirMetaId::ref_cast(&Oid::ZERO)
            }
        }
    }
    pub fn timestamp(&self) -> SystemTime {
        UNIX_EPOCH + Duration::new(*self.timestamp, 0)
    }
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
impl std::fmt::Debug for OwnedCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_commit())
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
            if let Ok(k) = CStr::from_bytes_with_nul(k) {
                if k.to_bytes() == key {
                    return Some(v);
                }
            }
        }
        None
    }
    pub fn iter_keys(&self) -> impl Iterator<Item = &CStr> {
        let (_, _, _, xattrs) = gv!("(uuua(ayay))").cast(&self.0).to_tuple();
        xattrs
            .iter()
            .filter_map(|x| CStr::from_bytes_with_nul(&x.to_tuple().0).ok())
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
    use crate::{CommitId, ContentId, ObjId, Oid, Repo};
    use hex::FromHex;
    use std::{ffi::CStr, path::PathBuf};
    use tempfile::tempdir;

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

    const BARE_USER_REPO_TAR: &[u8] = include_bytes!("../../ostree-repo/testdata/bare-user.tar");

    fn with_repo() -> (tempfile::TempDir, Repo) {
        // Setup
        let tmp = tempdir().unwrap();
        let mountpoint = tmp.path().join("repo");
        let repo = tmp.path().join("mnt");
        std::fs::create_dir(&mountpoint).unwrap();
        std::fs::create_dir(&repo).unwrap();

        let mut bare_user_repo = tar::Archive::new(BARE_USER_REPO_TAR);
        bare_user_repo.set_unpack_xattrs(true);
        bare_user_repo.set_preserve_permissions(true);
        bare_user_repo.unpack(&repo).unwrap();

        let repo = Repo::open(&repo).unwrap();
        (tmp, repo)
    }

    #[test]
    fn test_read_xattrs() {
        let (_tmpdir, repo) = with_repo();
        let xattrs = repo
            .read_content_xattrs(&ContentId(
                Oid::from_hex("aaf192101c4250090c9c442189abc29f34959fa1f1288d3d58755b4897cb6c53")
                    .unwrap(),
            ))
            .unwrap();
        assert_eq!(
            xattrs.iter_keys().collect::<Vec<_>>(),
            [
                CStr::from_bytes_with_nul(b"user.cow\0").unwrap(),
                CStr::from_bytes_with_nul(b"user.no-value\0").unwrap(),
            ]
        );
        assert_eq!(xattrs.get(b"user.no-value").unwrap(), b"");
        assert_eq!(xattrs.get(b"user.cow").unwrap(), b"goes moo");
    }

    #[test]
    fn test_for_each_object() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("testdata/repo");
        let r = Repo::open(d.as_path()).unwrap();
        let v: Result<Vec<_>, _> = r.iter_objects().collect();
        let v = v.unwrap();
        let oid = Oid([
            0x00u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13,
            0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
            0x28, 0x29, 0x30, 0x31,
        ]);
        assert_eq!(v[0], ObjId::Commit(CommitId(oid)));
    }
}
