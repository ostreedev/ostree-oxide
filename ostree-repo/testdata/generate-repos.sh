#!/bin/sh -ex

# See https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=900593 about making
# PaxHeaders deterministic:
TAR_DETERMINISTIC='tar --sort=name --format=posix --mtime=1970-01-01T00:00Z --pax-option=exthdr.name=%d/PaxHeaders/%f,atime:=0,ctime:=0'

rm -rf archive
ostree init --repo=archive --mode=archive
ostree --repo=archive commit --timestamp=0 --tree=tar=fs.tar --branch=fs
$TAR_DETERMINISTIC -C archive -cv --owner=0 --group=0 . >archive.tar

rm -rf bare-user
ostree init --repo=bare-user --mode=bare-user
ostree --repo=bare-user remote add archive --no-gpg-verify file://$PWD/archive
ostree --repo=bare-user pull --mirror archive fs
ostree --repo=bare-user remote delete archive

# Work around bug in tar extracting non-writable files with xattrs set.  See
# https://savannah.gnu.org/bugs/?59184
chmod -R u+w bare-user/objects

$TAR_DETERMINISTIC -C bare-user -cv --owner=0 --group=0 --xattrs . >bare-user.tar

sudo rm -rf bare
sudo ostree init --repo=bare --mode=bare
sudo ostree --repo=bare remote add archive --no-gpg-verify file://$PWD/archive
sudo ostree --repo=bare pull --mirror archive fs
sudo ostree --repo=bare remote delete archive
sudo $TAR_DETERMINISTIC -C bare -cv --numeric-owner --xattrs . >bare.tar

