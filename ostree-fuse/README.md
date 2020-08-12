ostree-fuse
===========

An experimental FUSE filesystem implementation backed by an ostree repo.  This
allows mounting an ostree-repo and inspecting the contained data using normal
shell tools like `find`, etc.

Usage
-----

Mount your repo:

    $ ostree-fuse path-to-repo mount-point

Use it:

    $ ls mount-point/by-commit/\[commit-id\]
    lrwxrwxrwx 1 root root  8 Jan  1  1970 bin -> usr/bin
    drwxr-xr-x 2 root root  0 Jan  1  1970 boot
    drwxr-xr-x 2 root root  0 Jan  1  1970 dev
    lrwxrwxrwx 1 root root  9 Jan  1  1970 home -> var/home
    lrwxrwxrwx 1 root root  8 Jan  1  1970 lib -> usr/lib
    lrwxrwxrwx 1 root root  8 Jan  1  1970 mnt -> var/mnt
    lrwxrwxrwx 1 root root  8 Jan  1  1970 opt -> var/opt
    lrwxrwxrwx 1 root root 15 Jan  1  1970 ostree -> sysroot/ostree
    drwxr-xr-x 2 root root  0 Jan  1  1970 proc
    lrwxrwxrwx 1 root root 13 Jan  1  1970 root -> var/roothome
    drwxr-xr-x 2 root root  0 Jan  1  1970 run
    lrwxrwxrwx 1 root root  9 Jan  1  1970 sbin -> usr/sbin
    lrwxrwxrwx 1 root root  8 Jan  1  1970 srv -> var/srv
    drwxr-xr-x 2 root root  0 Jan  1  1970 sys
    drwxr-xr-x 2 root root  0 Jan  1  1970 sysroot
    drwxr-xr-x 2 root root  0 Jan  1  1970 tmp
    drwxr-xr-x 2 root root  0 Jan  1  1970 usr
    drwxr-xr-x 2 root root  0 Jan  1  1970 var
    $ cat mount-point/by-commit/.../usr/share/ostree/trusted.gpg.d/README-gpg
    Any GPG keyring files ending in ".gpg" placed in this directory will
    be automatically trusted by OSTree.

Installation
------------

    cargo build --release  &&  ./target/release/

Status
------

This is a proof of concept done for its own sake.  It's experimental.  I wrote
it because I wanted to know how ergonomic an efficient implementation of ostree
could be in rust, and I've wanted to play with FUSE for some time.

Progress:

- [x] Read only access
- [ ] Read-write access
- Backing repo types
    - [ ] bare
    - [x] bare-user
    - [ ] bare-user-only
    - [ ] archive
- [x] Lookup commits by SHA
- [ ] Lookup commits by ref
- [ ] Listing refs
- [ ] Listing commits
- [ ] Tests
- [ ] SUID files?
- [ ] Mount remote repositories
- [ ] Performance measurements and testing
- [ ] Set up CI

