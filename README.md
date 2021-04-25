ostree-oxide
============

A pure-rust implementation of tools for reading ostree repositories.  It does
not link to or use libostree, but implements the ostree on-disk format.

It consists of:

1. [ostree-repo](ostree-repo) - a rust library for working with ostree repos.
2. [ostree-fuse](ostree-fuse/README.md) - a FUSE implementation allowing ostree
   commits to be mounted from bare-user ostree repositories.
3. [ostree-cmd](ostree-cmd) - a limited command line interface mimicing `ostree`

Status
------

This is an experimental proof of concept made for its own sake.  There is
currently no stable API, little documentation and very few tests.  This may
change in the future, but have no expectation that it will.
