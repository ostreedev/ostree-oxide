Test data
=========

`fs.tar` contains an example directory contents.  I created this with:

    sudo tar -c --sort=name --numeric-owner --xattrs --format=posix \\
             --mtime='1970-01-01 00:00Z' -v \\
             --pax-option=exthdr.name=%d/PaxHeaders/%f,atime:=0,ctime:=0 . >../fs.tar

A tarball seems like a good choice for a serialisation format for this data. It
can capture all the relevant data and metadata of the tree were interested in
and reproduce it easily.  It's installed everywhere and is very mature.

Similarly we capture ostree repos generated from this `fs.tar` by running
`generate-repos.sh`.  This depends on `ostree` itself.  We commit the contents
of these tarballs to git so normal testing ostree-oxide doesn't depend on ostree
to be installed.

