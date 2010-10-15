#!/bin/bash

[ "$(dpkg-parsechangelog -c1 | awk '$1=="Version:" {print $2}')" \
  = "$(python setup.py --version)" ] \
|| {
    echo "debian/changelog version does not match setup.py version" >&2
    exit 1
}
