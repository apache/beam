#!/bin/sh
set -e
set -x
envdir="$@"
virtualenv $envdir
. $envdir/bin/activate
ls -l $envdir/bin
cat $envdir/bin/pip
$envdir/bin/python -m trace --trace $envdir/bin/pip -v -V
$envdir/bin/pip install --upgrade tox==3.0.0 grpcio-tools==1.3.5
deactivate
