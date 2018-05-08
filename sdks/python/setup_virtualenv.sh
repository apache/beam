#!/bin/sh
set -e
set -x
envdir="$@"
virtualenv $envdir
. $envdir/bin/activate
ls -l $envdir/bin
$envdir/bin/pip -V
$envdir/bin/pip install --upgrade tox==3.0.0 grpcio-tools==1.3.5
deactivate
