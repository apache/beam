#!/bin/sh
set -e
set -x
envdir="$@"
virtualenv $envdir
. $envdir/bin/activate
ls -l $envdir/bin
cat $envdir/bin/pip
$envdir/bin/pip -v -V || (echo "!!! trace" ; $envdir/bin/python -m trace --trace $envdir/bin/pip -v -V ; exit)
$envdir/bin/pip install --upgrade tox==3.0.0 grpcio-tools==1.3.5 || (echo "!!! trace" ; $envdir/bin/python -m trace --trace $envdir/bin/pip install --upgrade tox==3.0.0 grpcio-tools==1.3.5 ; exit)
deactivate
