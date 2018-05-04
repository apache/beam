#!/bin/sh
set -e
envdir="$@"
virtualenv $envdir
. $envdir/bin/activate
pip -V
pip install --upgrade tox==3.0.0 grpcio-tools==1.3.5
deactivate
