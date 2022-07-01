#!/bin/bash

# This is a script rather than inlined in package.json so as to be portable
# for windows.

set -e

ttsc -p .
mkdir -p dist/resources
cp ../java/extensions/python/src/main/resources/org/apache/beam/sdk/extensions/python/bootstrap_beam_venv.py dist/resources
