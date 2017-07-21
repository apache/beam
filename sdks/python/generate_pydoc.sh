#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

# This script will run sphinx to create documentation for python sdk
#
# Use "generate_pydocs.sh" to update documentation in the docs directory.
#
# The exit-code of the script indicates success or a failure.

# Quit on any errors
set -e

# Create docs directory if it does not exist
mkdir -p target/docs
rm -rf target/docs/*

mkdir -p target/docs/source

# Sphinx apidoc autodoc options
export SPHINX_APIDOC_OPTIONS=\
members,\
undoc-members,\
show-inheritance

# Exclude internal/experimental/Cython paths/patterns from the documentation.
excluded_internal_code=(
    apache_beam/coders/stream.py*
    apache_beam/examples/
    apache_beam/internal/
    apache_beam/io/gcp/internal/
    apache_beam/io/gcp/tests/
    apache_beam/runners/api/
    apache_beam/runners/test/
    apache_beam/runners/dataflow/internal/
    apache_beam/runners/portability/
    apache_beam/runners/worker/
    apache_beam/testing/
    *_pb2.py
    *_test.py
    *_test_common.py
    *_test_utils.py
)

python $(type -p sphinx-apidoc) -fMeT -o target/docs/source apache_beam \
    "${excluded_internal_code[@]}"

#=== conf.py ===#
cat > target/docs/source/conf.py <<'EOF'
import os
import sys

import sphinx_rtd_theme

sys.path.insert(0, os.path.abspath('../../..'))

exclude_patterns = [
    '_build',
    'target/docs/source/apache_beam.rst',
]

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]
master_doc = 'index'
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
project = 'Apache Beam'

autoclass_content = 'both'
autodoc_member_order = 'bysource'

doctest_global_setup = '''
import apache_beam as beam
import sys
'''

intersphinx_mapping = {
  'python': ('https://docs.python.org/2', None),
}

nitpicky = True
nitpick_ignore = [
  # Ignore internal private classes. If there's any reference to them within the
  # documentation, it will report as a broken cross reference. This can happen
  # if a class inherits from a private class.
  ('py:class', 'apache_beam.coders.coders._PickleCoderBase'),
  ('py:class', 'apache_beam.io.gcp.datastore.v1.datastoreio._Mutate'),
  ('py:class', 'apache_beam.runners.direct.executor.CallableTask'),
  ('py:class', 'apache_beam.transforms.trigger._ParallelTriggerFn'),
]
EOF

#=== index.rst ===#
cat > target/docs/source/index.rst <<'EOF'
.. include:: ./apache_beam.rst
   :start-line: 2
EOF

# Build the documentation using sphinx
# Reference: http://www.sphinx-doc.org/en/stable/man/sphinx-build.html
python $(type -p sphinx-build) -v -a -E -q target/docs/source \
  target/docs/_build -c target/docs/source \
  -w "target/docs/sphinx-build.warnings.log"

# Fail if there are errors or warnings in docs
! grep -q "ERROR:" target/docs/sphinx-build.warnings.log || exit 1
! grep -q "WARNING:" target/docs/sphinx-build.warnings.log || exit 1

# Run tests for code samples, these can be:
# - Code blocks using '.. testsetup::', '.. testcode::' and '.. testoutput::'
# - Interactive code starting with '>>>'
python -msphinx -M doctest target/docs/source \
  target/docs/_build -c target/docs/source \
  -w "target/docs/sphinx-doctest.warnings.log"

# Fail if there are errors or warnings in docs
! grep -q "ERROR:" target/docs/sphinx-doctest.warnings.log || exit 1
! grep -q "WARNING:" target/docs/sphinx-doctest.warnings.log || exit 1

# Message is useful only when this script is run locally.  In a remote
# test environment, this path will be removed when the test completes.
echo "Browse to file://$PWD/target/docs/_build/index.html"
