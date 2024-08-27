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

# Check that the script is running in a known directory.
if [[ $PWD != *sdks/python* ]]; then
  echo 'Unable to locate Apache Beam Python SDK root directory'
  exit 1
fi

# Go to the Apache Beam Python SDK root
if [[ $PWD != *sdks/python ]]; then
  cd $(pwd | sed 's/sdks\/python.*/sdks\/python/')
fi

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

python_version=`python -V`
current_minor_version=`echo ${python_version} | sed -E "s/Python 3.([0-9])\..*/\1/"`

# Exclude internal, test, and Cython paths/patterns from the documentation.
excluded_patterns=(
    'apache_beam/coders/coder_impl.*'
    'apache_beam/coders/stream.*'
    'apache_beam/coders/coder_impl_row_encoders.*'
    'apache_beam/examples/'
    'apache_beam/io/gcp/tests/'
    'apache_beam/metrics/execution.*'
    'apache_beam/runners/api/'
    'apache_beam/runners/common.*'
    'apache_beam/runners/portability/'
    'apache_beam/runners/test/'
    'apache_beam/runners/worker/'
    'apache_beam/testing/benchmarks/chicago_taxi/'
    'apache_beam/testing/benchmarks/cloudml/'
    'apache_beam/testing/benchmarks/inference/'
    'apache_beam/testing/benchmarks/data/'
    'apache_beam/testing/benchmarks/load_tests/'
    'apache_beam/testing/analyzers'
    'apache_beam/testing/.*test.py'
    'apache_beam/testing/fast_test_utils.*'
    'apache_beam/tools/'
    'apache_beam/tools/map_fn_microbenchmark.*'
    'apache_beam/transforms/cy_combiners.*'
    'apache_beam/transforms/cy_dataflow_distribution_counter.*'
    'apache_beam/transforms/py_dataflow_distribution_counter.*'
    'apache_beam/utils/counters.*'
    'apache_beam/utils/windowed_value.*'
    'apache_beam/version.py'
    'apache_beam/yaml/integration_tests.py'
    '**/internal/*'
    '*_it.py'
    '*_pb2.py'
    '*_py3[0-9]*.py'
    '*_test.py'
    '*_test_common.py'
    '*_test_py3.py'
)

python $(type -p sphinx-apidoc) -fMeT -o target/docs/source apache_beam \
    "${excluded_patterns[@]}"

# Include inherited memebers for the DataFrame API
echo "    :inherited-members:" >> target/docs/source/apache_beam.dataframe.frames.rst

# Create the configuration and index files
#=== conf.py ===#
cat > target/docs/source/conf.py <<'EOF'
import os
import sys
from apache_beam import version as beam_version

import sphinx_rtd_theme

import numpy
import IPython

sys.path.insert(0, os.path.abspath('../../..'))

exclude_patterns = [
    '_build',
    'apache_beam.rst',
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
version = beam_version.__version__
release = version

autoclass_content = 'both'
autodoc_inherit_docstrings = False
autodoc_member_order = 'bysource'
autodoc_mock_imports = ["tensorrt", "cuda", "torch",
    "onnxruntime", "onnx", "tensorflow", "tensorflow_hub",
    "tensorflow_transform", "tensorflow_metadata", "transformers", "xgboost", "datatable", "transformers",
    "sentence_transformers", "redis", "tensorflow_text", "feast",
    ]

# Allow a special section for documenting DataFrame API
napoleon_custom_sections = ['Differences from pandas']

doctest_global_setup = '''
import apache_beam as beam
'''

intersphinx_mapping = {
  'python': ('https://docs.python.org/{}'.format(sys.version_info.major), None),
  'hamcrest': ('https://pyhamcrest.readthedocs.io/en/stable/', None),
  'numpy': ('https://numpy.org/doc/stable', None),
  'pandas': ('http://pandas.pydata.org/pandas-docs/dev', None),
}

# Since private classes are skipped by sphinx, if there is any cross reference
# to them, it will be broken. This can happen if a class inherits from a
# private class.
ignore_identifiers = [
  # Ignore "custom" builtin types
  '',
  'Any',
  'Dict',
  'Iterable',
  'List',
  'Set',
  'Text',
  'Tuple',

  # Ignore broken built-in type references
  'tuple',

  # Ignore private classes
  'apache_beam.coders.coders._PickleCoderBase',
  'apache_beam.coders.coders.FastCoder',
  'apache_beam.coders.coders.ListLikeCoder',
  'apache_beam.io._AvroSource',
  'apache_beam.io.fileio.FileSink',
  'apache_beam.io.gcp.bigquery.RowAsDictJsonCoder',
  'apache_beam.io.gcp.datastore.v1new.datastoreio._Mutate',
  'apache_beam.io.gcp.datastore.v1new.datastoreio.DatastoreMutateFn',
  'apache_beam.io.gcp.internal.clients.bigquery.'
      'bigquery_v2_messages.TableFieldSchema',
  'apache_beam.io.gcp.internal.clients.bigquery.'
      'bigquery_v2_messages.TableSchema',
  'apache_beam.io.iobase.SourceBase',
  'apache_beam.io.source_test_utils.ExpectedSplitOutcome',
  'apache_beam.metrics.metric.MetricResults',
  'apache_beam.pipeline.PipelineVisitor',
  'apache_beam.pipeline.PTransformOverride',
  'apache_beam.portability.api.schema_pb2.Schema',
  'apache_beam.pvalue.AsSideInput',
  'apache_beam.pvalue.DoOutputsTuple',
  'apache_beam.pvalue.PValue',
  'apache_beam.runners.direct.executor.CallableTask',
  'apache_beam.runners.portability.local_job_service.BeamJob',
  'apache_beam.testing.synthetic_pipeline._Random',
  'apache_beam.transforms.combiners.CombinerWithoutDefaults',
  'apache_beam.transforms.core.CallableWrapperCombineFn',
  'apache_beam.transforms.ptransform.PTransformWithSideInputs',
  'apache_beam.transforms.trigger._ParallelTriggerFn',
  'apache_beam.transforms.trigger.InMemoryUnmergedState',
  'apache_beam.transforms.utils.BatchElements',
  'apache_beam.typehints.typehints.AnyTypeConstraint',
  'apache_beam.typehints.typehints.CompositeTypeHint',
  'apache_beam.typehints.typehints.TypeConstraint',
  'apache_beam.typehints.typehints.validate_composite_type_param()',
  'apache_beam.utils.windowed_value._IntervalWindowBase',
  'apache_beam.coders.coder_impl.StreamCoderImpl',
  'apache_beam.io.requestresponse.Caller',
  'apache_beam.io.requestresponse.Repeater',
  'apache_beam.io.requestresponse.PreCallThrottler',
  'apache_beam.io.requestresponse.Cache',

  # Private classes which are used within the same module
  'apache_beam.transforms.external_test.PayloadBase',
  'apache_beam.typehints.typehints.WindowedTypeConstraint',

  # stdlib classes without documentation
  'unittest.case.TestCase',

  # DoFn param inner classes, due to a Sphinx misparsing of inner classes
  '_StateDoFnParam',
  '_TimerDoFnParam',
  '_BundleFinalizerParam',
  '_RestrictionDoFnParam',
  '_WatermarkEstimatorParam',
  '_BundleContextParam',
  '_SetupContextParam',

  # Sphinx cannot find this py:class reference target
  'callable',
  'types.FunctionType',
  'typing.Generic',
  'typing_extensions.Protocol',
  'concurrent.futures._base.Executor',
  'uuid',
  'google.cloud.datastore.key.Key',
  'google.cloud.datastore.entity.Entity',
  'google.cloud.datastore.batch.Batch',
  'is_in_ipython',
  'doctest.TestResults',

  # IPython Magics py:class reference target not found
  'IPython.core.magic.Magics',
]
ignore_references = [
  'BeamIOError',
  'HttpError',
  'ValueError',
]
# When inferring a base class it will use ':py:class'; if inferring a function
# argument type or return type, it will use ':py:obj'. We'll generate both.
nitpicky = True
nitpick_ignore = []
nitpick_ignore += [('py:class', iden) for iden in ignore_identifiers]
nitpick_ignore += [('py:obj', iden) for iden in ignore_identifiers]
nitpick_ignore += [('py:exc', iden) for iden in ignore_references]
EOF

#=== index.rst ===#
cat > target/docs/source/index.rst <<'EOF'
.. include:: ./apache_beam.rst
   :start-line: 2
EOF

# Build the documentation using sphinx
# Reference: http://www.sphinx-doc.org/en/stable/man/sphinx-build.html
# Note we cut out warnings from apache_beam.dataframe, this package uses pandas
# documentation verbatim, as do some of the textio transforms.
python $(type -p sphinx-build) -v -a -E -q target/docs/source \
  target/docs/_build -c target/docs/source \
  2>&1 | grep -E -v 'apache_beam\.dataframe.*WARNING:' \
  2>&1 | grep -E -v 'apache_beam\.io\.textio\.(ReadFrom|WriteTo)(Csv|Json).*WARNING:' \
  2>&1 | tee "target/docs/sphinx-build.log"

# Fail if there are errors or warnings in docs
! grep -q "ERROR:" target/docs/sphinx-build.log || exit 1
! grep -q "WARNING:" target/docs/sphinx-build.log || exit 1

# Run tests for code samples, these can be:
# - Code blocks using '.. testsetup::', '.. testcode::' and '.. testoutput::'
# - Interactive code starting with '>>>'
python -msphinx -M doctest target/docs/source \
  target/docs/_build -c target/docs/source \
  2>&1 | grep -E -v 'apache_beam\.dataframe.*WARNING:' \
  2>&1 | grep -E -v 'apache_beam\.io\.textio\.(ReadFrom|WriteTo)(Csv|Json).*WARNING:' \
  2>&1 | tee "target/docs/sphinx-doctest.log"

# Fail if there are errors or warnings in docs
! grep -q "ERROR:" target/docs/sphinx-doctest.log || exit 1
! grep -q "WARNING:" target/docs/sphinx-doctest.log || exit 1

# Message is useful only when this script is run locally.  In a remote
# test environment, this path will be removed when the test completes.
echo "Browse to file://$PWD/target/docs/_build/index.html"
