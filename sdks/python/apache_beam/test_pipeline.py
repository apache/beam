#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Test Pipeline, a wrapper of Pipeline for test purpose"""

import argparse
import shlex

from apache_beam.internal import pickler
from apache_beam.pipeline import Pipeline
from apache_beam.utils.pipeline_options import PipelineOptions
from nose.plugins.skip import SkipTest


class TestPipeline(Pipeline):
  """TestPipeline class is used inside of Beam tests that can be configured to
  run against pipeline runner.

  It has a functionality to parse arguments from command line and build pipeline
  options for tests who runs against a pipeline runner and utilizes resources
  of the pipeline runner. Those test functions are recommended to be tagged by
  @attr("ValidatesRunner") annotation.

  In order to configure the test with customized pipeline options from command
  line, system argument 'test-pipeline-options' can be used to obtains a list
  of pipeline options. If no options specified, default value will be used.

  For example, use following command line to execute all ValidatesRunner tests::

    python setup.py nosetests -a ValidatesRunner \
        --test-pipeline-options="--runner=DirectRunner \
                                 --job_name=myJobName \
                                 --num_workers=1"

  For example, use assert_that for test validation::

    pipeline = TestPipeline()
    pcoll = ...
    assert_that(pcoll, equal_to(...))
    pipeline.run()
  """

  def __init__(self,
               runner=None,
               options=None,
               argv=None,
               is_integration_test=False):
    """Initialize a pipeline object for test.

    Args:
      runner: An object of type 'PipelineRunner' that will be used to execute
        the pipeline. For registered runners, the runner name can be specified,
        otherwise a runner object must be supplied.
      options: A configured 'PipelineOptions' object containing arguments
        that should be used for running the pipeline job.
      argv: A list of arguments (such as sys.argv) to be used for building a
        'PipelineOptions' object. This will only be used if argument 'options'
        is None.
      is_integration_test: True if the test is an integration test, False
        otherwise.

    Raises:
      ValueError: if either the runner or options argument is not of the
      expected type.
    """
    self.is_integration_test = is_integration_test
    if options is None:
      options = PipelineOptions(self.get_test_option_args(argv))
    super(TestPipeline, self).__init__(runner, options)

  def _append_extra_opts(self, opt_list, extra_opts):
    """Append extra pipeline options to existing option list.

    Test verifier (if contains) should be pickled before append, and
    will be unpickled later in TestRunner.
    """
    for k, v in extra_opts.items():
      if not v:
        continue
      elif isinstance(v, bool) and v:
        opt_list.append('--%s' % k)
      elif 'matcher' in k:
        opt_list.append('--%s=%s' % (k, pickler.dumps(v)))
      else:
        opt_list.append('--%s=%s' % (k, v))

  def get_test_option_args(self, argv=None, **kwargs):
    """Get pipeline options as argument list by parsing value of command line
    argument: --test-pipeline-options combined with given extra options.

    Args:
      argv: An iterable of command line arguments to be used. If not specified
        then sys.argv will be used as input for parsing arguments.
      kwargs: Extra pipeline options for the test.

    Returns:
      An argument list of options that can be parsed by argparser or directly
      build a pipeline option.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-pipeline-options',
                        type=str,
                        action='store',
                        help='only run tests providing service options')
    known, unused_argv = parser.parse_known_args(argv)

    if self.is_integration_test and not known.test_pipeline_options:
      # Skip integration test when argument '--test-pipeline-options' is not
      # specified since nose calls integration tests when runs unit test by
      # 'setup.py test'.
      raise SkipTest('IT is skipped because --test-pipeline-options '
                     'is not specified')

    options_list = shlex.split(known.test_pipeline_options) \
      if known.test_pipeline_options else []

    if kwargs:
      self._append_extra_opts(options_list, kwargs)

    return options_list
