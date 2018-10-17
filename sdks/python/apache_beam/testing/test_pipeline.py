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

from __future__ import absolute_import

import argparse
import shlex

from nose.plugins.skip import SkipTest

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.runners.runner import PipelineState

__all__ = [
    'TestPipeline',
    ]


class TestPipeline(Pipeline):
  """:class:`TestPipeline` class is used inside of Beam tests that can be
  configured to run against pipeline runner.

  It has a functionality to parse arguments from command line and build pipeline
  options for tests who runs against a pipeline runner and utilizes resources
  of the pipeline runner. Those test functions are recommended to be tagged by
  ``@attr("ValidatesRunner")`` annotation.

  In order to configure the test with customized pipeline options from command
  line, system argument ``--test-pipeline-options`` can be used to obtains a
  list of pipeline options. If no options specified, default value will be used.

  For example, use following command line to execute all ValidatesRunner tests::

    python setup.py nosetests -a ValidatesRunner \\
        --test-pipeline-options="--runner=DirectRunner \\
                                 --job_name=myJobName \\
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
               is_integration_test=False,
               blocking=True):
    """Initialize a pipeline object for test.

    Args:
      runner (~apache_beam.runners.runner.PipelineRunner): An object of type
        :class:`~apache_beam.runners.runner.PipelineRunner` that will be used
        to execute the pipeline. For registered runners, the runner name can be
        specified, otherwise a runner object must be supplied.
      options (~apache_beam.options.pipeline_options.PipelineOptions):
        A configured
        :class:`~apache_beam.options.pipeline_options.PipelineOptions`
        object containing arguments that should be used for running the
        pipeline job.
      argv (List[str]): A list of arguments (such as :data:`sys.argv`) to be
        used for building a
        :class:`~apache_beam.options.pipeline_options.PipelineOptions` object.
        This will only be used if argument **options** is :data:`None`.
      is_integration_test (bool): :data:`True` if the test is an integration
        test, :data:`False` otherwise.
      blocking (bool): Run method will wait until pipeline execution is
        completed.

    Raises:
      ~exceptions.ValueError: if either the runner or options argument is not
        of the expected type.
    """
    self.is_integration_test = is_integration_test
    self.not_use_test_runner_api = False
    self.options_list = self._parse_test_option_args(argv)
    self.blocking = blocking
    if options is None:
      options = PipelineOptions(self.options_list)
    super(TestPipeline, self).__init__(runner, options)

  def run(self, test_runner_api=True):
    result = super(TestPipeline, self).run(
        test_runner_api=(False if self.not_use_test_runner_api
                         else test_runner_api))
    if self.blocking:
      state = result.wait_until_finish()
      assert state in (PipelineState.DONE, PipelineState.CANCELLED), \
          "Pipeline execution failed."

    return result

  def _parse_test_option_args(self, argv):
    """Parse value of command line argument: --test-pipeline-options to get
    pipeline options.

    Args:
      argv: An iterable of command line arguments to be used. If not specified
        then sys.argv will be used as input for parsing arguments.

    Returns:
      An argument list of options that can be parsed by argparser or directly
      build a pipeline option.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-pipeline-options',
                        type=str,
                        action='store',
                        help='only run tests providing service options')
    parser.add_argument('--not-use-test-runner-api',
                        action='store_true',
                        default=False,
                        help='whether not to use test-runner-api')
    known, unused_argv = parser.parse_known_args(argv)

    if self.is_integration_test and not known.test_pipeline_options:
      # Skip integration test when argument '--test-pipeline-options' is not
      # specified since nose calls integration tests when runs unit test by
      # 'setup.py test'.
      raise SkipTest('IT is skipped because --test-pipeline-options '
                     'is not specified')

    self.not_use_test_runner_api = known.not_use_test_runner_api
    return shlex.split(known.test_pipeline_options) \
      if known.test_pipeline_options else []

  def get_full_options_as_args(self, **extra_opts):
    """Get full pipeline options as an argument list.

    Append extra pipeline options to existing option list if provided.
    Test verifier (if contains in extra options) should be pickled before
    appending, and will be unpickled later in the TestRunner.
    """
    options = list(self.options_list)
    for k, v in extra_opts.items():
      if not v:
        continue
      elif isinstance(v, bool) and v:
        options.append('--%s' % k)
      elif 'matcher' in k:
        options.append('--%s=%s' % (k, pickler.dumps(v).decode()))
      else:
        options.append('--%s=%s' % (k, v))
    return options

  def get_option(self, opt_name):
    """Get a pipeline option value by name

    Args:
      opt_name: The name of the pipeline option.

    Returns:
      None if option is not found in existing option list which is generated
      by parsing value of argument `test-pipeline-options`.
    """
    parser = argparse.ArgumentParser()
    opt_name = opt_name[:2] if opt_name[:2] == '--' else opt_name
    # Option name should start with '--' when it's used for parsing.
    parser.add_argument('--' + opt_name,
                        type=str,
                        action='store')
    known, _ = parser.parse_known_args(self.options_list)
    return getattr(known, opt_name) if hasattr(known, opt_name) else None
