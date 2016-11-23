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

from apache_beam.pipeline import Pipeline
from apache_beam.utils.options import PipelineOptions


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
        --test-pipeline-options="--runner=DirectPipelineRunner \
                                 --job_name=myJobName \
                                 --num_workers=1"

  For example, use assert_that for test validation::

    pipeline = TestPipeline()
    pcoll = ...
    assert_that(pcoll, equal_to(...))
    pipeline.run()
  """

  def __init__(self, runner=None, options=None, argv=None):
    if options is None:
      options = self.create_pipeline_opt_from_args()
    super(TestPipeline, self).__init__(runner, options, argv)

  def create_pipeline_opt_from_args(self):
    """Create a pipeline options from command line argument:
    --test-pipeline-options
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-pipeline-options',
                        type=str,
                        action='store',
                        help='only run tests providing service options')
    known, unused_argv = parser.parse_known_args()

    if known.test_pipeline_options:
      options = shlex.split(known.test_pipeline_options)
    else:
      options = []

    return PipelineOptions(options)
