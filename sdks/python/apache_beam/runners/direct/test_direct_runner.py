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

"""Wrapper of Beam runners that's built for running and verifying e2e tests."""

# pytype: skip-file

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.runners.direct.direct_runner import DirectRunner
from apache_beam.runners.runner import PipelineState

__all__ = ['TestDirectRunner']


class TestDirectRunner(DirectRunner):
  def run_pipeline(self, pipeline, options):
    """Execute test pipeline and verify test matcher"""
    test_options = options.view_as(TestOptions)
    on_success_matcher = test_options.on_success_matcher
    is_streaming = options.view_as(StandardOptions).streaming

    # [BEAM-1889] Do not send this to remote workers also, there is no need to
    # send this option to remote executors.
    test_options.on_success_matcher = None

    self.result = super().run_pipeline(pipeline, options)

    try:
      if not is_streaming:
        self.result.wait_until_finish()

      if on_success_matcher:
        from hamcrest import assert_that as hc_assert_that
        hc_assert_that(self.result, pickler.loads(on_success_matcher))
    finally:
      if not PipelineState.is_terminal(self.result.state):
        self.result.cancel()
        self.result.wait_until_finish()

    return self.result
