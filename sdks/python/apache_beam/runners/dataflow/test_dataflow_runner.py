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
from __future__ import print_function

import time

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
from apache_beam.runners.runner import PipelineState

__all__ = ['TestDataflowRunner']

WAIT_TIMEOUT = 2 * 60


class TestDataflowRunner(DataflowRunner):
  def run_pipeline(self, pipeline):
    """Execute test pipeline and verify test matcher"""
    options = pipeline._options.view_as(TestOptions)
    on_success_matcher = options.on_success_matcher

    # [BEAM-1889] Do not send this to remote workers also, there is no need to
    # send this option to remote executors.
    options.on_success_matcher = None

    self.result = super(TestDataflowRunner, self).run_pipeline(pipeline)
    if self.result.has_job:
      # TODO(markflyhigh)(BEAM-1890): Use print since Nose dosen't show logs
      # in some cases.
      print('Found: %s.' % self.build_console_url(pipeline.options))

    if not options.view_as(StandardOptions).streaming:
      self.result.wait_until_finish()
    else:
      self.wait_until_in_state(PipelineState.RUNNING)

    if on_success_matcher:
      from hamcrest import assert_that as hc_assert_that
      hc_assert_that(self.result, pickler.loads(on_success_matcher))

    if options.view_as(StandardOptions).streaming:
      self.result.cancel()
      self.wait_until_in_state(PipelineState.CANCELLED, timeout=300)

    return self.result

  def build_console_url(self, options):
    """Build a console url of Dataflow job."""
    project = options.view_as(GoogleCloudOptions).project
    region_id = options.view_as(GoogleCloudOptions).region
    job_id = self.result.job_id()
    return (
        'https://console.cloud.google.com/dataflow/jobsDetail/locations'
        '/%s/jobs/%s?project=%s' % (region_id, job_id, project))

  def wait_until_in_state(self, state, timeout=WAIT_TIMEOUT):
    """Wait until Dataflow pipeline terminate or enter RUNNING state."""
    if not self.result.has_job:
      raise IOError('Failed to get the Dataflow job id.')

    start_time = time.time()
    while time.time() - start_time <= timeout:
      job_state = self.result.state
      if (self.result.is_in_terminal_state() or
          job_state == PipelineState.RUNNING):
        return job_state
      time.sleep(5)

    raise RuntimeError('Timeout after %d seconds while waiting for job %s '
                       'enters RUNNING or terminate state.' %
                       (WAIT_TIMEOUT, self.result.job_id))
