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

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner

__all__ = ['TestDataflowRunner']


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
      project = pipeline._options.view_as(GoogleCloudOptions).project
      region_id = pipeline._options.view_as(GoogleCloudOptions).region
      job_id = self.result.job_id()
      # TODO(markflyhigh)(BEAM-1890): Use print since Nose dosen't show logs
      # in some cases.
      print (
          'Found: https://console.cloud.google.com/dataflow/jobsDetail'
          '/locations/%s/jobs/%s?project=%s' % (region_id, job_id, project))
    self.result.wait_until_finish()

    if on_success_matcher:
      from hamcrest import assert_that as hc_assert_that
      hc_assert_that(self.result, pickler.loads(on_success_matcher))

    return self.result
