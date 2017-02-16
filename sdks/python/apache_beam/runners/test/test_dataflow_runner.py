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

from apache_beam.internal import pickler
from apache_beam.runners.google_cloud_dataflow.dataflow_runner import DataflowRunner
from apache_beam.utils.pipeline_options import TestOptions


class TestDataflowRunner(DataflowRunner):

  def __init__(self):
    super(TestDataflowRunner, self).__init__()

  def run(self, pipeline):
    """Execute test pipeline and verify test matcher"""
    self.result = super(TestDataflowRunner, self).run(pipeline)
    self.result.wait_until_finish()

    options = pipeline.options.view_as(TestOptions)
    if options.on_success_matcher:
      from hamcrest import assert_that as hc_assert_that
      hc_assert_that(self.result, pickler.loads(options.on_success_matcher))

    return self.result
