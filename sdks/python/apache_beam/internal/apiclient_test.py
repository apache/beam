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
"""Unit tests for the apiclient module."""

import unittest

from apache_beam.utils.options import PipelineOptions
from apache_beam.runners.dataflow_runner import DataflowPipelineRunner
from apache_beam.internal import apiclient


class UtilTest(unittest.TestCase):

  def test_create_application_client(self):
    pipeline_options = PipelineOptions()
    apiclient.DataflowApplicationClient(
        pipeline_options,
        DataflowPipelineRunner.BATCH_ENVIRONMENT_MAJOR_VERSION)

if __name__ == '__main__':
  unittest.main()
