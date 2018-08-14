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

from __future__ import absolute_import

import threading
import unittest

import apache_beam as beam
from apache_beam.runners import DirectRunner
from apache_beam.runners import TestDirectRunner
from apache_beam.runners import create_runner
from apache_beam.testing import test_pipeline


class DirectPipelineResultTest(unittest.TestCase):

  def test_waiting_on_result_stops_executor_threads(self):
    pre_test_threads = set(t.ident for t in threading.enumerate())

    for runner in ['DirectRunner', 'BundleBasedDirectRunner',
                   'SwitchingDirectRunner']:
      pipeline = test_pipeline.TestPipeline(runner=runner)
      _ = (pipeline | beam.Create([{'foo': 'bar'}]))
      result = pipeline.run()
      result.wait_until_finish()

      post_test_threads = set(t.ident for t in threading.enumerate())
      new_threads = post_test_threads - pre_test_threads
      self.assertEqual(len(new_threads), 0)

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DirectRunner'),
                   DirectRunner))
    self.assertTrue(
        isinstance(create_runner('TestDirectRunner'),
                   TestDirectRunner))


if __name__ == '__main__':
  unittest.main()
