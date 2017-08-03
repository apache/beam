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

"""Unit tests for the Executors"""

import imp
import unittest
import mock
import tenacity

import apache_beam as beam


class RetryTest(unittest.TestCase):

  def setUp(self):
    self.counter = 0

    def after_counter(func, trial_number, trial_time_taken):
      """After call strategy that does nothing."""
      self.counter += 1

    mock.patch.object(
        tenacity.after,
        'after_nothing',
        side_effect=after_counter).start()
    imp.reload(tenacity)

  def tearDown(self):
    mock.patch.stopall()
    imp.reload(tenacity)

  def test_pipeline_retrying_transform(self):
    def failure(x):
      raise ValueError("Boom!!")

    p = beam.Pipeline('DirectRunner')
    _ = p | beam.Create([1]) | beam.Map(failure)

    try:
      p.run().wait_until_finish()
      self.fail('Pipeline should have failed for the unittest')
    except Exception:
      self.assertEquals(self.counter, 3)
