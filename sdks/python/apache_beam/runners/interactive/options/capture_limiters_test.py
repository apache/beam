#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import unittest

from apache_beam.portability.api.beam_interactive_api_pb2 import TestStreamFileRecord
from apache_beam.runners.interactive.options.capture_limiters import CountLimiter
from apache_beam.runners.interactive.options.capture_limiters import ProcessingTimeLimiter


class CaptureLimitersTest(unittest.TestCase):
  def test_count_limiter(self):
    limiter = CountLimiter(5)

    for e in range(4):
      limiter.update(e)

    self.assertFalse(limiter.is_triggered())
    limiter.update(5)
    self.assertTrue(limiter.is_triggered())

  def test_processing_time_limiter(self):
    limiter = ProcessingTimeLimiter(max_duration_secs=2)

    r = TestStreamFileRecord()
    r.recorded_event.processing_time_event.advance_duration = int(1 * 1e6)
    limiter.update(r)
    self.assertFalse(limiter.is_triggered())

    r = TestStreamFileRecord()
    r.recorded_event.processing_time_event.advance_duration = int(2 * 1e6)
    limiter.update(r)
    self.assertTrue(limiter.is_triggered())


if __name__ == '__main__':
  unittest.main()
