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

"""Unit tests for flink_streaming_impulse_source."""
# pytype: skip-file

import logging
import unittest

import apache_beam as beam
from apache_beam.io.flink.flink_streaming_impulse_source import FlinkStreamingImpulseSource


class FlinkStreamingImpulseSourceTest(unittest.TestCase):
  def test_serialization(self):
    p = beam.Pipeline()
    # pylint: disable=expression-not-assigned
    p | FlinkStreamingImpulseSource()
    # Test that roundtrip through Runner API works
    beam.Pipeline.from_runner_api(p.to_runner_api(), p.runner, p._options)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
