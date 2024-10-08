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
import unittest

from google.protobuf import duration_pb2
from google.protobuf import timestamp_pb2

from apache_beam.utils import proto_utils
from apache_beam.utils.timestamp import MAX_TIMESTAMP


class TestProtoUtils(unittest.TestCase):
  def test_from_micros_duration(self):
    ts = proto_utils.from_micros(duration_pb2.Duration, MAX_TIMESTAMP.micros)
    expected = duration_pb2.Duration(
        seconds=MAX_TIMESTAMP.seconds(), nanos=775000000)
    self.assertEqual(ts, expected)

  def test_from_micros_timestamp(self):
    ts = proto_utils.from_micros(timestamp_pb2.Timestamp, MAX_TIMESTAMP.micros)
    expected = timestamp_pb2.Timestamp(
        seconds=MAX_TIMESTAMP.seconds(), nanos=775000000)
    self.assertEqual(ts, expected)

  def test_to_micros_duration(self):
    dur = duration_pb2.Duration(seconds=MAX_TIMESTAMP.seconds(), nanos=775000000)
    ts = proto_utils.to_micros(dur)
    expected = MAX_TIMESTAMP.micros
    self.assertEqual(ts, expected)
  
  def test_to_micros_timestamp(self):
    dur = timestamp_pb2.Timestamp(seconds=MAX_TIMESTAMP.seconds(), nanos=775000000)
    ts = proto_utils.to_micros(dur)
    expected = MAX_TIMESTAMP.micros
    self.assertEqual(ts, expected)

  def test_round_trip_duration(self):
    expected = 919336704
    dur = proto_utils.from_micros(duration_pb2.Duration, expected)
    ms = proto_utils.to_micros(dur)
    self.assertEqual(ms, expected)
  
  def test_round_trip_timestamp(self):
    expected = 919336704
    ts = proto_utils.from_micros(timestamp_pb2.Timestamp, expected)
    ms = proto_utils.to_micros(ts)
    self.assertEqual(ms, expected)
  

if __name__ == '__main__':
  unittest.main()
