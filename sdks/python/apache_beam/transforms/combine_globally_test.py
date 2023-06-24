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

"""Unit tests for our libraries of combine PTransforms."""
# pytype: skip-file

import time
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse


class CombineGloballyTest(unittest.TestCase):
  def test_combine_globally_for_unbounded_source_with_default(self):
    # this error is expected since the below combination is ill-defined.
    with self.assertRaises(ValueError):
      with TestPipeline() as p:
        _ = (
            p
            | PeriodicImpulse(
                start_timestamp=time.time(),
                stop_timestamp=time.time() + 4,
                fire_interval=1,
                apply_windowing=False,
            )
            | beam.Map(lambda x: ('c', 1))
            | beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterCount(2)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | beam.combiners.Count.Globally())

  def test_combine_globally_for_unbounded_source_without_defaults(self):
    # this is the supported case
    with TestPipeline() as p:
      _ = (
          p
          | PeriodicImpulse(
              start_timestamp=time.time(),
              stop_timestamp=time.time() + 4,
              fire_interval=1,
              apply_windowing=False,
          )
          | beam.Map(lambda x: 1)
          | beam.WindowInto(
              window.GlobalWindows(),
              trigger=trigger.Repeatedly(trigger.AfterCount(2)),
              accumulation_mode=trigger.AccumulationMode.DISCARDING,
          )
          | beam.CombineGlobally(sum).without_defaults())


if __name__ == '__main__':
  unittest.main()
