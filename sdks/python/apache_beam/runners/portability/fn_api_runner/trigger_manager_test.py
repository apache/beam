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

from apache_beam import Map
from apache_beam import WindowInto
from apache_beam.runners.portability.fn_api_runner import trigger_manager
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import Windowing
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import SlidingWindows


class TriggerManagerTest(unittest.TestCase):
  def test_fixed_windows_simple_watermark(self):
    # yapf: disable
    test_stream = (
        TestStream()
          .advance_watermark_to(0)
          .add_elements([('k1', 1), ('k2', 1), ('k1', 1), ('k2', 1)])
          .add_elements([('k1', 1), ('k2', 1)])
          .advance_watermark_to(1)
          .add_elements([('k1', 2), ('k2', 2)])
          .add_elements([('k1', 2), ('k2', 2)])
          .advance_watermark_to_infinity())
    # yapf: enable

    # Fixed, one-second windows with DefaultTrigger (after watermark)
    windowing = Windowing(FixedWindows(1))

    with TestPipeline() as p:
      result = (
          p
          | test_stream
          | WindowInto(windowing.windowfn)
          | ParDo(trigger_manager.ReifyWindows())
          | ParDo(trigger_manager._GroupBundlesByKey())
          | ParDo(trigger_manager.GeneralTriggerManagerDoFn(windowing))
          | Map(
              lambda elm:
              (elm[0], elm[1][0].windows[0], [v.value for v in elm[1]])))
      assert_that(
          result,
          equal_to([
              ('k1', IntervalWindow(0, 1), [1, 1, 1]),
              ('k2', IntervalWindow(0, 1), [1, 1, 1]),
              ('k1', IntervalWindow(1, 2), [2, 2]),
              ('k2', IntervalWindow(1, 2), [2, 2]),
          ]))

  def test_sliding_windows_simple_watermark(self):
    # yapf: disable
    test_stream = (
        TestStream()
          .advance_watermark_to(0)
          .add_elements([('k1', 1), ('k2', 1), ('k1', 1), ('k2', 1)])
          .add_elements([('k1', 1), ('k2', 1)])
          .advance_watermark_to(1)
          .add_elements([('k1', 2), ('k2', 2)])
          .add_elements([('k1', 2), ('k2', 2)])
          .advance_watermark_to(2)
          .add_elements([('k1', 3), ('k2', 3)])
          .add_elements([('k1', 3), ('k2', 3)])
          .advance_watermark_to_infinity())
    # yapf: enable

    # Fixed, one-second windows with DefaultTrigger (after watermark)
    windowing = Windowing(SlidingWindows(2, 1))

    with TestPipeline() as p:
      result = (
          p
          | test_stream
          | WindowInto(windowing.windowfn)
          | ParDo(trigger_manager.ReifyWindows())
          | ParDo(trigger_manager._GroupBundlesByKey())
          | ParDo(trigger_manager.GeneralTriggerManagerDoFn(windowing))
          | Map(
          lambda elm:
          (elm[0], elm[1][0].windows[0], [v.value for v in elm[1]])))
      assert_that(
          result,
          equal_to([
              ('k1', IntervalWindow(-1, 1), [1, 1, 1]),
              ('k2', IntervalWindow(-1, 1), [1, 1, 1]),
              ('k1', IntervalWindow(0, 2), [1, 1, 1, 2, 2]),
              ('k2', IntervalWindow(0, 2), [1, 1, 1, 2, 2]),
              ('k1', IntervalWindow(1, 3), [2, 2, 3, 3]),
              ('k2', IntervalWindow(1, 3), [2, 2, 3, 3]),
              ('k1', IntervalWindow(2, 4), [3, 3]),
              ('k2', IntervalWindow(2, 4), [3, 3]),
          ]))
