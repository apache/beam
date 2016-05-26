
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for workercustomsources module."""

import logging
import unittest

from google.cloud.dataflow.internal import pickler
from google.cloud.dataflow.internal.json_value import to_json_value
from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.io import range_trackers
from google.cloud.dataflow.utils import names
from google.cloud.dataflow.worker import executor
from google.cloud.dataflow.worker import workercustomsources
from google.cloud.dataflow.worker import workitem

import google.cloud.dataflow.internal.clients.dataflow as dataflow


class TestSource(iobase.BoundedSource):

  def __init__(self, start_position, stop_position, test_range_tracker_fn):
    self._start_position = start_position
    self._stop_position = stop_position
    self._test_range_tracker_fn = test_range_tracker_fn

  def split(self, desired_bundle_size, start=None, stop=None):
    if not start:
      start = self._start_position
    if not stop:
      stop = self._stop_position

    range_start = start
    while range_start < stop:
      range_stop = min(range_start + desired_bundle_size, stop)
      yield iobase.SourceBundle(1, self, range_start, range_stop)
      range_start = range_stop

  def read(self, range_tracker):
    if not range_tracker:
      range_tracker = self.get_range_tracker(self._start_position,
                                             self._stop_position)
    val = range_tracker.start_position()
    while range_tracker.try_claim(val):
      yield val
      val += 1

  def get_range_tracker(self, start_position, stop_position):
    if self._test_range_tracker_fn:
      return self._test_range_tracker_fn()
    else:
      return range_trackers.OffsetRangeTracker(start_position, stop_position)


class WorkerCustomSourcesTest(unittest.TestCase):

  def test_native_bounded_source_read_all(self):
    source = TestSource(10, 24, None)
    splits = [split for split in source.split(5)]

    self.assertEquals(3, len(splits))

    read_data = []
    for split in splits:
      _, bundle, start, stop = split
      native_source = workercustomsources.NativeBoundedSource(
          bundle, start, stop)
      read_data.extend([val for val in native_source.reader()])

    self.assertEquals(14, len(read_data))
    self.assertItemsEqual(range(10, 24), read_data)

  def test_native_bounded_source_gets_range_tracker(self):
    def create_dummy_tracker():
      return range_trackers.OffsetRangeTracker(0, 3)

    source = TestSource(0, 20, create_dummy_tracker)

    read_data = []
    for split in source.split(5):
      _, bundle, start, stop = split
      native_source = workercustomsources.NativeBoundedSource(
          bundle, start, stop)
      read_data.extend([val for val in native_source.reader()])

    self.assertEquals(12, len(read_data))
    self.assertItemsEqual(range(0, 3) * 4, read_data)

  def build_split_proto(self, bounded_source, desired_bundle_size):
    split_proto = dataflow.SourceSplitRequest()
    split_proto.options = dataflow.SourceSplitOptions()
    split_proto.options.desiredBundleSizeBytes = desired_bundle_size

    source = dataflow.Source()
    spec = dataflow.Source.SpecValue()

    if bounded_source:
      spec.additionalProperties.append(
          dataflow.Source.SpecValue.AdditionalProperty(
              key=names.SERIALIZED_SOURCE_KEY,
              value=to_json_value({'value': pickler.dumps(bounded_source),
                                   '@type': 'http://schema.org/Text'})))
    spec.additionalProperties.append(
        dataflow.Source.SpecValue.AdditionalProperty(
            key='@type',
            value=to_json_value('CustomSourcesType')))
    source.spec = spec
    split_proto.source = source

    return split_proto

  def build_split_work_item(self, split_proto):
    lease_work_item_response_proto = dataflow.LeaseWorkItemResponse()
    work_item_proto = dataflow.WorkItem()
    lease_work_item_response_proto.workItems = [work_item_proto]
    source_operation_task = dataflow.SourceOperationRequest()
    work_item_proto.sourceOperationTask = source_operation_task
    source_operation_task.split = split_proto
    return workitem.get_work_items(lease_work_item_response_proto)

  def test_split_task_with_source_and_desired_size(self):
    test_source = TestSource(start_position=123, stop_position=456,
                             test_range_tracker_fn=None)
    split_proto = self.build_split_proto(test_source, 1234)
    split_task = workercustomsources.SourceOperationSplitTask(split_proto)
    self.assertEquals(1234, split_task.desired_bundle_size_bytes)
    self.assertIsNotNone(split_task.source)
    self.assertEquals(123, split_task.source._start_position)
    self.assertEquals(456, split_task.source._stop_position)

  def test_split_task_finds_source_no_desired_bundle_size(self):
    test_source = TestSource(start_position=123, stop_position=456,
                             test_range_tracker_fn=None)
    split_proto = self.build_split_proto(test_source, None)
    split_task = workercustomsources.SourceOperationSplitTask(split_proto)
    self.assertEquals(workercustomsources.DEFAULT_DESIRED_BUNDLE_SIZE,
                      split_task.desired_bundle_size_bytes)
    self.assertIsNotNone(split_task.source)
    self.assertEquals(123, split_task.source._start_position)
    self.assertEquals(456, split_task.source._stop_position)

  def test_split_task_finds_source_no_source_fails(self):
    split_proto = self.build_split_proto(None, 1234)
    with self.assertRaisesRegexp(
        ValueError, 'Source split spec must contain a serialized source'):
      workercustomsources.SourceOperationSplitTask(split_proto)

  def test_get_split_work_item(self):
    test_source = TestSource(start_position=123, stop_position=456,
                             test_range_tracker_fn=None)
    split_proto = self.build_split_proto(test_source, 1234)
    batch_work_item = self.build_split_work_item(split_proto)
    self.assertIsNotNone(batch_work_item.source_operation_split_task)
    self.assertEquals(
        1234,
        batch_work_item.source_operation_split_task.desired_bundle_size_bytes)
    self.assertIsNotNone(batch_work_item.source_operation_split_task.source)
    self.assertEquals(
        123,
        batch_work_item.source_operation_split_task.source._start_position)
    self.assertEquals(
        456,
        batch_work_item.source_operation_split_task.source._stop_position)

  def test_split_executor_generates_splits(self):
    test_source = TestSource(start_position=10, stop_position=30,
                             test_range_tracker_fn=None)

    split_proto = self.build_split_proto(test_source, 5)
    work_item = self.build_split_work_item(split_proto)
    split_executor = executor.CustomSourceSplitExecutor(
        work_item.source_operation_split_task)
    split_executor.execute()

    self.assertIsNotNone(split_executor.response)
    self.assertEquals(
        (dataflow.SourceSplitResponse.OutcomeValueValuesEnum.
         SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED),
        split_executor.response.split.outcome)
    bundles = split_executor.response.split.bundles
    self.assertEquals(4, len(bundles))
    for bundle in bundles:
      self.assertEquals(
          (dataflow.DerivedSource.DerivationModeValueValuesEnum.
           SOURCE_DERIVATION_MODE_INDEPENDENT),
          bundle.derivationMode)
      self.assertTrue(bundle.source.doesNotNeedSplitting)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
