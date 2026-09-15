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

"""Tests for operations.py, specifically PGBKCVOperation memory bounding."""

# pytype: skip-file

import unittest

from apache_beam.internal import pickler
from apache_beam.runners import common
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.utils.windowed_value import WindowedValue


class ListCombineFn(core.CombineFn):
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, input_val):
    accumulator.append(input_val)
    return accumulator

  def merge_accumulators(self, accumulators):
    res = []
    for a in accumulators:
      res.extend(a)
    return res

  def extract_output(self, accumulator):
    return accumulator


class MockOutputReceiver(common.Receiver):
  def __init__(self):
    self.output_values = []

  def receive(self, windowed_value):
    self.output_values.append(windowed_value)


class PGBKCVOperationTest(unittest.TestCase):
  def _create_operation(self, combine_fn, max_bytes=None, max_keys=None):
    spec = operation_specs.WorkerPartialGroupByKey(
        combine_fn=pickler.dumps((combine_fn, [], {})),
        input=None,
        output_coders=[None])
    op = operations.PGBKCVOperation(
        name_context=common.NameContext('test_step'),
        spec=spec,
        counter_factory=None,
        state_sampler=None,
        max_bytes=max_bytes,
        max_keys=max_keys)
    receiver = MockOutputReceiver()
    op.add_receiver(receiver, 0)
    op.setup()
    op.is_default_windowing = True
    return op, receiver

  def test_pgbkcv_max_bytes_bounding(self):
    max_bytes = 2000
    max_keys = 100000
    op, receiver = self._create_operation(
        ListCombineFn(), max_bytes=max_bytes, max_keys=max_keys)

    large_payload = 'x' * 500
    for i in range(100):
      key = f'key_{i}'
      wv = WindowedValue((key, large_payload), 0, ())
      op.process(wv)
      if len(receiver.output_values) > 0:
        break

    self.assertTrue(
        len(receiver.output_values) > 0,
        'Operation should flush due to max_bytes memory limit being exceeded')
    self.assertLess(
        op.key_count, max_keys,
        'Eviction should occur well before max_keys limit')

  def test_pgbkcv_max_keys_bounding(self):
    max_keys = 10
    max_bytes = 10 * 1024 * 1024
    op, receiver = self._create_operation(
        combiners.CountCombineFn(), max_bytes=max_bytes, max_keys=max_keys)

    for i in range(15):
      wv = WindowedValue((f'key_{i}', 1), 0, ())
      op.process(wv)

    self.assertTrue(
        len(receiver.output_values) > 0,
        'Operation should flush when key_count reaches max_keys')

  def test_pgbkcv_finish_outputs_all_remaining(self):
    op, receiver = self._create_operation(
        combiners.CountCombineFn(), max_bytes=100000, max_keys=1000)

    keys = ['a', 'b', 'c', 'a', 'b']
    for k in keys:
      op.process(WindowedValue((k, 1), 0, ()))

    op.finish()

    output_map = {wv.value[0]: wv.value[1] for wv in receiver.output_values}
    self.assertEqual(output_map['a'], 2)
    self.assertEqual(output_map['b'], 2)
    self.assertEqual(output_map['c'], 1)

  def test_custom_parameters(self):
    op, _ = self._create_operation(
        ListCombineFn(), max_bytes=12345, max_keys=678)
    self.assertEqual(op.max_bytes, 12345)
    self.assertEqual(op.max_keys, 678)


if __name__ == '__main__':
  unittest.main()
