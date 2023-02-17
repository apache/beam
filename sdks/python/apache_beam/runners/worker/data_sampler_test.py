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

# pytype: skip-file

import unittest

from apache_beam.coders import FastPrimitivesCoder
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders.coders import Coder
from apache_beam.runners.worker.data_sampler import DataSampler
from apache_beam.runners.worker.data_sampler import OutputSampler
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue


class DataSamplerTest(unittest.TestCase):
  def test_single_output(self):
    """Simple test for a single sample."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    output_sampler = data_sampler.sample_output('1', coder)
    output_sampler.sample('a')

    self.assertEqual(data_sampler.samples(), {'1': [coder.encode_nested('a')]})

  def test_multiple_outputs(self):
    """Tests that multiple PCollections have their own sampler."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    data_sampler.sample_output('1', coder).sample('a')
    data_sampler.sample_output('2', coder).sample('a')

    self.assertEqual(
        data_sampler.samples(), {
            '1': [coder.encode_nested('a')], '2': [coder.encode_nested('a')]
        })

  def gen_samples(self, data_sampler: DataSampler, coder: Coder):
    data_sampler.sample_output('a', coder).sample('1')
    data_sampler.sample_output('a', coder).sample('2')
    data_sampler.sample_output('b', coder).sample('3')
    data_sampler.sample_output('b', coder).sample('4')
    data_sampler.sample_output('c', coder).sample('5')
    data_sampler.sample_output('c', coder).sample('6')

  def test_sample_filters_single_pcollection_ids(self):
    """Tests the samples can be filtered based on a single pcollection id."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(pcollection_ids=['a']),
        {'a': [coder.encode_nested('1'), coder.encode_nested('2')]})

    self.assertEqual(
        data_sampler.samples(pcollection_ids=['b']),
        {'b': [coder.encode_nested('3'), coder.encode_nested('4')]})

  def test_sample_filters_multiple_pcollection_ids(self):
    """Tests the samples can be filtered based on a multiple pcollection ids."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(pcollection_ids=['a', 'c']),
        {
            'a': [coder.encode_nested('1'), coder.encode_nested('2')],
            'c': [coder.encode_nested('5'), coder.encode_nested('6')]
        })


class FakeClock:
  def __init__(self):
    self.clock = 0

  def time(self):
    return self.clock


class OutputSamplerTest(unittest.TestCase):
  def setUp(self):
    self.fake_clock = FakeClock()

  def control_time(self, new_time):
    self.fake_clock.clock = new_time

  def test_samples_first_n(self):
    """Tests that the first elements are always sampled."""
    coder = FastPrimitivesCoder()
    sampler = OutputSampler(coder)

    for i in range(15):
      sampler.sample(i)

    self.assertEqual(
        sampler.flush(), [coder.encode_nested(i) for i in range(10)])

  def test_acts_like_circular_buffer(self):
    """Tests that the buffer overwrites old samples."""
    coder = FastPrimitivesCoder()
    sampler = OutputSampler(coder, max_samples=2)

    for i in range(10):
      sampler.sample(i)

    self.assertEqual(sampler.flush(), [coder.encode_nested(i) for i in (8, 9)])

  def test_samples_every_n_secs(self):
    """Tests that the buffer overwrites old samples."""
    coder = FastPrimitivesCoder()
    sampler = OutputSampler(
        coder, max_samples=1, sample_every_sec=10, clock=self.fake_clock)

    # Always samples the first ten.
    for i in range(10):
      sampler.sample(i)
    self.assertEqual(sampler.flush(), [coder.encode_nested(9)])

    # Start at t=0
    sampler.sample(10)
    self.assertEqual(len(sampler.flush()), 0)

    # Still not over threshold yet.
    self.control_time(9)
    for i in range(100):
      sampler.sample(i)
    self.assertEqual(len(sampler.flush()), 0)

    # First sample after 10s.
    self.control_time(10)
    sampler.sample(10)
    self.assertEqual(sampler.flush(), [coder.encode_nested(10)])

    # No samples between tresholds.
    self.control_time(15)
    for i in range(100):
      sampler.sample(i)
    self.assertEqual(len(sampler.flush()), 0)

    # Second sample after 20s.
    self.control_time(20)
    sampler.sample(11)
    self.assertEqual(sampler.flush(), [coder.encode_nested(11)])

  def test_can_sample_windowed_value(self):
    """Tests that values with WindowedValueCoders are sampled wholesale."""
    data_sampler = DataSampler()
    coder = WindowedValueCoder(FastPrimitivesCoder())
    value = WindowedValue('Hello, World!', 0, [GlobalWindow()])
    data_sampler.sample_output('1', coder).sample(value)

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode_nested(value)]})

  def test_can_sample_non_windowed_value(self):
    """Tests that windowed values with WindowedValueCoders sample only the
    value.

    This is important because the Python SDK wraps all values in a WindowedValue
    even if the coder is not a WindowedValueCoder. In this case, the value must
    be retrieved from the WindowedValue to match the correct coder.
    """
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    data_sampler.sample_output('1', coder).sample(
        WindowedValue('Hello, World!', 0, [GlobalWindow()]))

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode_nested('Hello, World!')]})


if __name__ == '__main__':
  unittest.main()
