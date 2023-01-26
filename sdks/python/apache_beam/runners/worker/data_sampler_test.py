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
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue


class DataSamplerTest(unittest.TestCase):
  def test_single_output(self):
    """Simple test for a single sample."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    output_sampler = data_sampler.sample_output('a', '1', coder)
    output_sampler.sample('a')

    self.assertEqual(data_sampler.samples(), {'1': [coder.encode('a')]})

  def test_multiple_outputs(self):
    """Tests that multiple PCollections have their own sampler."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    data_sampler.sample_output('a', '1', coder).sample('a')
    data_sampler.sample_output('a', '2', coder).sample('a')

    self.assertEqual(
        data_sampler.samples(), {
            '1': [coder.encode('a')], '2': [coder.encode('a')]
        })

  def test_multiple_descriptors(self):
    """Tests that multiple PBDs sample into the same PCollection."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    data_sampler.sample_output('a', '1', coder).sample('a')
    data_sampler.sample_output('b', '1', coder).sample('b')

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode('a'), coder.encode('b')]})

  def gen_samples(self, data_sampler: DataSampler, coder: Coder):
    data_sampler.clear()
    data_sampler.sample_output('a', '1', coder).sample('a1')
    data_sampler.sample_output('a', '2', coder).sample('a2')
    data_sampler.sample_output('b', '1', coder).sample('b1')
    data_sampler.sample_output('b', '2', coder).sample('b2')

  def test_sample_filters_single_descriptor_ids(self):
    """Tests the samples can be filtered based on a single descriptor id."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['a']), {
            '1': [coder.encode('a1')], '2': [coder.encode('a2')]
        })

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['b']), {
            '1': [coder.encode('b1')], '2': [coder.encode('b2')]
        })

  def test_sample_filters_single_pcollection_ids(self):
    """Tests the samples can be filtered based on a single pcollection id."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(pcollection_ids=['1']),
        {'1': [coder.encode('a1'), coder.encode('b1')]})

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(pcollection_ids=['2']),
        {'2': [coder.encode('a2'), coder.encode('b2')]})

  def test_sample_filters_descriptor_and_pcollection_ids(self):
    """Tests the samples can be filtered based on a both ids."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['a'], pcollection_ids=['1']),
        {'1': [coder.encode('a1')]})

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['a'], pcollection_ids=['2']),
        {'2': [coder.encode('a2')]})

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['b'], pcollection_ids=['1']),
        {'1': [coder.encode('b1')]})

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['b'], pcollection_ids=['2']),
        {'2': [coder.encode('b2')]})

  def test_sample_filters_multiple_descriptor_ids(self):
    """Tests the samples are unioned based on descriptor ids."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(descriptor_ids=['a', 'b']),
        {
            '1': [coder.encode('a1'), coder.encode('b1')],
            '2': [coder.encode('a2'), coder.encode('b2')]
        })


class OutputSamplerTest(unittest.TestCase):
  def test_samples_first_n(self):
    """Tests that the first elements are always sampled."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    sampler = data_sampler.sample_output('a', '1', coder)

    for i in range(15):
      sampler.sample(i)

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode(i) for i in range(10)]})

  def test_acts_like_circular_buffer(self):
    """Tests that the buffer overwrites old samples."""
    data_sampler = DataSampler(max_samples=5, sample_every_n=20)
    coder = FastPrimitivesCoder()
    sampler = data_sampler.sample_output('a', '1', coder)

    for i in range(100):
      sampler.sample(i)

    self.assertEqual(
        data_sampler.samples(),
        {'1': [coder.encode(i) for i in (19, 39, 59, 79, 99)]})

  def test_can_sample_windowed_value(self):
    """Tests that values with WindowedValueCoders are sampled wholesale."""
    data_sampler = DataSampler()
    coder = WindowedValueCoder(FastPrimitivesCoder())
    value = WindowedValue('Hello, World!', 0, [GlobalWindow()])
    data_sampler.sample_output('a', '1', coder).sample(value)

    self.assertEqual(data_sampler.samples(), {'1': [coder.encode(value)]})

  def test_can_sample_non_windowed_value(self):
    """Tests that windowed values with WindowedValueCoders sample only the
    value.

    This is important because the Python SDK wraps all values in a WindowedValue
    even if the coder is not a WindowedValueCoder. In this case, the value must
    be retrieved from the WindowedValue to match the correct coder.
    """
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    data_sampler.sample_output('a', '1', coder).sample(
        WindowedValue('Hello, World!', 0, [GlobalWindow()]))

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode('Hello, World!')]})


if __name__ == '__main__':
  unittest.main()
