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

import logging
import string
import unittest
from collections import Counter

import pytest

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import FlatMap
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import ParDo
from apache_beam import Reshuffle
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.parquetio import ReadAllFromParquet
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.transforms import CombineGlobally
from apache_beam.transforms.combiners import Count

try:
  import pyarrow as pa
except ImportError:
  pa = None


@unittest.skipIf(pa is None, "PyArrow is not installed.")
class TestParquetIT(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  @pytest.mark.it_postcommit
  def test_parquetio_it(self):
    file_prefix = "parquet_it_test"
    init_size = 10
    data_size = 20000
    with TestPipeline(is_integration_test=True) as p:
      pcol = self._generate_data(p, file_prefix, init_size, data_size)
      self._verify_data(pcol, init_size, data_size)

  @staticmethod
  def _sum_verifier(init_size, data_size, x):
    expected = sum(range(data_size)) * init_size
    if x != expected:
      raise BeamAssertException(
          "incorrect sum: expected(%d) actual(%d)" % (expected, x))
    return []

  @staticmethod
  def _count_verifier(init_size, data_size, x):
    name, count = x[0].decode('utf-8'), x[1]
    counter = Counter(
        [string.ascii_uppercase[x % 26] for x in range(0, data_size * 4, 4)])
    expected_count = counter[name[0]] * init_size
    if count != expected_count:
      raise BeamAssertException(
          "incorrect count(%s): expected(%d) actual(%d)" %
          (name, expected_count, count))
    return []

  def _verify_data(self, pcol, init_size, data_size):
    read = pcol | 'read' >> ReadAllFromParquet()
    v1 = (
        read
        | 'get_number' >> Map(lambda x: x['number'])
        | 'sum_globally' >> CombineGlobally(sum)
        | 'validate_number' >>
        FlatMap(lambda x: TestParquetIT._sum_verifier(init_size, data_size, x)))
    v2 = (
        read
        | 'make_pair' >> Map(lambda x: (x['name'], x['number']))
        | 'count_per_key' >> Count.PerKey()
        | 'validate_name' >> FlatMap(
            lambda x: TestParquetIT._count_verifier(init_size, data_size, x)))
    _ = ((v1, v2, pcol)
         | 'flatten' >> Flatten()
         | 'reshuffle' >> Reshuffle()
         | 'cleanup' >> Map(lambda x: FileSystems.delete([x])))

  def _generate_data(self, p, output_prefix, init_size, data_size):
    init_data = [x for x in range(init_size)]

    lines = (
        p
        | 'create' >> Create(init_data)
        | 'produce' >> ParDo(ProducerFn(data_size)))

    schema = pa.schema([('name', pa.binary()), ('number', pa.int64())])

    files = lines | 'write' >> WriteToParquet(
        output_prefix, schema, codec='snappy', file_name_suffix='.parquet')

    return files


class ProducerFn(DoFn):
  def __init__(self, number):
    super().__init__()
    self._number = number
    self._string_index = 0
    self._number_index = 0

  def process(self, element):
    self._string_index = 0
    self._number_index = 0
    for _ in range(self._number):
      yield {'name': self.get_string(4), 'number': self.get_int()}

  def get_string(self, length):
    s = []
    for _ in range(length):
      s.append(string.ascii_uppercase[self._string_index])
      self._string_index = (self._string_index + 1) % 26
    return ''.join(s)

  def get_int(self):
    i = self._number_index
    self._number_index = self._number_index + 1
    return i


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
