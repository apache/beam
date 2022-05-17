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

"""UnitTests for Batched DoFn (process_batch) API."""

# pytype: skip-file

import unittest
from typing import Iterator
from typing import List

from parameterized import parameterized_class

import apache_beam as beam


class ElementDoFn(beam.DoFn):
  def process(self, element: int, *args, **kwargs) -> Iterator[float]:
    yield element / 2


class BatchDoFn(beam.DoFn):
  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[List[float]]:
    yield [element / 2 for element in batch]


class BatchDoFnNoReturnAnnotation(beam.DoFn):
  def process_batch(self, batch: List[int], *args, **kwargs):
    yield [element * 2 for element in batch]


class EitherDoFn(beam.DoFn):
  def process(self, element: int, *args, **kwargs) -> Iterator[float]:
    yield element / 2

  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[List[float]]:
    yield [element / 2 for element in batch]


def get_test_class_name(cls, num, params_dict):
  return "%s_%s" % (cls.__name__, params_dict['dofn'].__class__.__name__)


@parameterized_class([
    {
        "dofn": ElementDoFn(),
        "process_defined": True,
        "process_batch_defined": False,
        "input_batch_type": None,
        "output_batch_type": None
    },
    {
        "dofn": BatchDoFn(),
        "process_defined": False,
        "process_batch_defined": True,
        "input_batch_type": beam.typehints.List[int],
        "output_batch_type": beam.typehints.List[float]
    },
    {
        "dofn": BatchDoFnNoReturnAnnotation(),
        "process_defined": False,
        "process_batch_defined": True,
        "input_batch_type": beam.typehints.List[int],
        "output_batch_type": beam.typehints.List[int]
    },
    {
        "dofn": EitherDoFn(),
        "process_defined": True,
        "process_batch_defined": True,
        "input_batch_type": beam.typehints.List[int],
        "output_batch_type": beam.typehints.List[float]
    },
],
                     class_name_func=get_test_class_name)
class BatchDoFnParameterizedTest(unittest.TestCase):
  def test_process_defined(self):
    self.assertEqual(self.dofn.process_defined, self.process_defined)

  def test_process_batch_defined(self):
    self.assertEqual(
        self.dofn.process_batch_defined, self.process_batch_defined)

  def test_get_input_batch_type(self):
    self.assertEqual(self.dofn.get_input_batch_type(), self.input_batch_type)

  def test_get_output_batch_type(self):
    self.assertEqual(self.dofn.get_output_batch_type(), self.output_batch_type)


class BatchDoFnNoInputAnnotation(beam.DoFn):
  def process_batch(self, batch, *args, **kwargs):
    yield [element * 2 for element in batch]


class BatchDoFnTest(unittest.TestCase):
  def test_map_pardo(self):
    # verify batch dofn accessors work well with beam.Map generated DoFn
    # checking this in parameterized test causes a circular reference issue
    dofn = beam.Map(lambda x: x * 2).dofn

    self.assertTrue(dofn.process_defined)
    self.assertFalse(dofn.process_batch_defined)
    self.assertEqual(dofn.get_input_batch_type(), None)
    self.assertEqual(dofn.get_output_batch_type(), None)

  def test_no_input_annotation_raises(self):
    p = beam.Pipeline()
    pc = p | beam.Create([1, 2, 3])

    with self.assertRaisesRegex(TypeError,
                                r'BatchDoFnNoInputAnnotation.process_batch'):
      _ = pc | beam.ParDo(BatchDoFnNoInputAnnotation())


if __name__ == '__main__':
  unittest.main()
