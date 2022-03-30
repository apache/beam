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
from typing import NamedTuple
from typing import Optional

from parameterized import parameterized_class

import apache_beam as beam


class ElementDoFn(beam.DoFn):
  def process(self, element: int, *args, **kwargs) -> Iterator[int]:
    yield element


class BatchDoFn(beam.DoFn):
  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[List[int]]:
    yield [element * 2 for element in batch]


class BatchDoFnNoReturnAnnotation(beam.DoFn):
  def process_batch(self, batch: List[int], *args, **kwargs):
    yield [element * 2 for element in batch]


class EitherDoFn(beam.DoFn):
  def process(self, element: int, *args, **kwargs) -> Iterator[int]:
    yield element

  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[List[int]]:
    yield [element * 2 for element in batch]


class BatchDoFnTestCase(NamedTuple):
  dofn: beam.DoFn
  process_defined: bool
  process_batch_defined: bool
  input_batch_type: Optional[type]
  output_batch_type: Optional[type]


def make_map_pardo():
  return beam.Map(lambda x: x * 2).dofn


@parameterized_class(
    BatchDoFnTestCase.__annotations__.keys(),
    [
        BatchDoFnTestCase(
            dofn=ElementDoFn(),
            process_defined=True,
            process_batch_defined=False,
            input_batch_type=None,
            output_batch_type=None),
        BatchDoFnTestCase(
            dofn=BatchDoFn(),
            process_defined=False,
            process_batch_defined=True,
            input_batch_type=beam.typehints.List[int],
            output_batch_type=beam.typehints.List[int]),
        BatchDoFnTestCase(
            dofn=BatchDoFnNoReturnAnnotation(),
            process_defined=False,
            process_batch_defined=True,
            input_batch_type=beam.typehints.List[int],
            output_batch_type=beam.typehints.List[int]),
        BatchDoFnTestCase(
            dofn=EitherDoFn(),
            process_defined=True,
            process_batch_defined=True,
            input_batch_type=beam.typehints.List[int],
            output_batch_type=beam.typehints.List[int]),
        #BatchDoFnTestCase(
        #    dofn=make_map_pardo().dofn,
        #    process_defined=True,
        #    process_batch_defined=False,
        #    input_batch_type=None,
        #    batch_output_type=None),
    ],
    class_name_func=lambda _,
    __,
    params: params['dofn'].__class__.__name__)
class BatchDoFnTest(unittest.TestCase):
  def test_process_defined(self):
    self.assertEqual(self.dofn.process_defined, self.process_defined)

  def test_process_batch_defined(self):
    self.assertEqual(
        self.dofn.process_batch_defined, self.process_batch_defined)

  def test_get_input_batch_type(self):
    self.assertEqual(self.dofn.get_input_batch_type(), self.input_batch_type)

  def test_get_output_batch_type(self):
    self.assertEqual(self.dofn.get_output_batch_type(), self.output_batch_type)


if __name__ == '__main__':
  unittest.main()
