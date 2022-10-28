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
from typing import Tuple
from typing import no_type_check

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


class BatchDoFnOverrideTypeInference(beam.DoFn):
  def process_batch(self, batch, *args, **kwargs):
    yield [element * 2 for element in batch]

  def get_input_batch_type(self, input_element_type):
    return List[input_element_type]

  def get_output_batch_type(self, input_element_type):
    return List[input_element_type]


class EitherDoFn(beam.DoFn):
  def process(self, element: int, *args, **kwargs) -> Iterator[float]:
    yield element / 2

  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[List[float]]:
    yield [element / 2 for element in batch]


class ElementToBatchDoFn(beam.DoFn):
  @beam.DoFn.yields_batches
  def process(self, element: int, *args, **kwargs) -> Iterator[List[int]]:
    yield [element] * element

  def infer_output_type(self, input_element_type):
    return input_element_type


class BatchToElementDoFn(beam.DoFn):
  @beam.DoFn.yields_elements
  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[Tuple[int, int]]:
    yield (sum(batch), len(batch))


def get_test_class_name(cls, num, params_dict):
  return "%s_%s" % (cls.__name__, params_dict['dofn'].__class__.__name__)


@parameterized_class([
    {
        "dofn": ElementDoFn(),
        "input_element_type": int,
        "expected_process_defined": True,
        "expected_process_batch_defined": False,
        "expected_input_batch_type": None,
        "expected_output_batch_type": None
    },
    {
        "dofn": BatchDoFn(),
        "input_element_type": int,
        "expected_process_defined": False,
        "expected_process_batch_defined": True,
        "expected_input_batch_type": beam.typehints.List[int],
        "expected_output_batch_type": beam.typehints.List[float]
    },
    {
        "dofn": BatchDoFnNoReturnAnnotation(),
        "input_element_type": int,
        "expected_process_defined": False,
        "expected_process_batch_defined": True,
        "expected_input_batch_type": beam.typehints.List[int],
        "expected_output_batch_type": beam.typehints.List[int]
    },
    {
        "dofn": BatchDoFnOverrideTypeInference(),
        "input_element_type": int,
        "expected_process_defined": False,
        "expected_process_batch_defined": True,
        "expected_input_batch_type": beam.typehints.List[int],
        "expected_output_batch_type": beam.typehints.List[int]
    },
    {
        "dofn": EitherDoFn(),
        "input_element_type": int,
        "expected_process_defined": True,
        "expected_process_batch_defined": True,
        "expected_input_batch_type": beam.typehints.List[int],
        "expected_output_batch_type": beam.typehints.List[float]
    },
    {
        "dofn": ElementToBatchDoFn(),
        "input_element_type": int,
        "expected_process_defined": True,
        "expected_process_batch_defined": False,
        "expected_input_batch_type": None,
        "expected_output_batch_type": beam.typehints.List[int]
    },
    {
        "dofn": BatchToElementDoFn(),
        "input_element_type": int,
        "expected_process_defined": False,
        "expected_process_batch_defined": True,
        "expected_input_batch_type": beam.typehints.List[int],
        "expected_output_batch_type": None,
    },
],
                     class_name_func=get_test_class_name)
class BatchDoFnParameterizedTest(unittest.TestCase):
  def test_process_defined(self):
    self.assertEqual(self.dofn._process_defined, self.expected_process_defined)

  def test_process_batch_defined(self):
    self.assertEqual(
        self.dofn._process_batch_defined, self.expected_process_batch_defined)

  def test_get_input_batch_type(self):
    self.assertEqual(
        self.dofn._get_input_batch_type_normalized(self.input_element_type),
        self.expected_input_batch_type)

  def test_get_output_batch_type(self):
    self.assertEqual(
        self.dofn._get_output_batch_type_normalized(self.input_element_type),
        self.expected_output_batch_type)

  def test_can_yield_batches(self):
    expected = self.expected_output_batch_type is not None
    self.assertEqual(self.dofn._can_yield_batches, expected)


class BatchDoFnNoInputAnnotation(beam.DoFn):
  def process_batch(self, batch, *args, **kwargs):
    yield [element * 2 for element in batch]


class MismatchedBatchProducingDoFn(beam.DoFn):
  """A DoFn that produces batches from both process and process_batch, with
  mismatched return types (one yields floats, the other ints). Should yield
  a construction time error when applied."""
  @beam.DoFn.yields_batches
  def process(self, element: int, *args, **kwargs) -> Iterator[List[int]]:
    yield [element]

  def process_batch(self, batch: List[int], *args,
                    **kwargs) -> Iterator[List[float]]:
    yield [element / 2 for element in batch]


class MismatchedElementProducingDoFn(beam.DoFn):
  """A DoFn that produces elements from both process and process_batch, with
  mismatched return types (one yields floats, the other ints). Should yield
  a construction time error when applied."""
  def process(self, element: int, *args, **kwargs) -> Iterator[float]:
    yield element / 2

  @beam.DoFn.yields_elements
  def process_batch(self, batch: List[int], *args, **kwargs) -> Iterator[int]:
    yield batch[0]


class BatchDoFnTest(unittest.TestCase):
  def test_map_pardo(self):
    # verify batch dofn accessors work well with beam.Map generated DoFn
    # checking this in parameterized test causes a circular reference issue
    dofn = beam.Map(lambda x: x * 2).dofn

    self.assertTrue(dofn._process_defined)
    self.assertFalse(dofn._process_batch_defined)
    self.assertEqual(dofn._get_input_batch_type_normalized(int), None)
    self.assertEqual(dofn._get_output_batch_type_normalized(int), None)

  def test_no_input_annotation_raises(self):
    p = beam.Pipeline()
    pc = p | beam.Create([1, 2, 3])

    with self.assertRaisesRegex(TypeError,
                                r'BatchDoFnNoInputAnnotation.process_batch'):
      _ = pc | beam.ParDo(BatchDoFnNoInputAnnotation())

  def test_unsupported_dofn_param_raises(self):
    class BatchDoFnBadParam(beam.DoFn):
      @no_type_check
      def process_batch(self, batch: List[int], key=beam.DoFn.KeyParam):
        yield batch * key

    p = beam.Pipeline()
    pc = p | beam.Create([1, 2, 3])

    with self.assertRaisesRegex(NotImplementedError,
                                r'BatchDoFnBadParam.*KeyParam'):
      _ = pc | beam.ParDo(BatchDoFnBadParam())

  def test_mismatched_batch_producer_raises(self):
    p = beam.Pipeline()
    pc = p | beam.Create([1, 2, 3])

    # Note (?ms) makes this a multiline regex, where . matches newlines.
    # See (?aiLmsux) at
    # https://docs.python.org/3.4/library/re.html#regular-expression-syntax
    with self.assertRaisesRegex(
        TypeError,
        (r'(?ms)MismatchedBatchProducingDoFn.*'
         r'process: List\[<class \'int\'>\].*process_batch: '
         r'List\[<class \'float\'>\]')):
      _ = pc | beam.ParDo(MismatchedBatchProducingDoFn())

  def test_mismatched_element_producer_raises(self):
    p = beam.Pipeline()
    pc = p | beam.Create([1, 2, 3])

    # Note (?ms) makes this a multiline regex, where . matches newlines.
    # See (?aiLmsux) at
    # https://docs.python.org/3.4/library/re.html#regular-expression-syntax
    with self.assertRaisesRegex(
        TypeError,
        r'(?ms)MismatchedElementProducingDoFn.*process:.*process_batch:'):
      _ = pc | beam.ParDo(MismatchedElementProducingDoFn())

  def test_element_to_batch_dofn_typehint(self):
    # Verify that element to batch DoFn sets the correct typehint on the output
    # PCollection.

    p = beam.Pipeline()
    pc = (p | beam.Create([1, 2, 3]) | beam.ParDo(ElementToBatchDoFn()))

    self.assertEqual(pc.element_type, int)

  def test_batch_to_element_dofn_typehint(self):
    # Verify that batch to element DoFn sets the correct typehint on the output
    # PCollection.

    p = beam.Pipeline()
    pc = (p | beam.Create([1, 2, 3]) | beam.ParDo(BatchToElementDoFn()))

    self.assertEqual(pc.element_type, beam.typehints.Tuple[int, int])


if __name__ == '__main__':
  unittest.main()
