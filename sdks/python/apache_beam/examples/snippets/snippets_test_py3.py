# coding=utf-8
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

"""
Tests for all code snippets used in public docs, using Python 3 specific
syntax.
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import logging
import unittest

import apache_beam as beam
from apache_beam import typehints
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline


class TypeHintsTest(unittest.TestCase):
  def test_bad_types_annotations(self):
    p = TestPipeline(options=PipelineOptions(pipeline_type_check=True))

    numbers = p | beam.Create(['1', '2', '3'])

    # Consider the following code.
    # pylint: disable=expression-not-assigned
    # pylint: disable=unused-variable
    class FilterEvensDoFn(beam.DoFn):
      def process(self, element):
        if element % 2 == 0:
          yield element

    evens = numbers | 'Untyped Filter' >> beam.ParDo(FilterEvensDoFn())

    # Now suppose numbers was defined as [snippet above].
    # When running this pipeline, you'd get a runtime error,
    # possibly on a remote machine, possibly very late.

    with self.assertRaises(TypeError):
      p.run()

    # To catch this early, we can annotate process() with the expected types.
    # Beam will then use these as type hints and perform type checking before
    # the pipeline starts.
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_do_fn_annotations]
      from typing import Iterable

      class FilterEvensDoFn(beam.DoFn):
        def process(self, element: int) -> Iterable[int]:
          if element % 2 == 0:
            yield element

      evens = numbers | 'filter_evens' >> beam.ParDo(FilterEvensDoFn())
      # [END type_hints_do_fn_annotations]

    # Another example, using a list output type. Notice that the output
    # annotation has an additional Optional for the else clause.
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_do_fn_annotations_optional]
      from typing import List, Optional

      class FilterEvensDoubleDoFn(beam.DoFn):
        def process(self, element: int) -> Optional[List[int]]:
          if element % 2 == 0:
            return [element, element]
          return None

      evens = numbers | 'double_evens' >> beam.ParDo(FilterEvensDoubleDoFn())
      # [END type_hints_do_fn_annotations_optional]

    # Example using an annotated function.
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_map_annotations]
      def my_fn(element: int) -> str:
        return 'id_' + str(element)

      ids = numbers | 'to_id' >> beam.Map(my_fn)
      # [END type_hints_map_annotations]

    # Example using an annotated PTransform.
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_ptransforms]
      from apache_beam.pvalue import PCollection

      class IntToStr(beam.PTransform):
        def expand(self, pcoll: PCollection[int]) -> PCollection[str]:
          return pcoll | beam.Map(lambda elem: str(elem))

      ids = numbers | 'convert to str' >> IntToStr()
      # [END type_hints_ptransforms]


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
