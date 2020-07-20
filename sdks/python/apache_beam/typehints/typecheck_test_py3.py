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
Unit tests for typecheck.

See additional runtime_type_check=True tests in ptransform_test.py.
"""

# pytype: skip-file

from __future__ import absolute_import

import os
import tempfile
import unittest
from typing import Iterable

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import decorators

decorators._enable_from_callable = True


class MyDoFn(beam.DoFn):
  def __init__(self, output_filename):
    super().__init__()
    self.output_filename = output_filename

  def _output(self):
    """Returns a file used to record function calls."""
    if not hasattr(self, 'output_file'):
      self._output_file = open(self.output_filename, 'at', buffering=1)
    return self._output_file

  def start_bundle(self):
    self._output().write('start_bundle\n')

  def finish_bundle(self):
    self._output().write('finish_bundle\n')

  def setup(self):
    self._output().write('setup\n')

  def teardown(self):
    self._output().write('teardown\n')
    self._output().close()

  def process(self, element: int, *args, **kwargs) -> Iterable[int]:
    self._output().write('process\n')
    yield element


class MyDoFnBadAnnotation(MyDoFn):
  def process(self, element: int, *args, **kwargs) -> int:
    # Should raise an exception about return type not being iterable.
    return super().process()


class TypecheckTest(unittest.TestCase):
  def setUp(self):
    self.p = TestPipeline(options=PipelineOptions(runtime_type_check=True))

  def test_setup(self):
    # Verifies that runtime type checking is enabled for test cases.
    def fn(e: int) -> int:
      return str(e)  # type: ignore

    with self.assertRaisesRegex(beam.typehints.TypeCheckError,
                                r'output should be.*int.*received.*str'):
      _ = self.p | beam.Create([1, 2, 3]) | beam.Map(fn)
      self.p.run()

  def test_wrapper_pass_through(self):
    # We use a file to check the result because the MyDoFn instance passed is
    # not the same one that actually runs in the pipeline (it is serialized
    # here and deserialized in the worker).

    f = tempfile.NamedTemporaryFile(delete=False)
    f.close()
    try:
      dofn = MyDoFn(f.name)
      result = self.p | beam.Create([1, 2, 3]) | beam.ParDo(dofn)
      assert_that(result, equal_to([1, 2, 3]))
      self.p.run()
      with open(f.name, mode="r") as ft:
        lines = [line.strip() for line in ft]
        self.assertListEqual([
            'setup',
            'start_bundle',
            'process',
            'process',
            'process',
            'finish_bundle',
            'teardown',
        ],
                             lines)
    finally:
      if os.path.exists(f.name):
        os.remove(f.name)

  def test_wrapper_pipeline_type_check(self):
    # Verifies that type hints are not masked by the wrapper. What actually
    # happens is that the wrapper is applied during self.p.run() (not invoked
    # in this case), while pipeline type checks happen during pipeline creation.
    # Thus, the wrapper does not have to implement: default_type_hints,
    # infer_output_type, get_type_hints.
    with tempfile.NamedTemporaryFile(mode='w+t') as f:
      dofn = MyDoFnBadAnnotation(f.name)
      with self.assertRaisesRegex(ValueError, r'int.*is not iterable'):
        _ = self.p | beam.Create([1, 2, 3]) | beam.ParDo(dofn)


if __name__ == '__main__':
  unittest.main()
