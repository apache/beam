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

import logging
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import error_handling


class PTransformWithErrors(beam.PTransform):
  def __init__(self, limit):
    self._limit = limit
    self._error_handler = None

  def with_error_handler(self, error_handler):
    self._error_handler = error_handler
    return self

  def expand(self, pcoll):
    limit = self._limit

    def process(element):
      if len(element) < limit:
        return element.title()
      else:
        return beam.pvalue.TaggedOutput('bad', element)

    def raise_on_everything(element):
      raise ValueError(element)

    good, bad = pcoll | beam.Map(process).with_outputs('bad', main='good')
    if self._error_handler:
      self._error_handler.add_error_pcollection(bad)
    else:
      # Will throw an exception if there are any bad elements.
      _ = bad | beam.Map(raise_on_everything)
    return good


def exception_throwing_map(x, limit):
  if len(x) > limit:
    raise ValueError(x)
  else:
    return x.title()


class ErrorHandlingTest(unittest.TestCase):
  def test_error_handling(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create(['a', 'bb', 'cccc'])
      with error_handling.ErrorHandler(
          beam.Map(lambda x: "error: %s" % x)) as error_handler:
        result = pcoll | PTransformWithErrors(3).with_error_handler(
            error_handler)
      error_pcoll = error_handler.output()

      assert_that(result, equal_to(['A', 'Bb']), label='CheckGood')
      assert_that(error_pcoll, equal_to(['error: cccc']), label='CheckBad')

  def test_error_handling_pardo(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create(['a', 'bb', 'cccc'])
      with error_handling.ErrorHandler(
          beam.Map(lambda x: "error: %s" % x[0])) as error_handler:
        result = pcoll | beam.Map(
            exception_throwing_map, limit=3).with_error_handler(error_handler)
      error_pcoll = error_handler.output()

      assert_that(result, equal_to(['A', 'Bb']), label='CheckGood')
      assert_that(error_pcoll, equal_to(['error: cccc']), label='CheckBad')

  def test_error_handling_pardo_with_exception_handling_kwargs(self):
    def side_effect(*args):
      beam._test_error_handling_pardo_with_exception_handling_kwargs_val = True

    def check_side_effect():
      return getattr(
          beam,
          '_test_error_handling_pardo_with_exception_handling_kwargs_val',
          False)

    self.assertFalse(check_side_effect())

    with beam.Pipeline() as p:
      pcoll = p | beam.Create(['a', 'bb', 'cccc'])
      with error_handling.ErrorHandler(
          beam.Map(lambda x: "error: %s" % x[0])) as error_handler:
        result = pcoll | beam.Map(
            exception_throwing_map, limit=3).with_error_handler(
                error_handler, on_failure_callback=side_effect)
      error_pcoll = error_handler.output()

      assert_that(result, equal_to(['A', 'Bb']), label='CheckGood')
      assert_that(error_pcoll, equal_to(['error: cccc']), label='CheckBad')

    self.assertTrue(check_side_effect())

  def test_error_on_unclosed_error_handler(self):
    with self.assertRaisesRegex(RuntimeError, r'.*Unclosed error handler.*'):
      with beam.Pipeline() as p:
        pcoll = p | beam.Create(['a', 'bb', 'cccc'])
        # Use this outside of a context to allow it to remain unclosed.
        error_handler = error_handling.ErrorHandler(beam.Map(lambda x: x))
        _ = pcoll | PTransformWithErrors(3).with_error_handler(error_handler)

  def test_collecting_error_handler(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create(['a', 'bb', 'cccc'])
      with error_handling.CollectingErrorHandler() as error_handler:
        result = pcoll | beam.Map(
            exception_throwing_map, limit=3).with_error_handler(error_handler)
      error_pcoll = error_handler.output() | beam.Map(lambda x: x[0])

      assert_that(result, equal_to(['A', 'Bb']), label='CheckGood')
      assert_that(error_pcoll, equal_to(['cccc']), label='CheckBad')

  def test_error_on_collecting_error_handler_without_output_retrieval(self):
    with self.assertRaisesRegex(
        RuntimeError,
        r'.*CollectingErrorHandler requires the output to be retrieved.*'):
      with beam.Pipeline() as p:
        pcoll = p | beam.Create(['a', 'bb', 'cccc'])
        with error_handling.CollectingErrorHandler() as error_handler:
          _ = pcoll | beam.Map(
              exception_throwing_map,
              limit=3).with_error_handler(error_handler)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
