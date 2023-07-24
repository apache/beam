import unittest

import apache_beam as beam

from apache_beam.runners.trivial_runner import TrivialRunner
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class TrivialRunnerTest(unittest.TestCase):
  def test_trivial(self):
    # The most trivial pipeline, to ensure at least something is working.
    # (Notably avoids the non-trivial complexity within assert_that.)
    with beam.Pipeline(runner=TrivialRunner()) as p:
      _ = p | beam.Impulse()

  def test_assert_that(self):
    # If this fails, the other tests may be vacuous.
    with self.assertRaisesRegex(Exception, 'Failed assert'):
      with beam.Pipeline(runner=TrivialRunner()) as p:
        assert_that(p | beam.Impulse(), equal_to(['a']))

  def test_impulse(self):
    with beam.Pipeline(runner=TrivialRunner()) as p:
      assert_that(p | beam.Impulse(), equal_to([b'']))

  def test_create(self):
    with beam.Pipeline(runner=TrivialRunner()) as p:
      assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

  def test_flatten(self):
    with beam.Pipeline(runner=TrivialRunner()) as p:
      ab = p | 'AB' >> beam.Create(['a', 'b'])
      c = p | 'C' >> beam.Create(['c'])
      assert_that((ab, c, c) | beam.Flatten(), equal_to(['a', 'b', 'c', 'c']))

  def test_map(self):
    with beam.Pipeline(runner=TrivialRunner()) as p:
      assert_that(
          p | beam.Create(['a', 'b']) | beam.Map(str.upper),
          equal_to(['A', 'B']))

  def test_gbk(self):
    with beam.Pipeline(runner=TrivialRunner()) as p:
      result = (
          p
          | beam.Create([('a', 1), ('b', 2), ('b', 3)])
          | beam.GroupByKey()
          | beam.MapTuple(lambda k, vs: (k, sorted(vs))))
      assert_that(result, equal_to([('a', [1]), ('b', [2, 3])]))
