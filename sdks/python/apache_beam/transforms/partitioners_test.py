import doctest
import pytest
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import partitioners
from apache_beam.testing.test_pipeline import TestPipeline


class TopTest(unittest.TestCase):
  def test_bad_n(self):
    with pytest.raises(ValueError):
      partitioners.Top(0)
    with pytest.raises(ValueError):
      partitioners.Top(-1)
    with pytest.raises(ValueError):
      partitioners.Top(1.5)

  def test_empty(self):
    with TestPipeline() as p:
      sample, remaining = (p
                           | beam.Create([], reshuffle=False)
                           | partitioners.Top(3))
      assert_that(
          sample | "CountSample" >> beam.combiners.Count.Globally(),
          equal_to([0]),
          label="assert0")
      assert_that(
          remaining | "CountRemaining" >> beam.combiners.Count.Globally(),
          equal_to([0]),
          label="assert1")

  def test_Top(self):

    with TestPipeline() as p:
      sample, remaining = (p
                           | beam.Create([1, 1, 2, 2, 3, 3], reshuffle=False)
                           | partitioners.Top(3))
      assert_that(
          sample | "SampleAsList" >> beam.combiners.ToList()
          | "SortSample" >> beam.Map(sorted),
          equal_to([[
              2,
              3,
              3,
          ]]),
          label="assert0")
      assert_that(
          remaining | "RemainingAsList" >> beam.combiners.ToList()
          | "SortRemaining" >> beam.Map(sorted),
          equal_to([[1, 1, 2]]),
          label="assert1")

  def test_Top_key(self):

    with TestPipeline() as p:
      sample, remaining = (p
                           | beam.Create([1, 1, 2, 2, 3, 3],
                                           reshuffle=False)
                           | partitioners.Top(3, key=lambda x: -x))
      assert_that(
          sample | "SampleAsList" >> beam.combiners.ToList()
          | "SortSample" >> beam.Map(sorted),
          equal_to([[1, 1, 2]]),
          label="assert0")
      assert_that(
          remaining | "RemainingAsList" >> beam.combiners.ToList()
          | "SortRemaining" >> beam.Map(sorted),
          equal_to([[2, 3, 3]]),
          label="assert1")

  def test_Top_reverse(self):

    with TestPipeline() as p:
      sample, remaining = (p
                           | beam.Create([1, 1, 2, 2, 3, 3],
                                           reshuffle=False)
                           | partitioners.Top(3, reverse=True))
      assert_that(
          sample | "SampleAsList" >> beam.combiners.ToList()
          | "SortSample" >> beam.Map(sorted),
          equal_to([[1, 1, 2]]),
          label="assert0")
      assert_that(
          remaining | "RemainingAsList" >> beam.combiners.ToList()
          | "SortRemaining" >> beam.Map(sorted),
          equal_to([[2, 3, 3]]),
          label="assert1")


class DocTest(unittest.TestCase):
  def test_docs(self):
    doctest.testmod(partitioners)
