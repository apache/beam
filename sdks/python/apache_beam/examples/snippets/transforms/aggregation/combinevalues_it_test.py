import logging
import pytest
import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import combinevalues

class CombineValuesIT(unittest.TestCase):

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    
  @pytest.mark.it_postcommit
  def test_combinevalues_it(self):
    def merge(vals):
      out = ""
      for v in vals:
          out += v
      return out
    
    pcoll = \
        self.test_pipeline \
        | beam.Create([("key1", "foo"), ("key2", "bar"), ("key1", "foo")], reshuffle=False)
        | beam.GroupByKey()
        | beam.CombineValues(merge)
        | beam.MapTuple(lambda k, v: '{}: {}'.format(k, v))

    result = self.test_pipeline.run()
    result.wait_until_finish()

    assert result.state == PipelineState.DONE
    assert_that(result, equal_to(['key1: foofoo', 'key2: bar']))
    
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
