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

"""Integration tests for cross-language transform expansion."""

# pytype: skip-file

from __future__ import absolute_import

import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.runners.portability import expansion_service
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import ptransform


class ExternalTransformIT(unittest.TestCase):
  @attr('IT')
  def test_job_python_from_python_it(self):
    @ptransform.PTransform.register_urn('simple', None)
    class SimpleTransform(ptransform.PTransform):
      def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: 'Simple(%s)' % x)

      def to_runner_api_parameter(self, unused_context):
        return 'simple', None

      @staticmethod
      def from_runner_api_parameter(_0, _1, _2):
        return SimpleTransform()

    pipeline = TestPipeline(is_integration_test=True)

    res = (
        pipeline
        | beam.Create(['a', 'b'])
        | beam.ExternalTransform(
            'simple', None, expansion_service.ExpansionServiceServicer()))
    assert_that(res, equal_to(['Simple(a)', 'Simple(b)']))

    proto_pipeline, _ = pipeline.to_runner_api(return_context=True)
    pipeline_from_proto = Pipeline.from_runner_api(
        proto_pipeline, pipeline.runner, pipeline._options)
    pipeline_from_proto.run().wait_until_finish()


if __name__ == '__main__':
  unittest.main()
