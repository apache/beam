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

"""Tests for read_cache."""
# pytype: skip-file

import unittest
from unittest.mock import patch

import apache_beam as beam
from apache_beam.runners.interactive import augmented_pipeline as ap
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.caching import read_cache
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_proto_equal
from apache_beam.runners.interactive.testing.test_cache_manager import InMemoryCache


class ReadCacheTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  @patch(
      'apache_beam.runners.interactive.interactive_environment'
      '.InteractiveEnvironment.get_cache_manager')
  def test_read_cache(self, mocked_get_cache_manager):
    p = beam.Pipeline()
    pcoll = p | beam.Create([1, 2, 3])
    consumer_transform = beam.Map(lambda x: x * x)
    _ = pcoll | consumer_transform
    ib.watch(locals())

    # Create the cache in memory.
    cache_manager = InMemoryCache()
    mocked_get_cache_manager.return_value = cache_manager
    aug_p = ap.AugmentedPipeline(p)
    key = repr(aug_p._cacheables[pcoll].to_key())
    cache_manager.write('test', 'full', key)

    # Capture the applied transform of the consumer_transform.
    pcoll_id = aug_p._context.pcollections.get_id(pcoll)
    consumer_transform_id = None
    pipeline_proto = p.to_runner_api()
    for (transform_id,
         transform) in pipeline_proto.components.transforms.items():
      if pcoll_id in transform.inputs.values():
        consumer_transform_id = transform_id
        break
    self.assertIsNotNone(consumer_transform_id)

    # Read cache on the pipeline proto.
    _, cache_id = read_cache.ReadCache(
        pipeline_proto, aug_p._context, aug_p._cache_manager,
        aug_p._cacheables[pcoll]).read_cache()
    actual_pipeline = pipeline_proto

    # Read cache directly on the pipeline instance.
    transform = read_cache._ReadCacheTransform(aug_p._cache_manager, key)
    p | 'source_cache_' + key >> transform
    expected_pipeline = p.to_runner_api()

    # This rougly checks the equivalence between two protos, not detailed
    # wiring in sub transforms under top level transforms.
    assert_pipeline_proto_equal(self, expected_pipeline, actual_pipeline)

    # Check if the actual_pipeline uses cache as input of the
    # consumer_transform instead of the original pcoll from source.
    inputs = actual_pipeline.components.transforms[consumer_transform_id].inputs
    self.assertIn(cache_id, inputs.values())
    self.assertNotIn(pcoll_id, inputs.values())


if __name__ == '__main__':
  unittest.main()
