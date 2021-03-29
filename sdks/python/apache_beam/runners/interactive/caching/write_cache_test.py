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

"""Tests for write_cache."""
# pytype: skip-file

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.runners.interactive import augmented_pipeline as ap
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.caching import write_cache
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_proto_equal
from apache_beam.runners.interactive.testing.test_cache_manager import InMemoryCache


class WriteCacheTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  def test_write_cache(self):
    p = beam.Pipeline()
    pcoll = p | beam.Create([1, 2, 3])
    ib.watch(locals())

    cache_manager = InMemoryCache()
    ie.current_env().set_cache_manager(cache_manager, p)
    aug_p = ap.AugmentedPipeline(p)
    key = repr(aug_p._cacheables[pcoll].to_key())

    # Write cache on the pipeline proto.
    write_cache.WriteCache(
        aug_p._pipeline,
        aug_p._context,
        aug_p._cache_manager,
        aug_p._cacheables[pcoll]).write_cache()
    actual_pipeline = aug_p._pipeline

    # Write cache directly on the piepline instance.
    label = '{}{}'.format('_cache_', key)
    transform = write_cache._WriteCacheTransform(
        aug_p._cache_manager, key, label)
    _ = pcoll | 'sink' + label >> transform
    expected_pipeline = p.to_runner_api()

    assert_pipeline_proto_equal(self, expected_pipeline, actual_pipeline)

    # Check if the actual_pipeline uses pcoll as an input of a write transform.
    pcoll_id = aug_p._context.pcollections.get_id(pcoll)
    write_transform_id = None
    for transform_id, transform in actual_pipeline.components.transforms.items():
      if pcoll_id in transform.inputs.values():
        write_transform_id = transform_id
        break
    self.assertIsNotNone(write_transform_id)
    self.assertIn(
        'sink',
        actual_pipeline.components.transforms[write_transform_id].unique_name)


if __name__ == '__main__':
  unittest.main()
