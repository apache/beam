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

"""Tests for augmented_pipeline module."""

# pytest: skip-file

import unittest

import apache_beam as beam
from apache_beam.runners.interactive import augmented_pipeline as ap
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie


class CacheableTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  def test_find_all_cacheables(self):
    p = beam.Pipeline()
    cacheable_pcoll_1 = p | beam.Create([1, 2, 3])
    cacheable_pcoll_2 = cacheable_pcoll_1 | beam.Map(lambda x: x * x)
    ib.watch(locals())

    aug_p = ap.AugmentedPipeline(p)
    cacheables = aug_p.cacheables()
    self.assertIn(cacheable_pcoll_1, cacheables)
    self.assertIn(cacheable_pcoll_2, cacheables)

  def test_ignore_cacheables(self):
    p = beam.Pipeline()
    cacheable_pcoll_1 = p | 'cacheable_pcoll_1' >> beam.Create([1, 2, 3])
    cacheable_pcoll_2 = p | 'cacheable_pcoll_2' >> beam.Create([4, 5, 6])
    ib.watch(locals())

    aug_p = ap.AugmentedPipeline(p, (cacheable_pcoll_1, ))
    cacheables = aug_p.cacheables()
    self.assertIn(cacheable_pcoll_1, cacheables)
    self.assertNotIn(cacheable_pcoll_2, cacheables)

  def test_ignore_pcoll_from_other_pipeline(self):
    p = beam.Pipeline()
    p2 = beam.Pipeline()
    cacheable_from_p2 = p2 | beam.Create([1, 2, 3])
    ib.watch(locals())

    aug_p = ap.AugmentedPipeline(p)
    cacheables = aug_p.cacheables()
    self.assertNotIn(cacheable_from_p2, cacheables)


class AugmentTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  def test_error_when_pcolls_from_mixed_pipelines(self):
    p = beam.Pipeline()
    cacheable_from_p = p | beam.Create([1, 2, 3])
    p2 = beam.Pipeline()
    cacheable_from_p2 = p2 | beam.Create([1, 2, 3])
    ib.watch(locals())

    self.assertRaises(
        AssertionError,
        lambda: ap.AugmentedPipeline(p, (cacheable_from_p, cacheable_from_p2)))


if __name__ == '__main__':
  unittest.main()
