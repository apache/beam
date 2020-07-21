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
# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
from apache_beam.portability import common_urns
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.transforms import combiners
from apache_beam.transforms.core import Create


class TranslationsTest(unittest.TestCase):

  def test_pack_combiners(self):
    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | 'start-perkey' >> Create([('a', x) for x in vals])
    _ = pcoll | 'mean-perkey' >> combiners.Mean.PerKey()
    _ = pcoll | 'count-perkey' >> combiners.Count.PerKey()

    pipeline_proto = pipeline.to_runner_api()
    _, stages = translations.create_and_optimize_stages(
        pipeline_proto, [translations.pack_combiners],
        known_runner_urns=frozenset())
    combine_per_key_stages = []
    for stage in stages:
      for transform in stage.transforms:
        if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
          combine_per_key_stages.append(stage)
    self.assertEqual(len(combine_per_key_stages), 1)
    self.assertIn('/Pack', combine_per_key_stages[0].name)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
