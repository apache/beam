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
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform


class FakeTransform(beam.PTransform):
  def __init__(self, creator, urn):
    self.creator = creator
    self.urn = urn

  def expand(self, pcoll):
    return pcoll | beam.Map(lambda x: x + ((self.urn, self.creator), ))


class FakeExternalProvider(yaml_provider.ExternalProvider):
  def __init__(
      self, id, known_transforms, extra_transform_urns, error_on_use=False):
    super().__init__(known_transforms, None)
    self._id = id
    self._schema_transforms = {urn: None for urn in extra_transform_urns}
    self._error_on_use = error_on_use

  def create_transform(self, type, *unused_args, **unused_kwargs):
    if self._error_on_use:
      raise RuntimeError(f'Provider {self._id} should not be used.')
    return FakeTransform(self._id, self._urns[type])

  def available(self):
    # Claim we're available even if we error on use.
    return True


class YamlProvidersTest(unittest.TestCase):
  def test_external_with_underlying_provider(self):
    providerA = FakeExternalProvider("A", {'A': 'a:urn'}, ['b:urn'])
    providerB = FakeExternalProvider("B", {'B': 'b:urn', 'alias': 'a:urn'}, [])
    newA = providerB.with_underlying_provider(providerA)

    self.assertIn('B', list(newA.provided_transforms()))
    t = newA.create_transform('B')
    self.assertEqual('A', t.creator)
    self.assertEqual('b:urn', t.urn)

    self.assertIn('alias', list(newA.provided_transforms()))
    t = newA.create_transform('alias')
    self.assertEqual('A', t.creator)
    self.assertEqual('a:urn', t.urn)

  def test_renaming_with_underlying_provider(self):
    providerA = FakeExternalProvider("A", {'A': 'a:urn'}, ['b:urn'])
    providerB = FakeExternalProvider("B", {'B': 'b:urn', 'C': 'c:urn'}, [])
    providerR = yaml_provider.RenamingProvider(  # keep wrapping
        {'RenamedB': 'B', 'RenamedC': 'C' },
        {'RenamedB': {},  'RenamedC': {}},
        providerB)

    newR = providerR.with_underlying_provider(providerA)
    self.assertIn('RenamedB', list(newR.provided_transforms()))
    self.assertNotIn('RenamedC', list(newR.provided_transforms()))
    t = newR.create_transform('RenamedB', {}, None)
    self.assertEqual('A', t.creator)
    self.assertEqual('b:urn', t.urn)

  def test_extended_providers_reused(self):
    providerA = FakeExternalProvider("A", {'A': 'a:urn'}, ['b:urn'])
    providerB = FakeExternalProvider(
        "B", {
            'B': 'b:urn', 'C': 'c:urn'
        }, [], error_on_use=True)
    providerR = yaml_provider.RenamingProvider(  # keep wrapping
        {'RenamedB': 'B', 'RenamedC': 'C' },
        {'RenamedB': {},  'RenamedC': {}},
        providerB)

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | 'Yaml1' >> yaml_transform.YamlTransform(
          '''
          type: chain
          transforms:
            - type: Create
              config:
                  elements: [0]
            - type: A
            - type: B
            - type: RenamedB
          ''',
          providers=[providerA, providerB, providerR])
      # All of these transforms should be serviced by providerA,
      # negating the need to invoke providerB.
      assert_that(
          result,
          equal_to([(0, ('a:urn', 'A'), ('b:urn', 'A'), ('b:urn', 'A'))]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
