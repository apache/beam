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

"""Runs integration tests in the tests directory."""

import argparse
import contextlib
import copy
import glob
import itertools
import logging
import mock
import os
import random
import re
import sys
import tempfile
import uuid
import unittest

import yaml
from yaml.loader import SafeLoader

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.typehints import trivial_inference
from apache_beam.yaml import yaml_mapping
from apache_beam.yaml import yaml_provider
from apache_beam.yaml import yaml_transform

from apache_beam.utils import python_callable


@contextlib.contextmanager
def gcs_temp_dir(bucket):
  gcs_tempdir = bucket + '/yaml-' + str(uuid.uuid4())
  yield gcs_tempdir
  filesystems.FileSystems.delete([gcs_tempdir])


def replace_recursive(spec, vars):
  if isinstance(spec, dict):
    return {
        key: replace_recursive(value, vars)
        for (key, value) in spec.items()
    }
  elif isinstance(spec, list):
    return [replace_recursive(value, vars) for value in spec]
  elif isinstance(spec, str) and '{' in spec:
    try:
      return eval('f' + repr(spec), vars)
    except Exception as exn:
      raise ValueError(f"Error evaluating {spec}: {exn}") from exn
  else:
    return spec


def transform_types(spec):
  if spec.get('type', None) in (None, 'composite', 'chain'):
    if 'source' in spec:
      yield from transform_types(spec['source'])
    for t in spec.get('transforms', []):
      yield from transform_types(t)
    if 'sink' in spec:
      yield from transform_types(spec['sink'])
  else:
    yield spec['type']


def provider_sets(spec, require_available=False):
  """For transforms that are vended by multiple providers, yields all possible
  combinations of providers to use.
  """
  all_transform_types = set.union(
      *(
          set(
              transform_types(
                  yaml_transform.preprocess(copy.deepcopy(p['pipeline']))))
          for p in spec['pipelines']))

  def filter_to_available(t, providers):
    if require_available:
      for p in providers:
        if not p.available():
          raise ValueError("Provider {p} required for {t} is not available.")
      return providers
    else:
      return [p for p in providers if p.available()]

  standard_providers = yaml_provider.standard_providers()
  multiple_providers = {
      t: filter_to_available(t, standard_providers[t])
      for t in all_transform_types
      if len(filter_to_available(t, standard_providers[t])) > 1
  }
  if not multiple_providers:
    return None, standard_providers
  else:
    names, provider_lists = zip(*sorted(multiple_providers.items()))
    for ix, c in enumerate(itertools.product(*provider_lists)):
      yield (
          '_'.join(
              n + '_' + type(p.underlying_provider()).__name__
              for (n, p) in zip(names, c)) + f'_{ix}',
          dict(standard_providers, **{n: [p]
                                      for (n, p) in zip(names, c)}))


def create_test_methods(spec):
  for suffix, providers in provider_sets(spec):

    def test(self, providers=providers):  # default arg to capture loop value
      vars = {}
      with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch(
                'apache_beam.yaml.yaml_provider.standard_providers',
                lambda: providers))
        for fixture in spec.get('fixtures', []):
          vars[fixture['name']] = stack.enter_context(
              python_callable.PythonCallableWithSource.
              load_from_fully_qualified_name(fixture['type'])(
                  **yaml_transform.SafeLineLoader.strip_metadata(
                      fixture.get('config', {}))))
        for pipeline_spec in spec['pipelines']:
          with beam.Pipeline(options=PipelineOptions(
              pickle_library='cloudpickle',
              **yaml_transform.SafeLineLoader.strip_metadata(pipeline_spec.get(
                  'options', {})))) as p:
            yaml_transform.expand_pipeline(
                p, replace_recursive(pipeline_spec, vars))

    yield suffix, test


def parse_test_files(filepattern):
  for path in glob.glob(filepattern):
    with open(path) as fin:
      base_name = f'test_{os.path.basename(path)}'.replace('.', '_')
      for suffix, func in create_test_methods(
          yaml.load(fin, Loader=yaml_transform.SafeLineLoader)):
        if suffix:
          yield f'{base_name}_{suffix}', func
        else:
          yield base_name, func


def createTestSuite(name, filepattern):
  return type(name, (unittest.TestCase, ), dict(parse_test_files(filepattern)))


IntegrationTests = createTestSuite(
    'IntegrationTests',
    os.path.join(os.path.dirname(__file__), 'tests', '*.yaml'))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main(argv=unknown_args)
