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

"""Unit tests for the pipeline options validator module."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest
from builtins import object

from hamcrest import assert_that
from hamcrest import contains_string
from hamcrest import only_contains
from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator


# Mock runners to use for validations.
class MockRunners(object):
  class DataflowRunner(object):
    def get_default_gcp_region(self):
      # Return a default so we don't have to specify --region in every test
      # (unless specifically testing it).
      return 'us-central1'

  class TestDataflowRunner(DataflowRunner):
    pass

  class OtherRunner(object):
    pass


# Matcher that always passes for testing on_success_matcher option
class AlwaysPassMatcher(BaseMatcher):
  def _matches(self, item):
    return True


class SetupTest(unittest.TestCase):
  def check_errors_for_arguments(self, errors, args):
    """Checks that there is exactly one error for each given argument."""
    missing = []
    remaining = list(errors)

    for arg in args:
      found = False
      for error in remaining:
        if arg in error:
          remaining.remove(error)
          found = True
          break
      if not found:
        missing.append('Missing error for: ' + arg)

    # Return missing and remaining (not matched) errors.
    return missing + remaining

  def test_local_runner(self):
    runner = MockRunners.OtherRunner()
    options = PipelineOptions([])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 0)

  def test_missing_required_options(self):
    options = PipelineOptions([''])
    runner = MockRunners.DataflowRunner()
    # Remove default region for this test.
    runner.get_default_gcp_region = lambda: None
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()

    self.assertEqual(
        self.check_errors_for_arguments(
            errors, ['project', 'staging_location', 'temp_location', 'region']),
        [])

  def test_gcs_path(self):
    def get_validator(temp_location, staging_location):
      options = ['--project=example:example', '--job_name=job']

      if temp_location is not None:
        options.append('--temp_location=' + temp_location)

      if staging_location is not None:
        options.append('--staging_location=' + staging_location)

      pipeline_options = PipelineOptions(options)
      runner = MockRunners.DataflowRunner()
      validator = PipelineOptionsValidator(pipeline_options, runner)
      return validator

    test_cases = [
        {
            'temp_location': None,
            'staging_location': 'gs://foo/bar',
            'errors': ['temp_location']
        },
        {
            'temp_location': None,
            'staging_location': None,
            'errors': ['staging_location', 'temp_location']
        },
        {
            'temp_location': 'gs://foo/bar',
            'staging_location': None,
            'errors': []
        },
        {
            'temp_location': 'gs://foo/bar',
            'staging_location': 'gs://ABC/bar',
            'errors': ['staging_location']
        },
        {
            'temp_location': 'gcs:/foo/bar',
            'staging_location': 'gs://foo/bar',
            'errors': ['temp_location']
        },
        {
            'temp_location': 'gs:/foo/bar',
            'staging_location': 'gs://foo/bar',
            'errors': ['temp_location']
        },
        {
            'temp_location': 'gs://ABC/bar',
            'staging_location': 'gs://foo/bar',
            'errors': ['temp_location']
        },
        {
            'temp_location': 'gs://ABC/bar',
            'staging_location': 'gs://foo/bar',
            'errors': ['temp_location']
        },
        {
            'temp_location': 'gs://foo',
            'staging_location': 'gs://foo/bar',
            'errors': ['temp_location']
        },
        {
            'temp_location': 'gs://foo/',
            'staging_location': 'gs://foo/bar',
            'errors': []
        },
        {
            'temp_location': 'gs://foo/bar',
            'staging_location': 'gs://foo/bar',
            'errors': []
        },
    ]

    for case in test_cases:
      errors = get_validator(case['temp_location'],
                             case['staging_location']).validate()
      self.assertEqual(
          self.check_errors_for_arguments(errors, case['errors']), [])

  def test_project(self):
    def get_validator(project):
      options = [
          '--job_name=job',
          '--staging_location=gs://foo/bar',
          '--temp_location=gs://foo/bar'
      ]

      if project is not None:
        options.append('--project=' + project)

      pipeline_options = PipelineOptions(options)
      runner = MockRunners.DataflowRunner()
      validator = PipelineOptionsValidator(pipeline_options, runner)
      return validator

    test_cases = [
        {
            'project': None, 'errors': ['project']
        },
        {
            'project': '12345', 'errors': ['project']
        },
        {
            'project': 'FOO', 'errors': ['project']
        },
        {
            'project': 'foo:BAR', 'errors': ['project']
        },
        {
            'project': 'fo', 'errors': ['project']
        },
        {
            'project': 'foo', 'errors': []
        },
        {
            'project': 'foo:bar', 'errors': []
        },
    ]

    for case in test_cases:
      errors = get_validator(case['project']).validate()
      self.assertEqual(
          self.check_errors_for_arguments(errors, case['errors']), [])

  def test_job_name(self):
    def get_validator(job_name):
      options = [
          '--project=example:example',
          '--staging_location=gs://foo/bar',
          '--temp_location=gs://foo/bar'
      ]

      if job_name is not None:
        options.append('--job_name=' + job_name)

      pipeline_options = PipelineOptions(options)
      runner = MockRunners.DataflowRunner()
      validator = PipelineOptionsValidator(pipeline_options, runner)
      return validator

    test_cases = [
        {
            'job_name': None, 'errors': []
        },
        {
            'job_name': '12345', 'errors': ['job_name']
        },
        {
            'job_name': 'FOO', 'errors': ['job_name']
        },
        {
            'job_name': 'foo:bar', 'errors': ['job_name']
        },
        {
            'job_name': 'fo', 'errors': []
        },
        {
            'job_name': 'foo', 'errors': []
        },
    ]

    for case in test_cases:
      errors = get_validator(case['job_name']).validate()
      self.assertEqual(
          self.check_errors_for_arguments(errors, case['errors']), [])

  def test_num_workers(self):
    def get_validator(num_workers):
      options = [
          '--project=example:example',
          '--job_name=job',
          '--staging_location=gs://foo/bar',
          '--temp_location=gs://foo/bar'
      ]

      if num_workers is not None:
        options.append('--num_workers=' + num_workers)

      pipeline_options = PipelineOptions(options)
      runner = MockRunners.DataflowRunner()
      validator = PipelineOptionsValidator(pipeline_options, runner)
      return validator

    test_cases = [
        {
            'num_workers': None, 'errors': []
        },
        {
            'num_workers': '1', 'errors': []
        },
        {
            'num_workers': '0', 'errors': ['num_workers']
        },
        {
            'num_workers': '-1', 'errors': ['num_workers']
        },
    ]

    for case in test_cases:
      errors = get_validator(case['num_workers']).validate()
      self.assertEqual(
          self.check_errors_for_arguments(errors, case['errors']), [])

  def test_is_service_runner(self):
    test_cases = [
        {
            'runner': MockRunners.OtherRunner(),
            'options': [],
            'expected': False,
        },
        {
            'runner': MockRunners.OtherRunner(),
            'options': ['--dataflow_endpoint=https://dataflow.googleapis.com'],
            'expected': False,
        },
        {
            'runner': MockRunners.OtherRunner(),
            'options': ['--dataflow_endpoint=https://dataflow.googleapis.com/'],
            'expected': False,
        },
        {
            'runner': MockRunners.DataflowRunner(),
            'options': ['--dataflow_endpoint=https://another.service.com'],
            'expected': False,
        },
        {
            'runner': MockRunners.DataflowRunner(),
            'options': ['--dataflow_endpoint=https://another.service.com/'],
            'expected': False,
        },
        {
            'runner': MockRunners.DataflowRunner(),
            'options': ['--dataflow_endpoint=https://dataflow.googleapis.com'],
            'expected': True,
        },
        {
            'runner': MockRunners.DataflowRunner(),
            'options': ['--dataflow_endpoint=https://dataflow.googleapis.com/'],
            'expected': True,
        },
        {
            'runner': MockRunners.DataflowRunner(),
            'options': [],
            'expected': True,
        },
    ]

    for case in test_cases:
      validator = PipelineOptionsValidator(
          PipelineOptions(case['options']), case['runner'])
      self.assertEqual(validator.is_service_runner(), case['expected'])

  def test_dataflow_job_file_and_template_location_mutually_exclusive(self):
    runner = MockRunners.OtherRunner()
    options = PipelineOptions(
        ['--template_location', 'abc', '--dataflow_job_file', 'def'])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertTrue(errors)

  def test_validate_template_location(self):
    runner = MockRunners.OtherRunner()
    options = PipelineOptions([
        '--template_location',
        'abc',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertFalse(errors)

  def test_validate_dataflow_job_file(self):
    runner = MockRunners.OtherRunner()
    options = PipelineOptions(['--dataflow_job_file', 'abc'])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertFalse(errors)

  def test_zone_and_worker_region_mutually_exclusive(self):
    runner = MockRunners.DataflowRunner()
    options = PipelineOptions([
        '--zone',
        'us-east1-b',
        '--worker_region',
        'us-east1',
        '--project=example:example',
        '--temp_location=gs://foo/bar',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 1)
    self.assertIn('zone', errors[0])
    self.assertIn('worker_region', errors[0])

  def test_zone_and_worker_zone_mutually_exclusive(self):
    runner = MockRunners.DataflowRunner()
    options = PipelineOptions([
        '--zone',
        'us-east1-b',
        '--worker_zone',
        'us-east1-c',
        '--project=example:example',
        '--temp_location=gs://foo/bar',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 1)
    self.assertIn('zone', errors[0])
    self.assertIn('worker_zone', errors[0])

  def test_experiment_region_and_worker_region_mutually_exclusive(self):
    runner = MockRunners.DataflowRunner()
    options = PipelineOptions([
        '--experiments',
        'worker_region=us-west1',
        '--worker_region',
        'us-east1',
        '--project=example:example',
        '--temp_location=gs://foo/bar',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 1)
    self.assertIn('experiment', errors[0])
    self.assertIn('worker_region', errors[0])

  def test_experiment_region_and_worker_zone_mutually_exclusive(self):
    runner = MockRunners.DataflowRunner()
    options = PipelineOptions([
        '--experiments',
        'worker_region=us-west1',
        '--worker_zone',
        'us-east1-b',
        '--project=example:example',
        '--temp_location=gs://foo/bar',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 1)
    self.assertIn('experiment', errors[0])
    self.assertIn('worker_region', errors[0])
    self.assertIn('worker_zone', errors[0])

  def test_worker_region_and_worker_zone_mutually_exclusive(self):
    runner = MockRunners.DataflowRunner()
    options = PipelineOptions([
        '--worker_region',
        'us-east1',
        '--worker_zone',
        'us-east1-b',
        '--project=example:example',
        '--temp_location=gs://foo/bar',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 1)
    self.assertIn('worker_region', errors[0])
    self.assertIn('worker_zone', errors[0])

  def test_zone_alias_worker_zone(self):
    runner = MockRunners.DataflowRunner()
    options = PipelineOptions([
        '--zone=us-east1-b',
        '--project=example:example',
        '--temp_location=gs://foo/bar',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 0)
    self.assertIsNone(options.view_as(WorkerOptions).zone)
    self.assertEqual(options.view_as(WorkerOptions).worker_zone, 'us-east1-b')

  def test_region_optional_for_non_service_runner(self):
    runner = MockRunners.DataflowRunner()
    # Remove default region for this test.
    runner.get_default_gcp_region = lambda: None
    options = PipelineOptions([
        '--project=example:example',
        '--temp_location=gs://foo/bar',
        '--dataflow_endpoint=http://localhost:20281',
    ])
    validator = PipelineOptionsValidator(options, runner)
    errors = validator.validate()
    self.assertEqual(len(errors), 0)

  def test_test_matcher(self):
    def get_validator(matcher):
      options = [
          '--project=example:example',
          '--job_name=job',
          '--staging_location=gs://foo/bar',
          '--temp_location=gs://foo/bar',
      ]
      if matcher:
        options.append('%s=%s' % ('--on_success_matcher', matcher.decode()))

      pipeline_options = PipelineOptions(options)
      runner = MockRunners.TestDataflowRunner()
      return PipelineOptionsValidator(pipeline_options, runner)

    test_case = [
        {
            'on_success_matcher': None, 'errors': []
        },
        {
            'on_success_matcher': pickler.dumps(AlwaysPassMatcher()),
            'errors': []
        },
        {
            'on_success_matcher': b'abc', 'errors': ['on_success_matcher']
        },
        {
            'on_success_matcher': pickler.dumps(object),
            'errors': ['on_success_matcher']
        },
    ]

    for case in test_case:
      errors = get_validator(case['on_success_matcher']).validate()
      self.assertEqual(
          self.check_errors_for_arguments(errors, case['errors']), [])

  def test_transform_name_mapping_without_update(self):
    options = [
        '--project=example:example',
        '--staging_location=gs://foo/bar',
        '--temp_location=gs://foo/bar',
        '--transform_name_mapping={\"fromPardo\":\"toPardo\"}'
    ]

    pipeline_options = PipelineOptions(options)
    runner = MockRunners.DataflowRunner()
    validator = PipelineOptionsValidator(pipeline_options, runner)
    errors = validator.validate()
    assert_that(
        errors,
        only_contains(
            contains_string(
                'Transform name mapping option is only useful when '
                '--update and --streaming is specified')))

  def test_transform_name_mapping_invalid_format(self):
    options = [
        '--project=example:example',
        '--staging_location=gs://foo/bar',
        '--temp_location=gs://foo/bar',
        '--update',
        '--job_name=test',
        '--streaming',
        '--transform_name_mapping={\"fromPardo\":123}'
    ]

    pipeline_options = PipelineOptions(options)
    runner = MockRunners.DataflowRunner()
    validator = PipelineOptionsValidator(pipeline_options, runner)
    errors = validator.validate()
    assert_that(
        errors,
        only_contains(
            contains_string('Invalid transform name mapping format.')))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
