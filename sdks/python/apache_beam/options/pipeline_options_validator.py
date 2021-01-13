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

"""Pipeline options validator.

For internal use only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

from __future__ import absolute_import

import logging
import re
from builtins import object

from past.builtins import unicode

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options import WorkerOptions

_LOGGER = logging.getLogger(__name__)


class PipelineOptionsValidator(object):
  """Validates PipelineOptions.

  Goes through a list of known PipelineOption subclassess and calls::

    validate(validator)

  if one is implemented. Aggregates a list of validation errors from all and
  returns an aggregated list.
  """

  # Validator will call validate on these subclasses of PipelineOptions
  OPTIONS = [
      DebugOptions,
      GoogleCloudOptions,
      PortableOptions,
      SetupOptions,
      StandardOptions,
      TestOptions,
      TypeOptions,
      WorkerOptions
  ]

  # Mutually exclusive options for different types of portable environments.
  REQUIRED_ENVIRONMENT_OPTIONS = {
      'DOCKER': [],
      'PROCESS': ['process_command'],
      'EXTERNAL': ['external_service_address'],
      'LOOPBACK': []
  }
  OPTIONAL_ENVIRONMENT_OPTIONS = {
      'DOCKER': ['docker_container_image'],
      'PROCESS': ['process_variables'],
      'EXTERNAL': [],
      'LOOPBACK': []
  }

  # Possible validation errors.
  ERR_MISSING_OPTION = 'Missing required option: %s.'
  ERR_MISSING_GCS_PATH = 'Missing GCS path option: %s.'
  ERR_INVALID_GCS_PATH = 'Invalid GCS path (%s), given for the option: %s.'
  ERR_INVALID_GCS_BUCKET = (
      'Invalid GCS bucket (%s), given for the option: %s. See '
      'https://developers.google.com/storage/docs/bucketnaming '
      'for more details.')
  ERR_INVALID_GCS_OBJECT = 'Invalid GCS object (%s), given for the option: %s.'
  ERR_INVALID_JOB_NAME = (
      'Invalid job_name (%s); the name must consist of only the characters '
      '[-a-z0-9], starting with a letter and ending with a letter or number')
  ERR_INVALID_PROJECT_NUMBER = (
      'Invalid Project ID (%s). Please make sure you specified the Project ID, '
      'not project number.')
  ERR_INVALID_PROJECT_ID = (
      'Invalid Project ID (%s). Please make sure you specified the Project ID, '
      'not project description.')
  ERR_INVALID_NOT_POSITIVE = (
      'Invalid value (%s) for option: %s. Value needs '
      'to be positive.')
  ERR_INVALID_TEST_MATCHER_TYPE = (
      'Invalid value (%s) for option: %s. Please extend your matcher object '
      'from hamcrest.core.base_matcher.BaseMatcher.')
  ERR_INVALID_TEST_MATCHER_UNPICKLABLE = (
      'Invalid value (%s) for option: %s. Please make sure the test matcher '
      'is unpicklable.')
  ERR_INVALID_TRANSFORM_NAME_MAPPING = (
      'Invalid transform name mapping format. Please make sure the mapping is '
      'string key-value pairs. Invalid pair: (%s:%s)')
  ERR_INVALID_ENVIRONMENT = (
      'Option %s is not compatible with environment type %s.')
  ERR_ENVIRONMENT_CONFIG = (
      'Option environment_config is incompatible with option(s) %s.')
  ERR_MISSING_REQUIRED_ENVIRONMENT_OPTION = (
      'Option %s is required for environment type %s.')

  # GCS path specific patterns.
  GCS_URI = '(?P<SCHEME>[^:]+)://(?P<BUCKET>[^/]+)(/(?P<OBJECT>.*))?'
  GCS_BUCKET = '^[a-z0-9][-_a-z0-9.]+[a-z0-9]$'
  GCS_SCHEME = 'gs'

  # GoogleCloudOptions specific patterns.
  JOB_PATTERN = '[a-z]([-a-z0-9]*[a-z0-9])?'
  PROJECT_ID_PATTERN = '[a-z][-a-z0-9:.]+[a-z0-9]'
  PROJECT_NUMBER_PATTERN = '[0-9]*'
  ENDPOINT_PATTERN = r'https://[\S]*googleapis\.com[/]?'

  def __init__(self, options, runner):
    self.options = options
    self.runner = runner

  def validate(self):
    """Calls validate on subclassess and returns a list of errors.

    validate will call validate method on subclasses, accumulate the returned
    list of errors, and returns the aggregate list.

    Returns:
      Aggregate list of errors after all calling all possible validate methods.
    """
    errors = []
    for cls in self.OPTIONS:
      if 'validate' in cls.__dict__ and callable(cls.__dict__['validate']):
        errors.extend(self.options.view_as(cls).validate(self))
    return errors

  def is_service_runner(self):
    """True if pipeline will execute on the Google Cloud Dataflow service."""
    is_service_runner = (
        self.runner is not None and
        type(self.runner).__name__ in ['DataflowRunner', 'TestDataflowRunner'])

    dataflow_endpoint = (
        self.options.view_as(GoogleCloudOptions).dataflow_endpoint)
    is_service_endpoint = (
        dataflow_endpoint is not None and
        self.is_full_string_match(self.ENDPOINT_PATTERN, dataflow_endpoint))

    return is_service_runner and is_service_endpoint

  def is_full_string_match(self, pattern, string):
    """Returns True if the pattern matches the whole string."""
    pattern = '^%s$' % pattern
    return re.search(pattern, string) is not None

  def _validate_error(self, err, *args):
    return [err % args]

  def validate_gcs_path(self, view, arg_name):
    """Validates a GCS path against gs://bucket/object URI format."""
    arg = getattr(view, arg_name, None)
    if arg is None:
      return self._validate_error(self.ERR_MISSING_GCS_PATH, arg_name)
    match = re.match(self.GCS_URI, arg, re.DOTALL)
    if match is None:
      return self._validate_error(self.ERR_INVALID_GCS_PATH, arg, arg_name)

    scheme = match.group('SCHEME')
    bucket = match.group('BUCKET')
    gcs_object = match.group('OBJECT')

    if ((scheme is None) or (scheme.lower() != self.GCS_SCHEME) or
        (bucket is None)):
      return self._validate_error(self.ERR_INVALID_GCS_PATH, arg, arg_name)

    if not self.is_full_string_match(self.GCS_BUCKET, bucket):
      return self._validate_error(self.ERR_INVALID_GCS_BUCKET, arg, arg_name)
    if gcs_object is None or '\n' in gcs_object or '\r' in gcs_object:
      return self._validate_error(self.ERR_INVALID_GCS_OBJECT, arg, arg_name)

    return []

  def validate_cloud_options(self, view):
    """Validates job_name and project arguments."""
    errors = []
    if (view.job_name and
        not self.is_full_string_match(self.JOB_PATTERN, view.job_name)):
      errors.extend(
          self._validate_error(self.ERR_INVALID_JOB_NAME, view.job_name))
    project = view.project
    if project is None:
      errors.extend(self._validate_error(self.ERR_MISSING_OPTION, 'project'))
    else:
      if self.is_full_string_match(self.PROJECT_NUMBER_PATTERN, project):
        errors.extend(
            self._validate_error(self.ERR_INVALID_PROJECT_NUMBER, project))
      elif not self.is_full_string_match(self.PROJECT_ID_PATTERN, project):
        errors.extend(
            self._validate_error(self.ERR_INVALID_PROJECT_ID, project))
    if view.update:
      if not view.job_name:
        errors.extend(
            self._validate_error(
                'Existing job name must be provided when updating a pipeline.'))
    if view.transform_name_mapping:
      if not view.update or not self.options.view_as(StandardOptions).streaming:
        errors.append(
            'Transform name mapping option is only useful when '
            '--update and --streaming is specified')
      for _, (key, value) in enumerate(view.transform_name_mapping.items()):
        if not isinstance(key, (str, unicode)) \
            or not isinstance(value, (str, unicode)):
          errors.extend(
              self._validate_error(
                  self.ERR_INVALID_TRANSFORM_NAME_MAPPING, key, value))
          break
    if view.region is None and self.is_service_runner():
      default_region = self.runner.get_default_gcp_region()
      if default_region is None:
        errors.extend(self._validate_error(self.ERR_MISSING_OPTION, 'region'))
      else:
        view.region = default_region
    return errors

  def validate_worker_region_zone(self, view):
    """Validates Dataflow worker region and zone arguments are consistent."""
    errors = []
    if view.zone and (view.worker_region or view.worker_zone):
      errors.extend(
          self._validate_error(
              'Cannot use deprecated flag --zone along with worker_region or '
              'worker_zone.'))
    if self.options.view_as(DebugOptions).lookup_experiment('worker_region')\
        and (view.worker_region or view.worker_zone):
      errors.extend(
          self._validate_error(
              'Cannot use deprecated experiment worker_region along with '
              'worker_region or worker_zone.'))
    if view.worker_region and view.worker_zone:
      errors.extend(
          self._validate_error(
              'worker_region and worker_zone are mutually exclusive.'))
    if view.zone:
      _LOGGER.warning(
          'Option --zone is deprecated. Please use --worker_zone instead.')
      view.worker_zone = view.zone
      view.zone = None
    return errors

  def validate_optional_argument_positive(self, view, arg_name):
    """Validates that an optional argument (if set) has a positive value."""
    arg = getattr(view, arg_name, None)
    if arg is not None and int(arg) <= 0:
      return self._validate_error(self.ERR_INVALID_NOT_POSITIVE, arg, arg_name)
    return []

  def validate_test_matcher(self, view, arg_name):
    """Validates that on_success_matcher argument if set.

    Validates that on_success_matcher is unpicklable and is instance
    of `hamcrest.core.base_matcher.BaseMatcher`.
    """
    # This is a test only method and requires hamcrest
    from hamcrest.core.base_matcher import BaseMatcher
    pickled_matcher = view.on_success_matcher
    errors = []
    try:
      matcher = pickler.loads(pickled_matcher)
      if not isinstance(matcher, BaseMatcher):
        errors.extend(
            self._validate_error(
                self.ERR_INVALID_TEST_MATCHER_TYPE, matcher, arg_name))
    except:  # pylint: disable=bare-except
      errors.extend(
          self._validate_error(
              self.ERR_INVALID_TEST_MATCHER_UNPICKLABLE,
              pickled_matcher,
              arg_name))
    return errors

  def validate_environment_options(self, view):
    """Validates portable environment options."""
    errors = []
    actual_environment_type = (
        view.environment_type.upper() if view.environment_type else None)
    for environment_type, required in self.REQUIRED_ENVIRONMENT_OPTIONS.items():
      found_required_options = [
          opt for opt in required
          if view.lookup_environment_option(opt) is not None
      ]
      found_optional_options = [
          opt for opt in self.OPTIONAL_ENVIRONMENT_OPTIONS[environment_type]
          if view.lookup_environment_option(opt) is not None
      ]
      found_options = found_required_options + found_optional_options
      if environment_type == actual_environment_type:
        if view.environment_config:
          if found_options:
            errors.extend(
                self._validate_error(
                    self.ERR_ENVIRONMENT_CONFIG, ', '.join(found_options)))
        else:
          missing_options = set(required).difference(
              set(found_required_options))
          for opt in missing_options:
            errors.extend(
                self._validate_error(
                    self.ERR_MISSING_REQUIRED_ENVIRONMENT_OPTION,
                    opt,
                    environment_type))
      else:
        # Environment options classes are mutually exclusive.
        for opt in found_options:
          errors.extend(
              self._validate_error(
                  self.ERR_INVALID_ENVIRONMENT, opt, actual_environment_type))
    if actual_environment_type == 'LOOPBACK' and view.environment_config:
      errors.extend(
          self._validate_error(
              self.ERR_INVALID_ENVIRONMENT, 'environment_config', 'LOOPBACK'))
    return errors
