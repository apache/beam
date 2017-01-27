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
"""

import re

from apache_beam.utils.options import DebugOptions
from apache_beam.utils.options import GoogleCloudOptions
from apache_beam.utils.options import SetupOptions
from apache_beam.utils.options import StandardOptions
from apache_beam.utils.options import TypeOptions
from apache_beam.utils.options import WorkerOptions


class PipelineOptionsValidator(object):
  """Validates PipelineOptions.

  Goes through a list of known PipelineOption subclassess and calls::

    validate(validator)

  if one is implemented. Aggregates a list of validation errors from all and
  returns an aggregated list.
  """

  # Validator will call validate on these subclasses of PipelineOptions
  OPTIONS = [DebugOptions, GoogleCloudOptions, SetupOptions, StandardOptions,
             TypeOptions, WorkerOptions]

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
  ERR_INVALID_NOT_POSITIVE = ('Invalid value (%s) for option: %s. Value needs '
                              'to be positive.')

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
      if 'validate' in cls.__dict__:
        errors.extend(self.options.view_as(cls).validate(self))
    return errors

  def is_service_runner(self):
    """True if pipeline will execute on the Google Cloud Dataflow service."""
    is_service_runner = (self.runner is not None and
                         type(self.runner).__name__ in [
                             'DataflowPipelineRunner',
                             'BlockingDataflowPipelineRunner'])

    dataflow_endpoint = (
        self.options.view_as(GoogleCloudOptions).dataflow_endpoint)
    is_service_endpoint = (dataflow_endpoint is not None and
                           self.is_full_string_match(
                               self.ENDPOINT_PATTERN, dataflow_endpoint))

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
    job_name = view.job_name
    if (job_name is None or
        not self.is_full_string_match(self.JOB_PATTERN, job_name)):
      errors.extend(self._validate_error(self.ERR_INVALID_JOB_NAME, job_name))

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
    return errors

  def validate_optional_argument_positive(self, view, arg_name):
    """Validates that an optional argument (if set) has a positive value."""
    arg = getattr(view, arg_name, None)
    if arg is not None and int(arg) <= 0:
      return self._validate_error(self.ERR_INVALID_NOT_POSITIVE, arg, arg_name)
    return []
