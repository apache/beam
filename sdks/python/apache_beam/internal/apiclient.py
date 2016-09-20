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

"""Dataflow client utility functions."""

import codecs
import json
import logging
import os
import re
import time


from apitools.base.py import encoding
from apitools.base.py import exceptions

from apache_beam import utils
from apache_beam.internal.auth import get_service_credentials
from apache_beam.internal.json_value import to_json_value
from apache_beam.transforms import cy_combiners
from apache_beam.utils import dependency
from apache_beam.utils import retry
from apache_beam.utils.dependency import get_required_container_version
from apache_beam.utils.dependency import get_sdk_name_and_version
from apache_beam.utils.names import PropertyNames
from apache_beam.utils.options import GoogleCloudOptions
from apache_beam.utils.options import StandardOptions
from apache_beam.utils.options import WorkerOptions

from apache_beam.internal.clients import storage
import apache_beam.internal.clients.dataflow as dataflow


BIGQUERY_API_SERVICE = 'bigquery.googleapis.com'
COMPUTE_API_SERVICE = 'compute.googleapis.com'
STORAGE_API_SERVICE = 'storage.googleapis.com'


class Step(object):
  """Wrapper for a dataflow Step protobuf."""

  def __init__(self, step_kind, step_name):
    self.step_kind = step_kind
    self.step_name = step_name
    self.proto = dataflow.Step(kind=step_kind, name=step_name)
    self.proto.properties = {}

  def add_property(self, name, value, with_type=False):
    self.proto.properties.additionalProperties.append(
        dataflow.Step.PropertiesValue.AdditionalProperty(
            key=name, value=to_json_value(value, with_type=with_type)))

  def _get_outputs(self):
    """Returns a list of all output labels for a step."""
    outputs = []
    for p in self.proto.properties.additionalProperties:
      if p.key == PropertyNames.OUTPUT_INFO:
        for entry in p.value.array_value.entries:
          for entry_prop in entry.object_value.properties:
            if entry_prop.key == PropertyNames.OUTPUT_NAME:
              outputs.append(entry_prop.value.string_value)
    return outputs

  def get_output(self, tag=None):
    """Returns name if it is one of the outputs or first output if name is None.

    Args:
      tag: tag of the output as a string or None if we want to get the
        name of the first output.

    Returns:
      The name of the output associated with the tag or the first output
      if tag was None.

    Raises:
      ValueError: if the tag does not exist within outputs.
    """
    outputs = self._get_outputs()
    if tag is None:
      return outputs[0]
    else:
      name = '%s_%s' % (PropertyNames.OUT, tag)
      if name not in outputs:
        raise ValueError(
            'Cannot find named output: %s in %s.' % (name, outputs))
      return name


class Environment(object):
  """Wrapper for a dataflow Environment protobuf."""

  def __init__(self, packages, options, environment_version):
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    self.worker_options = options.view_as(WorkerOptions)
    self.proto = dataflow.Environment()
    self.proto.clusterManagerApiService = COMPUTE_API_SERVICE
    self.proto.dataset = '%s/cloud_dataflow' % BIGQUERY_API_SERVICE
    self.proto.tempStoragePrefix = (
        self.google_cloud_options.temp_location.replace('gs:/',
                                                        STORAGE_API_SERVICE))
    # User agent information.
    self.proto.userAgent = dataflow.Environment.UserAgentValue()
    self.local = 'localhost' in self.google_cloud_options.dataflow_endpoint

    if self.google_cloud_options.service_account_email:
      self.proto.serviceAccountEmail = (
          self.google_cloud_options.service_account_email)

    sdk_name, version_string = get_sdk_name_and_version()

    self.proto.userAgent.additionalProperties.extend([
        dataflow.Environment.UserAgentValue.AdditionalProperty(
            key='name',
            value=to_json_value(sdk_name)),
        dataflow.Environment.UserAgentValue.AdditionalProperty(
            key='version', value=to_json_value(version_string))])
    # Version information.
    self.proto.version = dataflow.Environment.VersionValue()
    if self.standard_options.streaming:
      job_type = 'PYTHON_STREAMING'
    else:
      job_type = 'PYTHON_BATCH'
    self.proto.version.additionalProperties.extend([
        dataflow.Environment.VersionValue.AdditionalProperty(
            key='job_type',
            value=to_json_value(job_type)),
        dataflow.Environment.VersionValue.AdditionalProperty(
            key='major', value=to_json_value(environment_version))])
    # Worker pool(s) information.
    package_descriptors = []
    for package in packages:
      package_descriptors.append(
          dataflow.Package(
              location='%s/%s' % (
                  self.google_cloud_options.staging_location.replace(
                      'gs:/', STORAGE_API_SERVICE),
                  package),
              name=package))

    pool = dataflow.WorkerPool(
        kind='local' if self.local else 'harness',
        packages=package_descriptors,
        taskrunnerSettings=dataflow.TaskRunnerSettings(
            parallelWorkerSettings=dataflow.WorkerSettings(
                baseUrl='https://dataflow.googleapis.com',
                servicePath=self.google_cloud_options.dataflow_endpoint)))
    pool.autoscalingSettings = dataflow.AutoscalingSettings()
    # Set worker pool options received through command line.
    if self.worker_options.num_workers:
      pool.numWorkers = self.worker_options.num_workers
    if self.worker_options.max_num_workers:
      pool.autoscalingSettings.maxNumWorkers = (
          self.worker_options.max_num_workers)
    if self.worker_options.autoscaling_algorithm:
      values_enum = dataflow.AutoscalingSettings.AlgorithmValueValuesEnum
      pool.autoscalingSettings.algorithm = {
          'NONE': values_enum.AUTOSCALING_ALGORITHM_NONE,
          'THROUGHPUT_BASED': values_enum.AUTOSCALING_ALGORITHM_BASIC,
      }.get(self.worker_options.autoscaling_algorithm)
    if self.worker_options.machine_type:
      pool.machineType = self.worker_options.machine_type
    if self.worker_options.disk_size_gb:
      pool.diskSizeGb = self.worker_options.disk_size_gb
    if self.worker_options.disk_type:
      pool.diskType = self.worker_options.disk_type
    if self.worker_options.disk_source_image:
      pool.diskSourceImage = self.worker_options.disk_source_image
    if self.worker_options.zone:
      pool.zone = self.worker_options.zone
    if self.worker_options.network:
      pool.network = self.worker_options.network
    if self.worker_options.worker_harness_container_image:
      pool.workerHarnessContainerImage = (
          self.worker_options.worker_harness_container_image)
    else:
      # Default to using the worker harness container image for the current SDK
      # version.
      pool.workerHarnessContainerImage = (
          'dataflow.gcr.io/v1beta3/python:%s' %
          get_required_container_version())
    if self.worker_options.teardown_policy:
      if self.worker_options.teardown_policy == 'TEARDOWN_NEVER':
        pool.teardownPolicy = (
            dataflow.WorkerPool.TeardownPolicyValueValuesEnum.TEARDOWN_NEVER)
      elif self.worker_options.teardown_policy == 'TEARDOWN_ALWAYS':
        pool.teardownPolicy = (
            dataflow.WorkerPool.TeardownPolicyValueValuesEnum.TEARDOWN_ALWAYS)
      elif self.worker_options.teardown_policy == 'TEARDOWN_ON_SUCCESS':
        pool.teardownPolicy = (
            dataflow.WorkerPool
            .TeardownPolicyValueValuesEnum.TEARDOWN_ON_SUCCESS)

    if self.standard_options.streaming:
      # Use separate data disk for streaming.
      disk = dataflow.Disk()
      if self.local:
        disk.diskType = 'local'
      # TODO(ccy): allow customization of disk.
      pool.dataDisks.append(disk)
    self.proto.workerPools.append(pool)

    sdk_pipeline_options = options.get_all_options()
    if sdk_pipeline_options:
      self.proto.sdkPipelineOptions = (
          dataflow.Environment.SdkPipelineOptionsValue())

      for k, v in sdk_pipeline_options.iteritems():
        if v is not None:
          self.proto.sdkPipelineOptions.additionalProperties.append(
              dataflow.Environment.SdkPipelineOptionsValue.AdditionalProperty(
                  key=k, value=to_json_value(v)))


class Job(object):
  """Wrapper for a dataflow Job protobuf."""

  def __str__(self):
    def encode_shortstrings(input_buffer, errors='strict'):
      """Encoder (from Unicode) that suppresses long base64 strings."""
      original_len = len(input_buffer)
      if original_len > 150:
        if self.base64_str_re.match(input_buffer):
          input_buffer = '<string of %d bytes>' % original_len
          input_buffer = input_buffer.encode('ascii', errors=errors)
        else:
          matched = self.coder_str_re.match(input_buffer)
          if matched:
            input_buffer = '%s<string of %d bytes>' % (
                matched.group(1), matched.end(2) - matched.start(2))
            input_buffer = input_buffer.encode('ascii', errors=errors)
      return input_buffer, original_len

    def decode_shortstrings(input_buffer, errors='strict'):
      """Decoder (to Unicode) that suppresses long base64 strings."""
      shortened, length = encode_shortstrings(input_buffer, errors)
      return unicode(shortened), length

    def shortstrings_registerer(encoding_name):
      if encoding_name == 'shortstrings':
        return codecs.CodecInfo(name='shortstrings',
                                encode=encode_shortstrings,
                                decode=decode_shortstrings)
      return None

    codecs.register(shortstrings_registerer)

    # Use json "dump string" method to get readable formatting;
    # further modify it to not output too-long strings, aimed at the
    # 10,000+ character hex-encoded "serialized_fn" values.
    return json.dumps(
        json.loads(encoding.MessageToJson(self.proto), encoding='shortstrings'),
        indent=2, sort_keys=True)

  def __init__(self, options):
    self.options = options
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    required_google_cloud_options = ['project', 'job_name', 'staging_location']
    missing = [
        option for option in required_google_cloud_options
        if not getattr(self.google_cloud_options, option)]
    if missing:
      raise ValueError(
          'Missing required configuration parameters: %s' % missing)

    if not self.google_cloud_options.temp_location:
      logging.info('Defaulting to the staging_location as temp_location: %s',
                   self.google_cloud_options.staging_location)
      (self.google_cloud_options
       .temp_location) = self.google_cloud_options.staging_location

    # Make the staging and temp locations job name and time specific. This is
    # needed to avoid clashes between job submissions using the same staging
    # area or team members using same job names. This method is not entirely
    # foolproof since two job submissions with same name can happen at exactly
    # the same time. However the window is extremely small given that
    # time.time() has at least microseconds granularity. We add the suffix only
    # for GCS staging locations where the potential for such clashes is high.
    if self.google_cloud_options.staging_location.startswith('gs://'):
      path_suffix = '%s.%f' % (self.google_cloud_options.job_name, time.time())
      self.google_cloud_options.staging_location = utils.path.join(
          self.google_cloud_options.staging_location, path_suffix)
      self.google_cloud_options.temp_location = utils.path.join(
          self.google_cloud_options.temp_location, path_suffix)
    self.proto = dataflow.Job(name=self.google_cloud_options.job_name)
    if self.options.view_as(StandardOptions).streaming:
      self.proto.type = dataflow.Job.TypeValueValuesEnum.JOB_TYPE_STREAMING
    else:
      self.proto.type = dataflow.Job.TypeValueValuesEnum.JOB_TYPE_BATCH
    self.base64_str_re = re.compile(r'^[A-Za-z0-9+/]*=*$')
    self.coder_str_re = re.compile(r'^([A-Za-z]+\$)([A-Za-z0-9+/]*=*)$')


class DataflowApplicationClient(object):
  """A Dataflow API client used by application code to create and query jobs."""

  def __init__(self, options, environment_version):
    """Initializes a Dataflow API client object."""
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    self.environment_version = environment_version
    if self.google_cloud_options.no_auth:
      credentials = None
    else:
      credentials = get_service_credentials()
    self._client = dataflow.DataflowV1b3(
        url=self.google_cloud_options.dataflow_endpoint,
        credentials=credentials,
        get_credentials=(not self.google_cloud_options.no_auth))
    self._storage_client = storage.StorageV1(
        url='https://www.googleapis.com/storage/v1',
        credentials=credentials,
        get_credentials=(not self.google_cloud_options.no_auth))

  # TODO(silviuc): Refactor so that retry logic can be applied.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def _gcs_file_copy(self, from_path, to_path):
    to_folder, to_name = os.path.split(to_path)
    with open(from_path, 'rb') as f:
      self.stage_file(to_folder, to_name, f)

  def stage_file(self, gcs_or_local_path, file_name, stream,
                 mime_type='application/octet-stream'):
    """Stages a file at a GCS or local path with stream-supplied contents."""
    if not gcs_or_local_path.startswith('gs://'):
      local_path = os.path.join(gcs_or_local_path, file_name)
      logging.info('Staging file locally to %s', local_path)
      with open(local_path, 'wb') as f:
        f.write(stream.read())
      return
    gcs_location = gcs_or_local_path + '/' + file_name
    bucket, name = gcs_location[5:].split('/', 1)

    request = storage.StorageObjectsInsertRequest(
        bucket=bucket, name=name)
    logging.info('Starting GCS upload to %s...', gcs_location)
    upload = storage.Upload(stream, mime_type)
    try:
      response = self._storage_client.objects.Insert(request, upload=upload)
    except exceptions.HttpError as e:
      reportable_errors = {
          403: 'access denied',
          404: 'bucket not found',
      }
      if e.status_code in reportable_errors:
        raise IOError(('Could not upload to GCS path %s: %s. Please verify '
                       'that credentials are valid and that you have write '
                       'access to the specified path. Stale credentials can be '
                       'refreshed by executing "gcloud auth login".') %
                      (gcs_or_local_path, reportable_errors[e.status_code]))
      raise
    logging.info('Completed GCS upload to %s', gcs_location)
    return response

  # TODO(silviuc): Refactor so that retry logic can be applied.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def create_job(self, job):
    """Submits for remote execution a job described by the workflow proto."""
    # Stage job resources and add an environment proto with their paths.
    resources = dependency.stage_job_resources(
        job.options, file_copy=self._gcs_file_copy)
    job.proto.environment = Environment(
        packages=resources, options=job.options,
        environment_version=self.environment_version).proto
    # TODO(silviuc): Remove the debug logging eventually.
    logging.info('JOB: %s', job)
    request = dataflow.DataflowProjectsJobsCreateRequest()

    request.projectId = self.google_cloud_options.project
    request.job = job.proto

    try:
      response = self._client.projects_jobs.Create(request)
    except exceptions.BadStatusCodeError as e:
      logging.error('HTTP status %d trying to create job'
                    ' at dataflow service endpoint %s',
                    e.response.status,
                    self.google_cloud_options.dataflow_endpoint)
      logging.fatal('details of server error: %s', e)
      raise
    logging.info('Create job: %s', response)
    # The response is a Job proto with the id for the new job.
    logging.info('Created job with id: [%s]', response.id)
    logging.info(
        'To access the Dataflow monitoring console, please navigate to '
        'https://console.developers.google.com/project/%s/dataflow/job/%s',
        self.google_cloud_options.project, response.id)

    return response

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def modify_job_state(self, job_id, new_state):
    """Modify the run state of the job.

    Args:
      job_id: The id of the job.
      new_state: A string representing the new desired state. It could be set to
      either 'JOB_STATE_DONE', 'JOB_STATE_CANCELLED' or 'JOB_STATE_DRAINING'.

    Returns:
      True if the job was modified successfully.
    """
    if new_state == 'JOB_STATE_DONE':
      new_state = dataflow.Job.RequestedStateValueValuesEnum.JOB_STATE_DONE
    elif new_state == 'JOB_STATE_CANCELLED':
      new_state = dataflow.Job.RequestedStateValueValuesEnum.JOB_STATE_CANCELLED
    elif new_state == 'JOB_STATE_DRAINING':
      new_state = dataflow.Job.RequestedStateValueValuesEnum.JOB_STATE_DRAINING
    else:
      # Other states could only be set by the service.
      return False

    request = dataflow.DataflowProjectsJobsUpdateRequest()
    request.jobId = job_id
    request.projectId = self.google_cloud_options.project
    request.job = dataflow.Job(requestedState=new_state)

    self._client.projects_jobs.Update(request)
    return True

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def get_job(self, job_id):
    """Gets the job status for a submitted job.

    Args:
      job_id: A string representing the job_id for the workflow as returned
        by the a create_job() request.

    Returns:
      A Job proto. See below for interesting fields.

    The Job proto returned from a get_job() request contains some interesting
    fields:
      currentState: An object representing the current state of the job. The
        string representation of the object (str() result) has the following
        possible values: JOB_STATE_UNKNONW, JOB_STATE_STOPPED,
        JOB_STATE_RUNNING, JOB_STATE_DONE, JOB_STATE_FAILED,
        JOB_STATE_CANCELLED.
      createTime: UTC time when the job was created
        (e.g. '2015-03-10T00:01:53.074Z')
      currentStateTime: UTC time for the current state of the job.
    """
    request = dataflow.DataflowProjectsJobsGetRequest()
    request.jobId = job_id
    request.projectId = self.google_cloud_options.project
    response = self._client.projects_jobs.Get(request)
    return response

  @retry.with_exponential_backoff()  # Using retry defaults from utils/retry.py
  def list_messages(
      self, job_id, start_time=None, end_time=None, page_token=None,
      minimum_importance=None):
    """List messages associated with the execution of a job.

    Args:
      job_id: A string representing the job_id for the workflow as returned
        by the a create_job() request.
      start_time: If specified, only messages generated after the start time
        will be returned, otherwise all messages since job started will be
        returned. The value is a string representing UTC time
        (e.g., '2015-08-18T21:03:50.644Z')
      end_time: If specified, only messages generated before the end time
        will be returned, otherwise all messages up to current time will be
        returned. The value is a string representing UTC time
        (e.g., '2015-08-18T21:03:50.644Z')
      page_token: A string to be used as next page token if the list call
        returned paginated results.
      minimum_importance: Filter for messages based on importance. The possible
        string values in increasing order of importance are: JOB_MESSAGE_DEBUG,
        JOB_MESSAGE_DETAILED, JOB_MESSAGE_BASIC, JOB_MESSAGE_WARNING,
        JOB_MESSAGE_ERROR. For example, a filter set on warning will allow only
        warnings and errors and exclude all others.

    Returns:
      A tuple consisting of a list of JobMessage instances and a
      next page token string.

    Raises:
      RuntimeError: if an unexpected value for the message_importance argument
        is used.

    The JobMessage objects returned by the call contain the following  fields:
      id: A unique string identifier for the message.
      time: A string representing the UTC time of the message
        (e.g., '2015-08-18T21:03:50.644Z')
      messageImportance: An enumeration value for the message importance. The
        value if converted to string will have the following possible values:
        JOB_MESSAGE_DEBUG, JOB_MESSAGE_DETAILED, JOB_MESSAGE_BASIC,
        JOB_MESSAGE_WARNING, JOB_MESSAGE_ERROR.
     messageText: A message string.
    """
    request = dataflow.DataflowProjectsJobsMessagesListRequest(
        jobId=job_id, projectId=self.google_cloud_options.project)
    if page_token is not None:
      request.pageToken = page_token
    if start_time is not None:
      request.startTime = start_time
    if end_time is not None:
      request.endTime = end_time
    if minimum_importance is not None:
      if minimum_importance == 'JOB_MESSAGE_DEBUG':
        request.minimumImportance = (
            dataflow.DataflowProjectsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_DEBUG)
      elif minimum_importance == 'JOB_MESSAGE_DETAILED':
        request.minimumImportance = (
            dataflow.DataflowProjectsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_DETAILED)
      elif minimum_importance == 'JOB_MESSAGE_BASIC':
        request.minimumImportance = (
            dataflow.DataflowProjectsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_BASIC)
      elif minimum_importance == 'JOB_MESSAGE_WARNING':
        request.minimumImportance = (
            dataflow.DataflowProjectsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_WARNING)
      elif minimum_importance == 'JOB_MESSAGE_ERROR':
        request.minimumImportance = (
            dataflow.DataflowProjectsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_ERROR)
      else:
        raise RuntimeError(
            'Unexpected value for minimum_importance argument: %r',
            minimum_importance)
    response = self._client.projects_jobs_messages.List(request)
    return response.jobMessages, response.nextPageToken


def set_scalar(accumulator, metric_update):
  metric_update.scalar = to_json_value(accumulator.value, with_type=True)


def set_mean(accumulator, metric_update):
  if accumulator.count:
    metric_update.meanSum = to_json_value(accumulator.sum, with_type=True)
    metric_update.meanCount = to_json_value(accumulator.count, with_type=True)
  else:
    # A denominator of 0 will raise an error in the service.
    # What it means is we have nothing to report yet, so don't.
    metric_update.kind = None


# To enable a counter on the service, add it to this dictionary.
metric_translations = {
    cy_combiners.CountCombineFn: ('sum', set_scalar),
    cy_combiners.SumInt64Fn: ('sum', set_scalar),
    cy_combiners.MinInt64Fn: ('min', set_scalar),
    cy_combiners.MaxInt64Fn: ('max', set_scalar),
    cy_combiners.MeanInt64Fn: ('mean', set_mean),
    cy_combiners.SumFloatFn: ('sum', set_scalar),
    cy_combiners.MinFloatFn: ('min', set_scalar),
    cy_combiners.MaxFloatFn: ('max', set_scalar),
    cy_combiners.MeanFloatFn: ('mean', set_mean),
    cy_combiners.AllCombineFn: ('and', set_scalar),
    cy_combiners.AnyCombineFn: ('or', set_scalar),
}
