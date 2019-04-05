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

""" For internal use only. No backwards compatibility guarantees.

Dataflow client utility functions."""

from __future__ import absolute_import

import codecs
import getpass
import io
import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime

from builtins import object
from past.builtins import unicode

from apitools.base.py import encoding
from apitools.base.py import exceptions

from apache_beam import version as beam_version
from apache_beam.internal.gcp.auth import get_service_credentials
from apache_beam.internal.gcp.json_value import to_json_value
from apache_beam.internal.http_client import get_new_http
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.internal.clients import storage
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.dataflow.internal.clients import dataflow
from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.runners.internal import names as shared_names
from apache_beam.runners.portability.stager import Stager
from apache_beam.transforms import DataflowDistributionCounter
from apache_beam.transforms import cy_combiners
from apache_beam.transforms.display import DisplayData
from apache_beam.utils import retry

# Environment version information. It is passed to the service during a
# a job submission and is used by the service to establish what features
# are expected by the workers.
_LEGACY_ENVIRONMENT_MAJOR_VERSION = '7'
_FNAPI_ENVIRONMENT_MAJOR_VERSION = '7'


class Step(object):
  """Wrapper for a dataflow Step protobuf."""

  def __init__(self, step_kind, step_name, additional_properties=None):
    self.step_kind = step_kind
    self.step_name = step_name
    self.proto = dataflow.Step(kind=step_kind, name=step_name)
    self.proto.properties = {}
    self._additional_properties = []

    if additional_properties is not None:
      for (n, v, t) in additional_properties:
        self.add_property(n, v, t)

  def add_property(self, name, value, with_type=False):
    self._additional_properties.append((name, value, with_type))
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

  def __reduce__(self):
    """Reduce hook for pickling the Step class more easily."""
    return (Step, (self.step_kind, self.step_name, self._additional_properties))

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

  def __init__(self, packages, options, environment_version, pipeline_url):
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    self.worker_options = options.view_as(WorkerOptions)
    self.debug_options = options.view_as(DebugOptions)
    self.pipeline_url = pipeline_url
    self.proto = dataflow.Environment()
    self.proto.clusterManagerApiService = GoogleCloudOptions.COMPUTE_API_SERVICE
    self.proto.dataset = '{}/cloud_dataflow'.format(
        GoogleCloudOptions.BIGQUERY_API_SERVICE)
    self.proto.tempStoragePrefix = (
        self.google_cloud_options.temp_location.replace(
            'gs:/',
            GoogleCloudOptions.STORAGE_API_SERVICE))
    # User agent information.
    self.proto.userAgent = dataflow.Environment.UserAgentValue()
    self.local = 'localhost' in self.google_cloud_options.dataflow_endpoint

    if self.google_cloud_options.service_account_email:
      self.proto.serviceAccountEmail = (
          self.google_cloud_options.service_account_email)

    self.proto.userAgent.additionalProperties.extend([
        dataflow.Environment.UserAgentValue.AdditionalProperty(
            key='name',
            value=to_json_value(self._get_python_sdk_name())),
        dataflow.Environment.UserAgentValue.AdditionalProperty(
            key='version', value=to_json_value(beam_version.__version__))])
    # Version information.
    self.proto.version = dataflow.Environment.VersionValue()
    _verify_interpreter_version_is_supported(options)
    if self.standard_options.streaming:
      job_type = 'FNAPI_STREAMING'
    else:
      if _use_fnapi(options):
        job_type = 'FNAPI_BATCH'
      else:
        job_type = 'PYTHON_BATCH'
    self.proto.version.additionalProperties.extend([
        dataflow.Environment.VersionValue.AdditionalProperty(
            key='job_type',
            value=to_json_value(job_type)),
        dataflow.Environment.VersionValue.AdditionalProperty(
            key='major', value=to_json_value(environment_version))])
    # TODO: Use enumerated type instead of strings for job types.
    if job_type.startswith('FNAPI_'):
      runner_harness_override = (
          get_runner_harness_container_image())
      self.debug_options.experiments = self.debug_options.experiments or []
      if runner_harness_override:
        self.debug_options.experiments.append(
            'runner_harness_container_image=' + runner_harness_override)
      # Add use_multiple_sdk_containers flag if its not already present. Do not
      # add the flag if 'no_use_multiple_sdk_containers' is present.
      # TODO: Cleanup use_multiple_sdk_containers once we deprecate Python SDK
      # till version 2.4.
      debug_options_experiments = self.debug_options.experiments
      if ('use_multiple_sdk_containers' not in debug_options_experiments and
          'no_use_multiple_sdk_containers' not in debug_options_experiments):
        self.debug_options.experiments.append('use_multiple_sdk_containers')
    # FlexRS
    if self.google_cloud_options.flexrs_goal == 'COST_OPTIMIZED':
      self.proto.flexResourceSchedulingGoal = (
          dataflow.Environment.FlexResourceSchedulingGoalValueValuesEnum.
          FLEXRS_COST_OPTIMIZED)
    elif self.google_cloud_options.flexrs_goal == 'SPEED_OPTIMIZED':
      self.proto.flexResourceSchedulingGoal = (
          dataflow.Environment.FlexResourceSchedulingGoalValueValuesEnum.
          FLEXRS_SPEED_OPTIMIZED)
    # Experiments
    if self.debug_options.experiments:
      for experiment in self.debug_options.experiments:
        self.proto.experiments.append(experiment)
    # Worker pool(s) information.
    package_descriptors = []
    for package in packages:
      package_descriptors.append(
          dataflow.Package(
              location='%s/%s' % (
                  self.google_cloud_options.staging_location.replace(
                      'gs:/', GoogleCloudOptions.STORAGE_API_SERVICE),
                  package),
              name=package))

    pool = dataflow.WorkerPool(
        kind='local' if self.local else 'harness',
        packages=package_descriptors,
        taskrunnerSettings=dataflow.TaskRunnerSettings(
            parallelWorkerSettings=dataflow.WorkerSettings(
                baseUrl=GoogleCloudOptions.DATAFLOW_ENDPOINT,
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
    if self.worker_options.zone:
      pool.zone = self.worker_options.zone
    if self.worker_options.network:
      pool.network = self.worker_options.network
    if self.worker_options.subnetwork:
      pool.subnetwork = self.worker_options.subnetwork
    if self.worker_options.worker_harness_container_image:
      pool.workerHarnessContainerImage = (
          self.worker_options.worker_harness_container_image)
    else:
      pool.workerHarnessContainerImage = (
          get_default_container_image_for_current_sdk(job_type))
    if self.worker_options.use_public_ips is not None:
      if self.worker_options.use_public_ips:
        pool.ipConfiguration = (
            dataflow.WorkerPool
            .IpConfigurationValueValuesEnum.WORKER_IP_PUBLIC)
      else:
        pool.ipConfiguration = (
            dataflow.WorkerPool
            .IpConfigurationValueValuesEnum.WORKER_IP_PRIVATE)

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

      options_dict = {k: v
                      for k, v in sdk_pipeline_options.items()
                      if v is not None}
      options_dict["pipelineUrl"] = pipeline_url
      self.proto.sdkPipelineOptions.additionalProperties.append(
          dataflow.Environment.SdkPipelineOptionsValue.AdditionalProperty(
              key='options', value=to_json_value(options_dict)))

      dd = DisplayData.create_from_options(options)
      items = [item.get_dict() for item in dd.items]
      self.proto.sdkPipelineOptions.additionalProperties.append(
          dataflow.Environment.SdkPipelineOptionsValue.AdditionalProperty(
              key='display_data', value=to_json_value(items)))

  def _get_python_sdk_name(self):
    python_version = '%d.%d' % (sys.version_info[0], sys.version_info[1])
    return 'Apache Beam Python %s SDK' % python_version


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

  @staticmethod
  def _build_default_job_name(user_name):
    """Generates a default name for a job.

    user_name is lowercased, and any characters outside of [-a-z0-9]
    are removed. If necessary, the user_name is truncated to shorten
    the job name to 63 characters."""
    user_name = re.sub('[^-a-z0-9]', '', user_name.lower())
    date_component = datetime.utcnow().strftime('%m%d%H%M%S-%f')
    app_user_name = 'beamapp-{}'.format(user_name)
    job_name = '{}-{}'.format(app_user_name, date_component)
    if len(job_name) > 63:
      job_name = '{}-{}'.format(app_user_name[:-(len(job_name) - 63)],
                                date_component)
    return job_name

  @staticmethod
  def default_job_name(job_name):
    if job_name is None:
      job_name = Job._build_default_job_name(getpass.getuser())
    return job_name

  def __init__(self, options, proto_pipeline):
    self.options = options
    self.proto_pipeline = proto_pipeline
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    if not self.google_cloud_options.job_name:
      self.google_cloud_options.job_name = self.default_job_name(
          self.google_cloud_options.job_name)

    required_google_cloud_options = ['project', 'job_name', 'temp_location']
    missing = [
        option for option in required_google_cloud_options
        if not getattr(self.google_cloud_options, option)]
    if missing:
      raise ValueError(
          'Missing required configuration parameters: %s' % missing)

    if not self.google_cloud_options.staging_location:
      logging.info('Defaulting to the temp_location as staging_location: %s',
                   self.google_cloud_options.temp_location)
      (self.google_cloud_options
       .staging_location) = self.google_cloud_options.temp_location

    # Make the staging and temp locations job name and time specific. This is
    # needed to avoid clashes between job submissions using the same staging
    # area or team members using same job names. This method is not entirely
    # foolproof since two job submissions with same name can happen at exactly
    # the same time. However the window is extremely small given that
    # time.time() has at least microseconds granularity. We add the suffix only
    # for GCS staging locations where the potential for such clashes is high.
    if self.google_cloud_options.staging_location.startswith('gs://'):
      path_suffix = '%s.%f' % (self.google_cloud_options.job_name, time.time())
      self.google_cloud_options.staging_location = FileSystems.join(
          self.google_cloud_options.staging_location, path_suffix)
      self.google_cloud_options.temp_location = FileSystems.join(
          self.google_cloud_options.temp_location, path_suffix)

    self.proto = dataflow.Job(name=self.google_cloud_options.job_name)
    if self.options.view_as(StandardOptions).streaming:
      self.proto.type = dataflow.Job.TypeValueValuesEnum.JOB_TYPE_STREAMING
    else:
      self.proto.type = dataflow.Job.TypeValueValuesEnum.JOB_TYPE_BATCH
    if self.google_cloud_options.update:
      self.proto.replaceJobId = self.job_id_for_name(self.proto.name)

    # Labels.
    if self.google_cloud_options.labels:
      self.proto.labels = dataflow.Job.LabelsValue()
      for label in self.google_cloud_options.labels:
        parts = label.split('=', 1)
        key = parts[0]
        value = parts[1] if len(parts) > 1 else ''
        self.proto.labels.additionalProperties.append(
            dataflow.Job.LabelsValue.AdditionalProperty(key=key, value=value))

    self.base64_str_re = re.compile(r'^[A-Za-z0-9+/]*=*$')
    self.coder_str_re = re.compile(r'^([A-Za-z]+\$)([A-Za-z0-9+/]*=*)$')

  def job_id_for_name(self, job_name):
    return DataflowApplicationClient(
        self.google_cloud_options).job_id_for_name(job_name)

  def json(self):
    return encoding.MessageToJson(self.proto)

  def __reduce__(self):
    """Reduce hook for pickling the Job class more easily."""
    return (Job, (self.options,))


class DataflowApplicationClient(object):
  """A Dataflow API client used by application code to create and query jobs."""

  def __init__(self, options):
    """Initializes a Dataflow API client object."""
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)

    if _use_fnapi(options):
      self.environment_version = _FNAPI_ENVIRONMENT_MAJOR_VERSION
    else:
      self.environment_version = _LEGACY_ENVIRONMENT_MAJOR_VERSION

    if self.google_cloud_options.no_auth:
      credentials = None
    else:
      credentials = get_service_credentials()

    http_client = get_new_http()
    self._client = dataflow.DataflowV1b3(
        url=self.google_cloud_options.dataflow_endpoint,
        credentials=credentials,
        get_credentials=(not self.google_cloud_options.no_auth),
        http=http_client,
        response_encoding=get_response_encoding())
    self._storage_client = storage.StorageV1(
        url='https://www.googleapis.com/storage/v1',
        credentials=credentials,
        get_credentials=(not self.google_cloud_options.no_auth),
        http=http_client,
        response_encoding=get_response_encoding())

  # TODO(silviuc): Refactor so that retry logic can be applied.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def _gcs_file_copy(self, from_path, to_path):
    to_folder, to_name = os.path.split(to_path)
    with open(from_path, 'rb') as f:
      self.stage_file(to_folder, to_name, f)

  def _stage_resources(self, options):
    google_cloud_options = options.view_as(GoogleCloudOptions)
    if google_cloud_options.staging_location is None:
      raise RuntimeError('The --staging_location option must be specified.')
    if google_cloud_options.temp_location is None:
      raise RuntimeError('The --temp_location option must be specified.')

    resource_stager = _LegacyDataflowStager(self)
    _, resources = resource_stager.stage_job_resources(
        options,
        temp_dir=tempfile.mkdtemp(),
        staging_location=google_cloud_options.staging_location)
    return resources

  def stage_file(self, gcs_or_local_path, file_name, stream,
                 mime_type='application/octet-stream'):
    """Stages a file at a GCS or local path with stream-supplied contents."""
    if not gcs_or_local_path.startswith('gs://'):
      local_path = FileSystems.join(gcs_or_local_path, file_name)
      logging.info('Staging file locally to %s', local_path)
      with open(local_path, 'wb') as f:
        f.write(stream.read())
      return
    gcs_location = FileSystems.join(gcs_or_local_path, file_name)
    bucket, name = gcs_location[5:].split('/', 1)

    request = storage.StorageObjectsInsertRequest(
        bucket=bucket, name=name)
    start_time = time.time()
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
                       'access to the specified path.') %
                      (gcs_or_local_path, reportable_errors[e.status_code]))
      raise
    logging.info('Completed GCS upload to %s in %s seconds.', gcs_location,
                 int(time.time() - start_time))
    return response

  @retry.no_retries  # Using no_retries marks this as an integration point.
  def create_job(self, job):
    """Creates job description. May stage and/or submit for remote execution."""
    self.create_job_description(job)

    # Stage and submit the job when necessary
    dataflow_job_file = job.options.view_as(DebugOptions).dataflow_job_file
    template_location = (
        job.options.view_as(GoogleCloudOptions).template_location)

    job_location = template_location or dataflow_job_file
    if job_location:
      gcs_or_local_path = os.path.dirname(job_location)
      file_name = os.path.basename(job_location)
      self.stage_file(gcs_or_local_path, file_name,
                      io.BytesIO(job.json().encode('utf-8')))

    if not template_location:
      return self.submit_job_description(job)

    logging.info('A template was just created at location %s',
                 template_location)
    return None

  def create_job_description(self, job):
    """Creates a job described by the workflow proto."""

    # Stage the pipeline for the runner harness
    self.stage_file(job.google_cloud_options.staging_location,
                    shared_names.STAGED_PIPELINE_FILENAME,
                    io.BytesIO(job.proto_pipeline.SerializeToString()))

    # Stage other resources for the SDK harness
    resources = self._stage_resources(job.options)

    job.proto.environment = Environment(
        pipeline_url=FileSystems.join(job.google_cloud_options.staging_location,
                                      shared_names.STAGED_PIPELINE_FILENAME),
        packages=resources, options=job.options,
        environment_version=self.environment_version).proto
    logging.debug('JOB: %s', job)

  @retry.with_exponential_backoff(num_retries=3, initial_delay_secs=3)
  def get_job_metrics(self, job_id):
    request = dataflow.DataflowProjectsLocationsJobsGetMetricsRequest()
    request.jobId = job_id
    request.location = self.google_cloud_options.region
    request.projectId = self.google_cloud_options.project
    try:
      response = self._client.projects_locations_jobs.GetMetrics(request)
    except exceptions.BadStatusCodeError as e:
      logging.error('HTTP status %d. Unable to query metrics',
                    e.response.status)
      raise
    return response

  @retry.with_exponential_backoff(num_retries=3)
  def submit_job_description(self, job):
    """Creates and excutes a job request."""
    request = dataflow.DataflowProjectsLocationsJobsCreateRequest()
    request.projectId = self.google_cloud_options.project
    request.location = self.google_cloud_options.region
    request.job = job.proto

    try:
      response = self._client.projects_locations_jobs.Create(request)
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
        'https://console.cloud.google.com/dataflow/jobsDetail'
        '/locations/%s/jobs/%s?project=%s',
        self.google_cloud_options.region,
        response.id,
        self.google_cloud_options.project)

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

    request = dataflow.DataflowProjectsLocationsJobsUpdateRequest()
    request.jobId = job_id
    request.projectId = self.google_cloud_options.project
    request.location = self.google_cloud_options.region
    request.job = dataflow.Job(requestedState=new_state)

    self._client.projects_locations_jobs.Update(request)
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
    request = dataflow.DataflowProjectsLocationsJobsGetRequest()
    request.jobId = job_id
    request.projectId = self.google_cloud_options.project
    request.location = self.google_cloud_options.region
    response = self._client.projects_locations_jobs.Get(request)
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
    request = dataflow.DataflowProjectsLocationsJobsMessagesListRequest(
        jobId=job_id, location=self.google_cloud_options.region,
        projectId=self.google_cloud_options.project)
    if page_token is not None:
      request.pageToken = page_token
    if start_time is not None:
      request.startTime = start_time
    if end_time is not None:
      request.endTime = end_time
    if minimum_importance is not None:
      if minimum_importance == 'JOB_MESSAGE_DEBUG':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_DEBUG)
      elif minimum_importance == 'JOB_MESSAGE_DETAILED':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_DETAILED)
      elif minimum_importance == 'JOB_MESSAGE_BASIC':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_BASIC)
      elif minimum_importance == 'JOB_MESSAGE_WARNING':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_WARNING)
      elif minimum_importance == 'JOB_MESSAGE_ERROR':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest
            .MinimumImportanceValueValuesEnum
            .JOB_MESSAGE_ERROR)
      else:
        raise RuntimeError(
            'Unexpected value for minimum_importance argument: %r'
            % minimum_importance)
    response = self._client.projects_locations_jobs_messages.List(request)
    return response.jobMessages, response.nextPageToken

  def job_id_for_name(self, job_name):
    token = None
    while True:
      request = dataflow.DataflowProjectsLocationsJobsListRequest(
          projectId=self.google_cloud_options.project,
          location=self.google_cloud_options.region,
          pageToken=token)
      response = self._client.projects_locations_jobs.List(request)
      for job in response.jobs:
        if (job.name == job_name
            and job.currentState
            == dataflow.Job.CurrentStateValueValuesEnum.JOB_STATE_RUNNING):
          return job.id
      token = response.nextPageToken
      if token is None:
        raise ValueError("No running job found with name '%s'" % job_name)


class MetricUpdateTranslators(object):
  """Translators between accumulators and dataflow metric updates."""

  @staticmethod
  def translate_boolean(accumulator, metric_update_proto):
    metric_update_proto.boolean = accumulator.value

  @staticmethod
  def translate_scalar_mean_int(accumulator, metric_update_proto):
    if accumulator.count:
      metric_update_proto.integerMean = dataflow.IntegerMean()
      metric_update_proto.integerMean.sum = to_split_int(accumulator.sum)
      metric_update_proto.integerMean.count = to_split_int(accumulator.count)
    else:
      metric_update_proto.nameAndKind.kind = None

  @staticmethod
  def translate_scalar_mean_float(accumulator, metric_update_proto):
    if accumulator.count:
      metric_update_proto.floatingPointMean = dataflow.FloatingPointMean()
      metric_update_proto.floatingPointMean.sum = accumulator.sum
      metric_update_proto.floatingPointMean.count = to_split_int(
          accumulator.count)
    else:
      metric_update_proto.nameAndKind.kind = None

  @staticmethod
  def translate_scalar_counter_int(accumulator, metric_update_proto):
    metric_update_proto.integer = to_split_int(accumulator.value)

  @staticmethod
  def translate_scalar_counter_float(accumulator, metric_update_proto):
    metric_update_proto.floatingPoint = accumulator.value


class _LegacyDataflowStager(Stager):
  def __init__(self, dataflow_application_client):
    super(_LegacyDataflowStager, self).__init__()
    self._dataflow_application_client = dataflow_application_client

  def stage_artifact(self, local_path_to_artifact, artifact_name):
    self._dataflow_application_client._gcs_file_copy(local_path_to_artifact,
                                                     artifact_name)

  def commit_manifest(self):
    pass

  @staticmethod
  def get_sdk_package_name():
    """For internal use only; no backwards-compatibility guarantees.

          Returns the PyPI package name to be staged to Google Cloud Dataflow.
    """
    return shared_names.BEAM_PACKAGE_NAME


def to_split_int(n):
  res = dataflow.SplitInt64()
  res.lowBits = n & 0xffffffff
  res.highBits = n >> 32
  return res


def translate_distribution(distribution_update, metric_update_proto):
  """Translate metrics DistributionUpdate to dataflow distribution update.

  Args:
    distribution_update: Instance of DistributionData or
    DataflowDistributionCounter.
    metric_update_proto: Used for report metrics.
  """
  dist_update_proto = dataflow.DistributionUpdate()
  dist_update_proto.min = to_split_int(distribution_update.min)
  dist_update_proto.max = to_split_int(distribution_update.max)
  dist_update_proto.count = to_split_int(distribution_update.count)
  dist_update_proto.sum = to_split_int(distribution_update.sum)
  # DatadflowDistributionCounter needs to translate histogram
  if isinstance(distribution_update, DataflowDistributionCounter):
    dist_update_proto.histogram = dataflow.Histogram()
    distribution_update.translate_to_histogram(dist_update_proto.histogram)
  metric_update_proto.distribution = dist_update_proto


def translate_value(value, metric_update_proto):
  metric_update_proto.integer = to_split_int(value)


def translate_mean(accumulator, metric_update):
  if accumulator.count:
    metric_update.meanSum = to_json_value(accumulator.sum, with_type=True)
    metric_update.meanCount = to_json_value(accumulator.count, with_type=True)
  else:
    # A denominator of 0 will raise an error in the service.
    # What it means is we have nothing to report yet, so don't.
    metric_update.kind = None


def _use_fnapi(pipeline_options):
  standard_options = pipeline_options.view_as(StandardOptions)
  debug_options = pipeline_options.view_as(DebugOptions)

  return standard_options.streaming or (
      debug_options.experiments and 'beam_fn_api' in debug_options.experiments)


def _use_unified_worker(pipeline_options):
  debug_options = pipeline_options.view_as(DebugOptions)

  return _use_fnapi(pipeline_options) and (
      debug_options.experiments and
      'use_unified_worker' in debug_options.experiments)


def get_default_container_image_for_current_sdk(job_type):
  """For internal use only; no backwards-compatibility guarantees.

    Args:
      job_type (str): BEAM job type.

    Returns:
      str: Google Cloud Dataflow container image for remote execution.
    """
  if sys.version_info[0] == 2:
    version_suffix = ''
  elif sys.version_info[0:2] == (3, 5):
    version_suffix = '3'
  elif sys.version_info[0:2] == (3, 6):
    version_suffix = '36'
  elif sys.version_info[0:2] == (3, 7):
    version_suffix = '37'
  else:
    raise Exception('Dataflow only supports Python versions 2 and 3.5+, got: %s'
                    % str(sys.version_info[0:2]))

  # TODO(tvalentyn): Use enumerated type instead of strings for job types.
  if job_type == 'FNAPI_BATCH' or job_type == 'FNAPI_STREAMING':
    fnapi_suffix = '-fnapi'
  else:
    fnapi_suffix = ''

  image_name = '{repository}/python{version_suffix}{fnapi_suffix}'.format(
      repository=names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY,
      version_suffix=version_suffix,
      fnapi_suffix=fnapi_suffix)

  image_tag = _get_required_container_version(job_type)
  return image_name + ':' + image_tag


def _get_required_container_version(job_type=None):
  """For internal use only; no backwards-compatibility guarantees.

    Args:
      job_type (str, optional): BEAM job type. Defaults to None.

    Returns:
      str: The tag of worker container images in GCR that corresponds to
        current version of the SDK.
    """
  if 'dev' in beam_version.__version__:
    if job_type == 'FNAPI_BATCH' or job_type == 'FNAPI_STREAMING':
      return names.BEAM_FNAPI_CONTAINER_VERSION
    else:
      return names.BEAM_CONTAINER_VERSION
  else:
    return beam_version.__version__


def get_runner_harness_container_image():
  """For internal use only; no backwards-compatibility guarantees.

     Returns:
       str: Runner harness container image that shall be used by default
         for current SDK version or None if the runner harness container image
         bundled with the service shall be used.
    """
  # Pin runner harness for released versions of the SDK.
  if 'dev' not in beam_version.__version__:
    return (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/' + 'harness' + ':' +
            beam_version.__version__)
  # Don't pin runner harness for dev versions so that we can notice
  # potential incompatibility between runner and sdk harnesses.
  return None


def get_response_encoding():
  """Encoding to use to decode HTTP response from Google APIs."""
  return None if sys.version_info[0] < 3 else 'utf8'


def _verify_interpreter_version_is_supported(pipeline_options):
  if sys.version_info[0:2] in [(2, 7), (3, 5), (3, 6), (3, 7)]:
    return

  debug_options = pipeline_options.view_as(DebugOptions)
  if (debug_options.experiments and 'ignore_py3_minor_version' in
      debug_options.experiments):
    return

  raise Exception(
      'Dataflow runner currently supports Python versions '
      '2.7, 3.5, 3.6, and 3.7. To ignore this requirement and start a job '
      'using a different version of Python 3 interpreter, pass '
      '--experiment ignore_py3_minor_version pipeline option.')


# To enable a counter on the service, add it to this dictionary.
structured_counter_translations = {
    cy_combiners.CountCombineFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.SUM,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.SumInt64Fn: (
        dataflow.CounterMetadata.KindValueValuesEnum.SUM,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MinInt64Fn: (
        dataflow.CounterMetadata.KindValueValuesEnum.MIN,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MaxInt64Fn: (
        dataflow.CounterMetadata.KindValueValuesEnum.MAX,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MeanInt64Fn: (
        dataflow.CounterMetadata.KindValueValuesEnum.MEAN,
        MetricUpdateTranslators.translate_scalar_mean_int),
    cy_combiners.SumFloatFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.SUM,
        MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MinFloatFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.MIN,
        MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MaxFloatFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.MAX,
        MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MeanFloatFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.MEAN,
        MetricUpdateTranslators.translate_scalar_mean_float),
    cy_combiners.AllCombineFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.AND,
        MetricUpdateTranslators.translate_boolean),
    cy_combiners.AnyCombineFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.OR,
        MetricUpdateTranslators.translate_boolean),
    cy_combiners.DataflowDistributionCounterFn: (
        dataflow.CounterMetadata.KindValueValuesEnum.DISTRIBUTION,
        translate_distribution)
}


counter_translations = {
    cy_combiners.CountCombineFn: (
        dataflow.NameAndKind.KindValueValuesEnum.SUM,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.SumInt64Fn: (
        dataflow.NameAndKind.KindValueValuesEnum.SUM,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MinInt64Fn: (
        dataflow.NameAndKind.KindValueValuesEnum.MIN,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MaxInt64Fn: (
        dataflow.NameAndKind.KindValueValuesEnum.MAX,
        MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MeanInt64Fn: (
        dataflow.NameAndKind.KindValueValuesEnum.MEAN,
        MetricUpdateTranslators.translate_scalar_mean_int),
    cy_combiners.SumFloatFn: (
        dataflow.NameAndKind.KindValueValuesEnum.SUM,
        MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MinFloatFn: (
        dataflow.NameAndKind.KindValueValuesEnum.MIN,
        MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MaxFloatFn: (
        dataflow.NameAndKind.KindValueValuesEnum.MAX,
        MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MeanFloatFn: (
        dataflow.NameAndKind.KindValueValuesEnum.MEAN,
        MetricUpdateTranslators.translate_scalar_mean_float),
    cy_combiners.AllCombineFn: (
        dataflow.NameAndKind.KindValueValuesEnum.AND,
        MetricUpdateTranslators.translate_boolean),
    cy_combiners.AnyCombineFn: (
        dataflow.NameAndKind.KindValueValuesEnum.OR,
        MetricUpdateTranslators.translate_boolean),
    cy_combiners.DataflowDistributionCounterFn: (
        dataflow.NameAndKind.KindValueValuesEnum.DISTRIBUTION,
        translate_distribution)
}
