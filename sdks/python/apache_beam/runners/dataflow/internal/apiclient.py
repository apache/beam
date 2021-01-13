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

# pytype: skip-file

from __future__ import absolute_import

import codecs
import getpass
import io
import json
import logging
import os
import pkg_resources
import re
import sys
import time
import warnings
from copy import copy
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
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.dataflow.internal.clients import dataflow
from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.runners.internal import names as shared_names
from apache_beam.runners.portability.stager import Stager
from apache_beam.transforms import DataflowDistributionCounter
from apache_beam.transforms import cy_combiners
from apache_beam.transforms.display import DisplayData
from apache_beam.utils import retry
from apache_beam.utils import proto_utils

# Environment version information. It is passed to the service during a
# a job submission and is used by the service to establish what features
# are expected by the workers.
_LEGACY_ENVIRONMENT_MAJOR_VERSION = '8'
_FNAPI_ENVIRONMENT_MAJOR_VERSION = '8'

_LOGGER = logging.getLogger(__name__)

_PYTHON_VERSIONS_SUPPORTED_BY_DATAFLOW = ['3.6', '3.7', '3.8']


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
    if tag is None or len(outputs) == 1:
      return outputs[0]
    else:
      if tag not in outputs:
        raise ValueError('Cannot find named output: %s in %s.' % (tag, outputs))
      return tag


class Environment(object):
  """Wrapper for a dataflow Environment protobuf."""
  def __init__(
      self,
      packages,
      options,
      environment_version,
      proto_pipeline_staged_url,
      proto_pipeline=None,
      _sdk_image_overrides=None):
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    self.worker_options = options.view_as(WorkerOptions)
    self.debug_options = options.view_as(DebugOptions)
    self.pipeline_url = proto_pipeline_staged_url
    self.proto = dataflow.Environment()
    self.proto.clusterManagerApiService = GoogleCloudOptions.COMPUTE_API_SERVICE
    self.proto.dataset = '{}/cloud_dataflow'.format(
        GoogleCloudOptions.BIGQUERY_API_SERVICE)
    self.proto.tempStoragePrefix = (
        self.google_cloud_options.temp_location.replace(
            'gs:/', GoogleCloudOptions.STORAGE_API_SERVICE))
    if self.worker_options.worker_region:
      self.proto.workerRegion = self.worker_options.worker_region
    if self.worker_options.worker_zone:
      self.proto.workerZone = self.worker_options.worker_zone
    # User agent information.
    self.proto.userAgent = dataflow.Environment.UserAgentValue()
    self.local = 'localhost' in self.google_cloud_options.dataflow_endpoint
    self._proto_pipeline = proto_pipeline
    self._sdk_image_overrides = _sdk_image_overrides or dict()

    if self.google_cloud_options.service_account_email:
      self.proto.serviceAccountEmail = (
          self.google_cloud_options.service_account_email)
    if self.google_cloud_options.dataflow_kms_key:
      self.proto.serviceKmsKeyName = self.google_cloud_options.dataflow_kms_key

    self.proto.userAgent.additionalProperties.extend([
        dataflow.Environment.UserAgentValue.AdditionalProperty(
            key='name', value=to_json_value(self._get_python_sdk_name())),
        dataflow.Environment.UserAgentValue.AdditionalProperty(
            key='version', value=to_json_value(beam_version.__version__))
    ])
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
            key='job_type', value=to_json_value(job_type)),
        dataflow.Environment.VersionValue.AdditionalProperty(
            key='major', value=to_json_value(environment_version))
    ])
    # TODO: Use enumerated type instead of strings for job types.
    if job_type.startswith('FNAPI_'):
      self.debug_options.experiments = self.debug_options.experiments or []

      if self.debug_options.lookup_experiment(
          'runner_harness_container_image') or _use_unified_worker(options):
        # Default image is not used if user provides a runner harness image.
        # Default runner harness image is selected by the service for unified
        # worker.
        pass
      else:
        runner_harness_override = (get_runner_harness_container_image())
        if runner_harness_override:
          self.debug_options.add_experiment(
              'runner_harness_container_image=' + runner_harness_override)
      debug_options_experiments = self.debug_options.experiments
      # Add use_multiple_sdk_containers flag if it's not already present. Do not
      # add the flag if 'no_use_multiple_sdk_containers' is present.
      # TODO: Cleanup use_multiple_sdk_containers once we deprecate Python SDK
      # till version 2.4.
      if ('use_multiple_sdk_containers' not in debug_options_experiments and
          'no_use_multiple_sdk_containers' not in debug_options_experiments):
        debug_options_experiments.append('use_multiple_sdk_containers')
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
    pool.workerHarnessContainerImage = (
        get_container_image_from_options(options))

    # Setting worker pool sdk_harness_container_images option for supported
    # Dataflow workers.
    environments_to_use = self._get_environments_from_tranforms()
    if _use_unified_worker(options):
      # Adding a SDK container image for the pipeline SDKs
      container_image = dataflow.SdkHarnessContainerImage()
      pipeline_sdk_container_image = get_container_image_from_options(options)
      container_image.containerImage = pipeline_sdk_container_image
      container_image.useSingleCorePerContainer = True  # True for Python SDK.
      pool.sdkHarnessContainerImages.append(container_image)

      already_added_containers = [pipeline_sdk_container_image]

      # Adding container images for other SDKs that may be needed for
      # cross-language pipelines.
      for environment in environments_to_use:
        if environment.urn != common_urns.environments.DOCKER.urn:
          raise Exception(
              'Dataflow can only execute pipeline steps in Docker environments.'
              ' Received %r.' % environment)
        environment_payload = proto_utils.parse_Bytes(
            environment.payload, beam_runner_api_pb2.DockerPayload)
        container_image_url = environment_payload.container_image
        if container_image_url in already_added_containers:
          # Do not add the pipeline environment again.

          # Currently, Dataflow uses Docker container images to uniquely
          # identify execution environments. Hence Dataflow executes all
          # transforms that specify the the same Docker container image in a
          # single container instance. Dependencies of all environments that
          # specify a given container image will be staged in the container
          # instance for that particular container image.
          # TODO(BEAM-9455): loosen this restriction to support multiple
          # environments with the same container image when Dataflow supports
          # environment specific artifact provisioning.
          continue
        already_added_containers.append(container_image_url)

        container_image = dataflow.SdkHarnessContainerImage()
        container_image.containerImage = container_image_url
        # Currently we only set following to True for Python SDK.
        # TODO: set this correctly for remote environments that might be Python.
        container_image.useSingleCorePerContainer = False
        pool.sdkHarnessContainerImages.append(container_image)

    if self.debug_options.number_of_worker_harness_threads:
      pool.numThreadsPerWorker = (
          self.debug_options.number_of_worker_harness_threads)
    if self.worker_options.use_public_ips is not None:
      if self.worker_options.use_public_ips:
        pool.ipConfiguration = (
            dataflow.WorkerPool.IpConfigurationValueValuesEnum.WORKER_IP_PUBLIC)
      else:
        pool.ipConfiguration = (
            dataflow.WorkerPool.IpConfigurationValueValuesEnum.WORKER_IP_PRIVATE
        )

    if self.standard_options.streaming:
      # Use separate data disk for streaming.
      disk = dataflow.Disk()
      if self.local:
        disk.diskType = 'local'
      if self.worker_options.disk_type:
        disk.diskType = self.worker_options.disk_type
      pool.dataDisks.append(disk)
    self.proto.workerPools.append(pool)

    sdk_pipeline_options = options.get_all_options()
    if sdk_pipeline_options:
      self.proto.sdkPipelineOptions = (
          dataflow.Environment.SdkPipelineOptionsValue())

      options_dict = {
          k: v
          for k, v in sdk_pipeline_options.items() if v is not None
      }
      options_dict["pipelineUrl"] = proto_pipeline_staged_url
      self.proto.sdkPipelineOptions.additionalProperties.append(
          dataflow.Environment.SdkPipelineOptionsValue.AdditionalProperty(
              key='options', value=to_json_value(options_dict)))

      dd = DisplayData.create_from_options(options)
      items = [item.get_dict() for item in dd.items]
      self.proto.sdkPipelineOptions.additionalProperties.append(
          dataflow.Environment.SdkPipelineOptionsValue.AdditionalProperty(
              key='display_data', value=to_json_value(items)))

  def _get_environments_from_tranforms(self):
    if not self._proto_pipeline:
      return []

    environment_ids = set(
        transform.environment_id
        for transform in self._proto_pipeline.components.transforms.values()
        if transform.environment_id)

    return [
        self._proto_pipeline.components.environments[id]
        for id in environment_ids
    ]

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
        return codecs.CodecInfo(
            name='shortstrings',
            encode=encode_shortstrings,
            decode=decode_shortstrings)
      return None

    codecs.register(shortstrings_registerer)

    # Use json "dump string" method to get readable formatting;
    # further modify it to not output too-long strings, aimed at the
    # 10,000+ character hex-encoded "serialized_fn" values.
    return json.dumps(
        json.loads(encoding.MessageToJson(self.proto), encoding='shortstrings'),
        indent=2,
        sort_keys=True)

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
      job_name = '{}-{}'.format(
          app_user_name[:-(len(job_name) - 63)], date_component)
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
        if not getattr(self.google_cloud_options, option)
    ]
    if missing:
      raise ValueError(
          'Missing required configuration parameters: %s' % missing)

    if not self.google_cloud_options.staging_location:
      _LOGGER.info(
          'Defaulting to the temp_location as staging_location: %s',
          self.google_cloud_options.temp_location)
      (
          self.google_cloud_options.staging_location
      ) = self.google_cloud_options.temp_location

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
      if self.google_cloud_options.transform_name_mapping:
        self.proto.transformNameMapping = (
            dataflow.Job.TransformNameMappingValue())
        for _, (key, value) in enumerate(
            self.google_cloud_options.transform_name_mapping.items()):
          self.proto.transformNameMapping.additionalProperties.append(
              dataflow.Job.TransformNameMappingValue.AdditionalProperty(
                  key=key, value=value))
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
    return (Job, (self.options, ))


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
    self._sdk_image_overrides = self._get_sdk_image_overrides(options)

  def _get_sdk_image_overrides(self, pipeline_options):
    worker_options = pipeline_options.view_as(WorkerOptions)
    sdk_overrides = worker_options.sdk_harness_container_image_overrides
    if sdk_overrides:
      return dict(override_str.split(',', 1) for override_str in sdk_overrides)

  # TODO(silviuc): Refactor so that retry logic can be applied.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def _gcs_file_copy(self, from_path, to_path):
    to_folder, to_name = os.path.split(to_path)
    total_size = os.path.getsize(from_path)
    with open(from_path, 'rb') as f:
      self.stage_file(to_folder, to_name, f, total_size=total_size)

  def _stage_resources(self, pipeline, options):
    google_cloud_options = options.view_as(GoogleCloudOptions)
    if google_cloud_options.staging_location is None:
      raise RuntimeError('The --staging_location option must be specified.')
    if google_cloud_options.temp_location is None:
      raise RuntimeError('The --temp_location option must be specified.')

    resources = []
    hashs = {}
    for _, env in sorted(pipeline.components.environments.items(),
                         key=lambda kv: kv[0]):
      for dep in env.dependencies:
        if dep.type_urn != common_urns.artifact_types.FILE.urn:
          raise RuntimeError('unsupported artifact type %s' % dep.type_urn)
        if dep.role_urn != common_urns.artifact_roles.STAGING_TO.urn:
          raise RuntimeError('unsupported role type %s' % dep.role_urn)
        type_payload = beam_runner_api_pb2.ArtifactFilePayload.FromString(
            dep.type_payload)
        role_payload = (
            beam_runner_api_pb2.ArtifactStagingToRolePayload.FromString(
                dep.role_payload))
        if type_payload.sha256 and type_payload.sha256 in hashs:
          _LOGGER.info(
              'Found duplicated artifact: %s (%s)',
              type_payload.path,
              type_payload.sha256)
          dep.role_payload = beam_runner_api_pb2.ArtifactStagingToRolePayload(
              staged_name=hashs[type_payload.sha256]).SerializeToString()
        else:
          resources.append((type_payload.path, role_payload.staged_name))
          hashs[type_payload.sha256] = role_payload.staged_name

    resource_stager = _LegacyDataflowStager(self)
    staged_resources = resource_stager.stage_job_resources(
        resources, staging_location=google_cloud_options.staging_location)
    return staged_resources

  def stage_file(
      self,
      gcs_or_local_path,
      file_name,
      stream,
      mime_type='application/octet-stream',
      total_size=None):
    """Stages a file at a GCS or local path with stream-supplied contents."""
    if not gcs_or_local_path.startswith('gs://'):
      local_path = FileSystems.join(gcs_or_local_path, file_name)
      _LOGGER.info('Staging file locally to %s', local_path)
      with open(local_path, 'wb') as f:
        f.write(stream.read())
      return
    gcs_location = FileSystems.join(gcs_or_local_path, file_name)
    bucket, name = gcs_location[5:].split('/', 1)

    request = storage.StorageObjectsInsertRequest(bucket=bucket, name=name)
    start_time = time.time()
    _LOGGER.info('Starting GCS upload to %s...', gcs_location)
    upload = storage.Upload(stream, mime_type, total_size)
    try:
      response = self._storage_client.objects.Insert(request, upload=upload)
    except exceptions.HttpError as e:
      reportable_errors = {
          403: 'access denied',
          404: 'bucket not found',
      }
      if e.status_code in reportable_errors:
        raise IOError((
            'Could not upload to GCS path %s: %s. Please verify '
            'that credentials are valid and that you have write '
            'access to the specified path.') %
                      (gcs_or_local_path, reportable_errors[e.status_code]))
      raise
    _LOGGER.info(
        'Completed GCS upload to %s in %s seconds.',
        gcs_location,
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
      self.stage_file(
          gcs_or_local_path, file_name, io.BytesIO(job.json().encode('utf-8')))

    if job.options.view_as(DebugOptions).lookup_experiment('upload_graph'):
      self.stage_file(
          job.options.view_as(GoogleCloudOptions).staging_location,
          "dataflow_graph.json",
          io.BytesIO(job.json().encode('utf-8')))
      del job.proto.steps[:]
      job.proto.stepsLocation = FileSystems.join(
          job.options.view_as(GoogleCloudOptions).staging_location,
          "dataflow_graph.json")

    if not template_location:
      return self.submit_job_description(job)

    _LOGGER.info(
        'A template was just created at location %s', template_location)
    return None

  @staticmethod
  def _apply_sdk_environment_overrides(proto_pipeline, sdk_overrides):
    # Update environments based on user provided overrides
    if sdk_overrides:
      for environment in proto_pipeline.components.environments.values():
        docker_payload = proto_utils.parse_Bytes(
            environment.payload, beam_runner_api_pb2.DockerPayload)
        for pattern, override in sdk_overrides.items():
          new_payload = copy(docker_payload)
          new_payload.container_image = re.sub(
              pattern, override, docker_payload.container_image)
          environment.payload = new_payload.SerializeToString()

  def create_job_description(self, job):
    """Creates a job described by the workflow proto."""
    DataflowApplicationClient._apply_sdk_environment_overrides(
        job.proto_pipeline, self._sdk_image_overrides)

    # Stage proto pipeline.
    self.stage_file(
        job.google_cloud_options.staging_location,
        shared_names.STAGED_PIPELINE_FILENAME,
        io.BytesIO(job.proto_pipeline.SerializeToString()))

    # Stage other resources for the SDK harness
    resources = self._stage_resources(job.proto_pipeline, job.options)

    job.proto.environment = Environment(
        proto_pipeline_staged_url=FileSystems.join(
            job.google_cloud_options.staging_location,
            shared_names.STAGED_PIPELINE_FILENAME),
        packages=resources,
        options=job.options,
        environment_version=self.environment_version,
        proto_pipeline=job.proto_pipeline,
        _sdk_image_overrides=self._sdk_image_overrides).proto
    _LOGGER.debug('JOB: %s', job)

  @retry.with_exponential_backoff(num_retries=3, initial_delay_secs=3)
  def get_job_metrics(self, job_id):
    request = dataflow.DataflowProjectsLocationsJobsGetMetricsRequest()
    request.jobId = job_id
    request.location = self.google_cloud_options.region
    request.projectId = self.google_cloud_options.project
    try:
      response = self._client.projects_locations_jobs.GetMetrics(request)
    except exceptions.BadStatusCodeError as e:
      _LOGGER.error(
          'HTTP status %d. Unable to query metrics', e.response.status)
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
      _LOGGER.error(
          'HTTP status %d trying to create job'
          ' at dataflow service endpoint %s',
          e.response.status,
          self.google_cloud_options.dataflow_endpoint)
      _LOGGER.fatal('details of server error: %s', e)
      raise
    _LOGGER.info('Create job: %s', response)
    # The response is a Job proto with the id for the new job.
    _LOGGER.info('Created job with id: [%s]', response.id)
    _LOGGER.info('Submitted job: %s', response.id)
    _LOGGER.info(
        'To access the Dataflow monitoring console, please navigate to '
        'https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s',
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

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_notfound_filter)
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

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_notfound_filter)
  def list_messages(
      self,
      job_id,
      start_time=None,
      end_time=None,
      page_token=None,
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
        jobId=job_id,
        location=self.google_cloud_options.region,
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
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest.
            MinimumImportanceValueValuesEnum.JOB_MESSAGE_DEBUG)
      elif minimum_importance == 'JOB_MESSAGE_DETAILED':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest.
            MinimumImportanceValueValuesEnum.JOB_MESSAGE_DETAILED)
      elif minimum_importance == 'JOB_MESSAGE_BASIC':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest.
            MinimumImportanceValueValuesEnum.JOB_MESSAGE_BASIC)
      elif minimum_importance == 'JOB_MESSAGE_WARNING':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest.
            MinimumImportanceValueValuesEnum.JOB_MESSAGE_WARNING)
      elif minimum_importance == 'JOB_MESSAGE_ERROR':
        request.minimumImportance = (
            dataflow.DataflowProjectsLocationsJobsMessagesListRequest.
            MinimumImportanceValueValuesEnum.JOB_MESSAGE_ERROR)
      else:
        raise RuntimeError(
            'Unexpected value for minimum_importance argument: %r' %
            minimum_importance)
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
        if (job.name == job_name and job.currentState in [
            dataflow.Job.CurrentStateValueValuesEnum.JOB_STATE_RUNNING,
            dataflow.Job.CurrentStateValueValuesEnum.JOB_STATE_DRAINING
        ]):
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
    self._dataflow_application_client._gcs_file_copy(
        local_path_to_artifact, artifact_name)

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
    distribution_update: Instance of DistributionData,
    DistributionInt64Accumulator or DataflowDistributionCounter.
    metric_update_proto: Used for report metrics.
  """
  dist_update_proto = dataflow.DistributionUpdate()
  dist_update_proto.min = to_split_int(distribution_update.min)
  dist_update_proto.max = to_split_int(distribution_update.max)
  dist_update_proto.count = to_split_int(distribution_update.count)
  dist_update_proto.sum = to_split_int(distribution_update.sum)
  # DataflowDistributionCounter needs to translate histogram
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
  use_unified_worker_flag = 'use_unified_worker'
  use_runner_v2_flag = 'use_runner_v2'

  if (debug_options.lookup_experiment(use_runner_v2_flag) and
      not debug_options.lookup_experiment(use_unified_worker_flag)):
    debug_options.add_experiment(use_unified_worker_flag)

  return debug_options.lookup_experiment(use_unified_worker_flag)


def _get_container_image_tag():
  base_version = pkg_resources.parse_version(
      beam_version.__version__).base_version
  if base_version != beam_version.__version__:
    warnings.warn(
        "A non-standard version of Beam SDK detected: %s. "
        "Dataflow runner will use container image tag %s. "
        "This use case is not supported." %
        (beam_version.__version__, base_version))
  return base_version


def get_container_image_from_options(pipeline_options):
  """For internal use only; no backwards-compatibility guarantees.

    Args:
      pipeline_options (PipelineOptions): A container for pipeline options.

    Returns:
      str: Container image for remote execution.
  """
  worker_options = pipeline_options.view_as(WorkerOptions)
  if worker_options.worker_harness_container_image:
    return worker_options.worker_harness_container_image

  use_fnapi = _use_fnapi(pipeline_options)
  # TODO(tvalentyn): Use enumerated type instead of strings for job types.
  if use_fnapi:
    fnapi_suffix = '-fnapi'
  else:
    fnapi_suffix = ''

  version_suffix = '%s%s' % (sys.version_info[0:2])
  image_name = '{repository}/python{version_suffix}{fnapi_suffix}'.format(
      repository=names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY,
      version_suffix=version_suffix,
      fnapi_suffix=fnapi_suffix)

  image_tag = _get_required_container_version(use_fnapi)
  return image_name + ':' + image_tag


def _get_required_container_version(use_fnapi):
  """For internal use only; no backwards-compatibility guarantees.

    Args:
      use_fnapi (bool): True, if pipeline is using FnAPI, False otherwise.

    Returns:
      str: The tag of worker container images in GCR that corresponds to
        current version of the SDK.
    """
  if 'dev' in beam_version.__version__:
    if use_fnapi:
      return names.BEAM_FNAPI_CONTAINER_VERSION
    else:
      return names.BEAM_CONTAINER_VERSION
  else:
    return _get_container_image_tag()


def get_runner_harness_container_image():
  """For internal use only; no backwards-compatibility guarantees.

     Returns:
       str: Runner harness container image that shall be used by default
         for current SDK version or None if the runner harness container image
         bundled with the service shall be used.
    """
  # Pin runner harness for released versions of the SDK.
  if 'dev' not in beam_version.__version__:
    return (
        names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/' + 'harness' + ':' +
        _get_container_image_tag())
  # Don't pin runner harness for dev versions so that we can notice
  # potential incompatibility between runner and sdk harnesses.
  return None


def get_response_encoding():
  """Encoding to use to decode HTTP response from Google APIs."""
  return None if sys.version_info[0] < 3 else 'utf8'


def _verify_interpreter_version_is_supported(pipeline_options):
  if ('%s.%s' %
      (sys.version_info[0],
       sys.version_info[1]) in _PYTHON_VERSIONS_SUPPORTED_BY_DATAFLOW):
    return

  if 'dev' in beam_version.__version__:
    return

  debug_options = pipeline_options.view_as(DebugOptions)
  if (debug_options.experiments and
      'use_unsupported_python_version' in debug_options.experiments):
    return

  raise Exception(
      'Dataflow runner currently supports Python versions %s, got %s.\n'
      'To ignore this requirement and start a job '
      'using an unsupported version of Python interpreter, pass '
      '--experiment use_unsupported_python_version pipeline option.' %
      (_PYTHON_VERSIONS_SUPPORTED_BY_DATAFLOW, sys.version))


# To enable a counter on the service, add it to this dictionary.
# This is required for the legacy python dataflow runner, as portability
# does not communicate to the service via python code, but instead via a
# a runner harness (in C++ or Java).
# TODO(BEAM-7050) : Remove this antipattern, legacy dataflow python
# pipelines will break whenever a new cy_combiner type is used.
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
        translate_distribution),
    cy_combiners.DistributionInt64Fn: (
        dataflow.CounterMetadata.KindValueValuesEnum.DISTRIBUTION,
        translate_distribution),
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
        translate_distribution),
    cy_combiners.DistributionInt64Fn: (
        dataflow.CounterMetadata.KindValueValuesEnum.DISTRIBUTION,
        translate_distribution),
}
