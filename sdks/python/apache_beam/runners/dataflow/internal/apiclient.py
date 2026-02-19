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
# To regenerate the client:
# pip install google-apitools[cli]
# gen_client --discovery_url=cloudbuild.v1 --overwrite \
#  --outdir=apache_beam/runners/dataflow/internal/clients/cloudbuild \
#  --root_package=. client

import ast
import codecs
from functools import partial
import getpass
import hashlib
import io
import json
import logging
import os
import random
import string

from packaging import version
import re
import sys
import time
import traceback
import warnings
from copy import copy
from datetime import datetime
from datetime import timezone

from google.api_core import exceptions
from google.cloud import dataflow
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from google.protobuf.struct_pb2 import Value

from apache_beam import version as beam_version
from apache_beam.internal.gcp.auth import get_service_credentials
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.io.gcp.gcsio import create_storage_client
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.internal import names as shared_names
from apache_beam.runners.pipeline_utils import validate_pipeline_graph
from apache_beam.runners.portability.stager import Stager
# from apache_beam.transforms import DataflowDistributionCounter
from apache_beam.transforms import cy_combiners
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.environments import is_apache_beam_container
from apache_beam.utils import retry
from apache_beam.utils import proto_utils

# Environment version information. It is passed to the service during a
# a job submission and is used by the service to establish what features
# are expected by the workers.
_LEGACY_ENVIRONMENT_MAJOR_VERSION = '8'
_FNAPI_ENVIRONMENT_MAJOR_VERSION = '8'

_LOGGER = logging.getLogger(__name__)

_PYTHON_VERSIONS_SUPPORTED_BY_DATAFLOW = ['3.10', '3.11', '3.12', '3.13']


class Environment(object):
  """Wrapper for a dataflow Environment protobuf."""
  def __init__(
      self,
      packages,
      options,
      environment_version,
      proto_pipeline_staged_url,
      proto_pipeline=None):
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    self.worker_options = options.view_as(WorkerOptions)
    self.debug_options = options.view_as(DebugOptions)
    self.pipeline_url = proto_pipeline_staged_url
    self.proto = dataflow.Environment()
    self.proto.cluster_manager_api_service = GoogleCloudOptions.COMPUTE_API_SERVICE  # pylint: disable=line-too-long
    self.proto.dataset = '{}/cloud_dataflow'.format(
        GoogleCloudOptions.BIGQUERY_API_SERVICE)
    self.proto.temp_storage_prefix = (
        self.google_cloud_options.temp_location.replace(
            'gs:/', GoogleCloudOptions.STORAGE_API_SERVICE))
    if self.worker_options.worker_region:
      self.proto.worker_region = self.worker_options.worker_region
    if self.worker_options.worker_zone:
      self.proto.worker_zone = self.worker_options.worker_zone
    # User agent information.
    user_agent = Struct(
        fields={
            'name': Value(string_value=self._get_python_sdk_name()),
            'version': Value(string_value=beam_version.__version__)
        })
    self.local = 'localhost' in self.google_cloud_options.dataflow_endpoint
    self._proto_pipeline = proto_pipeline

    if self.google_cloud_options.service_account_email:
      self.proto.service_account_email = (
          self.google_cloud_options.service_account_email)
    if self.google_cloud_options.dataflow_kms_key:
      self.proto.service_kms_key_name = self.google_cloud_options.dataflow_kms_key  # pylint: disable=line-too-long

    self.proto.user_agent = user_agent

    # Version information.
    _verify_interpreter_version_is_supported(options)
    if self.standard_options.streaming:
      job_type = 'FNAPI_STREAMING'
    else:
      job_type = 'FNAPI_BATCH'
    version = Struct(
        fields={
            'job_type': Value(string_value=job_type),
            'major': Value(string_value=environment_version)
        })
    self.proto.version = version
    # TODO: Use enumerated type instead of strings for job types.
    if job_type.startswith('FNAPI_'):
      self.debug_options.experiments = self.debug_options.experiments or []

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
      self.proto.flex_resource_scheduling_goal = (
          dataflow.FlexResourceSchedulingGoal.FLEXRS_COST_OPTIMIZED)
    elif self.google_cloud_options.flexrs_goal == 'SPEED_OPTIMIZED':
      self.proto.flex_resource_scheduling_goal = (
          dataflow.FlexResourceSchedulingGoal.FLEXRS_SPEED_OPTIMIZED)
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
        taskrunner_settings=dataflow.TaskRunnerSettings(
            parallel_worker_settings=dataflow.WorkerSettings(
                base_url=GoogleCloudOptions.DATAFLOW_ENDPOINT,
                service_path=self.google_cloud_options.dataflow_endpoint)))

    pool.autoscaling_settings = dataflow.AutoscalingSettings()
    # Set worker pool options received through command line.
    if self.worker_options.num_workers:
      pool.num_workers = self.worker_options.num_workers
    if self.worker_options.max_num_workers:
      pool.autoscaling_settings.max_num_workers = (
          self.worker_options.max_num_workers)
    if self.worker_options.autoscaling_algorithm:
      values_enum = dataflow.AutoscalingSettings.algorithm
      pool.autoscaling_settings.algorithm = {
          'NONE': values_enum.AUTOSCALING_ALGORITHM_NONE,
          'THROUGHPUT_BASED': values_enum.AUTOSCALING_ALGORITHM_BASIC,
      }.get(self.worker_options.autoscaling_algorithm)
    if self.worker_options.machine_type:
      pool.machine_type = self.worker_options.machine_type
    if self.worker_options.disk_size_gb:
      pool.disk_size_gb = self.worker_options.disk_size_gb
    if self.worker_options.disk_type:
      pool.disk_type = self.worker_options.disk_type
    if self.worker_options.zone:
      pool.zone = self.worker_options.zone
    if self.worker_options.network:
      pool.network = self.worker_options.network
    if self.worker_options.subnetwork:
      pool.subnetwork = self.worker_options.subnetwork

    # Setting worker pool sdk_harness_container_images option for supported
    # Dataflow workers.
    environments_to_use = self._get_environments_from_tranforms()

    # Adding container images for other SDKs that may be needed for
    # cross-language pipelines.
    for id, environment in environments_to_use:
      if environment.urn != common_urns.environments.DOCKER.urn:
        raise Exception(
            'Dataflow can only execute pipeline steps in Docker environments.'
            ' Received %r.' % environment)
      environment_payload = proto_utils.parse_Bytes(
          environment.payload, beam_runner_api_pb2.DockerPayload)
      container_image_url = environment_payload.container_image

      container_image = dataflow.SdkHarnessContainerImage()
      container_image.container_image = container_image_url
      container_image.use_single_core_per_container = (
          common_urns.protocols.MULTI_CORE_BUNDLE_PROCESSING.urn
          not in environment.capabilities)
      container_image.environment_id = id
      for capability in environment.capabilities:
        container_image.capabilities.append(capability)
      pool.sdk_harness_container_images.append(container_image)

    if not pool.sdk_harness_container_images:
      pool.worker_harness_container_image = (
          get_container_image_from_options(options))
    elif len(pool.sdk_harness_container_images) == 1:
      # Dataflow expects a value here when there is only one environment.
      pool.worker_harness_container_image = (
          pool.sdk_harness_container_images[0].container_image)

    if self.debug_options.number_of_worker_harness_threads:
      pool.num_threads_per_worker = (
          self.debug_options.number_of_worker_harness_threads)
    if self.worker_options.use_public_ips is not None:
      if self.worker_options.use_public_ips:
        pool.ip_configuration = (
            dataflow.WorkerIPAddressConfiguration.WORKER_IP_PUBLIC)
      else:
        pool.ip_configuration = (
            dataflow.WorkerIPAddressConfiguration.WORKER_IP_PRIVATE)

    if self.standard_options.streaming:
      # Use separate data disk for streaming.
      disk = dataflow.Disk()
      if self.local:
        disk.disk_type = 'local'
      if self.worker_options.disk_type:
        disk.disk_type = self.worker_options.disk_type
      pool.data_disks.append(disk)
    self.proto.worker_pools.append(pool)

    sdk_pipeline_options = options.get_all_options(retain_unknown_options=True)
    if sdk_pipeline_options:

      options_dict = {
          k: v
          for k, v in sdk_pipeline_options.items() if v is not None
      }
      options_dict["pipelineUrl"] = proto_pipeline_staged_url
      # Don't pass impersonate_service_account through to the harness.
      # Though impersonation should start a job, the workers should
      # not try to modify their credentials.
      options_dict.pop('impersonate_service_account', None)

      dd = DisplayData.create_from_options(options)
      items = [item.get_dict() for item in dd.items]

      self.proto.sdk_pipeline_options = Struct(
          fields={
              'options': Value(struct_value=options_dict),
              'display_data': Value(list_value=items)
          })

    if self.google_cloud_options.dataflow_service_options:
      for option in self.google_cloud_options.dataflow_service_options:
        self.proto.service_options.append(option)

    if self.google_cloud_options.enable_hot_key_logging:
      self.proto.debug_options = dataflow.DebugOptions(
          enable_hot_key_logging=True)

  def _get_environments_from_tranforms(self):
    if not self._proto_pipeline:
      return []

    environment_ids = set(
        transform.environment_id
        for transform in self._proto_pipeline.components.transforms.values()
        if transform.environment_id)

    return [(id, self._proto_pipeline.components.environments[id])
            for id in environment_ids]

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
      return str(shortened), length

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
        json.loads(json_format.MessageToJson(self.proto._pb)),
        indent=2,
        sort_keys=True)

  @staticmethod
  def _build_default_job_name(user_name):
    """Generates a default name for a job.

    user_name is lowercased, and any characters outside of [-a-z0-9]
    are removed. If necessary, the user_name is truncated to shorten
    the job name to 63 characters."""
    user_name = re.sub('[^-a-z0-9]', '', user_name.lower())
    date_component = datetime.now(timezone.utc).strftime('%m%d%H%M%S-%f')
    app_user_name = 'beamapp-{}'.format(user_name)
    # append 8 random alphanumeric characters to avoid collisions.
    random_component = ''.join(
        random.choices(string.ascii_lowercase + string.digits, k=8))
    job_name = '{}-{}-{}'.format(
        app_user_name, date_component, random_component)
    if len(job_name) > 63:
      job_name = '{}-{}-{}'.format(
          app_user_name[:-(len(job_name) - 63)],
          date_component,
          random_component)
    return job_name

  @staticmethod
  def default_job_name(job_name):
    if job_name is None:
      job_name = Job._build_default_job_name(getpass.getuser())
    return job_name

  def __init__(self, options, proto_pipeline):
    self.options = options
    validate_pipeline_graph(proto_pipeline)
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

    self.root_staging_location = self.google_cloud_options.staging_location

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
      self.proto.type_ = dataflow.JobType.JOB_TYPE_STREAMING
    else:
      self.proto.type_ = dataflow.JobType.JOB_TYPE_BATCH
    if self.google_cloud_options.update:
      self.proto.replace_job_id = self.job_id_for_name(self.proto.name)
      if self.google_cloud_options.transform_name_mapping:
        self.proto.transform_name_mapping = Struct()
        for _, (key, value) in enumerate(
            self.google_cloud_options.transform_name_mapping.items()):
          self.proto.transform_name_mapping.update({key: value})
    if self.google_cloud_options.create_from_snapshot:
      self.proto.created_from_snapshot_id = (
          self.google_cloud_options.create_from_snapshot)
    # Labels.
    if self.google_cloud_options.labels:
      self.proto.labels = Struct()
      labels = self.google_cloud_options.labels
      if isinstance(labels, str):
        labels = [labels]
      elif isinstance(labels, dict):
        labels = [str(labels)]
      for label in labels:
        if '{' in label:
          label = ast.literal_eval(label)
          for key, value in label.items():
            self.proto.labels.update({key: value})
        else:
          parts = label.split('=', 1)
          key = parts[0]
          value = parts[1] if len(parts) > 1 else ''
          self.proto.labels.update({key: value})

    # Client Request ID
    self.proto.client_request_id = '{}-{}'.format(
        datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f'),
        random.randrange(9000) + 1000)

    self.base64_str_re = re.compile(r'^[A-Za-z0-9+/]*=*$')
    self.coder_str_re = re.compile(r'^([A-Za-z]+\$)([A-Za-z0-9+/]*=*)$')

  def job_id_for_name(self, job_name):
    return DataflowApplicationClient(
        self.google_cloud_options).job_id_for_name(job_name)

  def json(self):
    return json_format.MessageToJson(self.proto._pb)

  def __reduce__(self):
    """Reduce hook for pickling the Job class more easily."""
    return (Job, (self.options, ))


class DataflowApplicationClient(object):
  _HASH_CHUNK_SIZE = 1024 * 8
  _GCS_CACHE_PREFIX = "artifact_cache"
  """A Dataflow API client used by application code to create and query jobs."""
  def __init__(self, options, root_staging_location=None):
    """Initializes a Dataflow API client object."""
    self.standard_options = options.view_as(StandardOptions)
    self.google_cloud_options = options.view_as(GoogleCloudOptions)
    self._enable_caching = self.google_cloud_options.enable_artifact_caching
    self._enable_bucket_read_metric_counter = \
      self.google_cloud_options.enable_bucket_read_metric_counter
    self._enable_bucket_write_metric_counter =\
      self.google_cloud_options.enable_bucket_write_metric_counter
    self._root_staging_location = (
        root_staging_location or self.google_cloud_options.staging_location)
    self.environment_version = _FNAPI_ENVIRONMENT_MAJOR_VERSION

    if self.google_cloud_options.no_auth:
      credentials = None
    else:
      credentials = get_service_credentials(options)

    self._jobs_client = dataflow.JobsV1Beta3Client(credentials=credentials)
    self._messages_client = dataflow.MessagesV1Beta3Client(
        credentials=credentials)
    self._metrics_client = dataflow.MetricsV1Beta3Client(
        credentials=credentials)
    self._storage_client = create_storage_client(
        options, not self.google_cloud_options.no_auth)
    self._sdk_image_overrides = self._get_sdk_image_overrides(options)

  def _get_sdk_image_overrides(self, pipeline_options):
    worker_options = pipeline_options.view_as(WorkerOptions)
    sdk_overrides = worker_options.sdk_harness_container_image_overrides
    return (
        dict(s.split(',', 1) for s in sdk_overrides) if sdk_overrides else {})

  @staticmethod
  def _compute_sha256(file):
    hasher = hashlib.sha256()
    with open(file, 'rb') as f:
      for chunk in iter(partial(f.read,
                                DataflowApplicationClient._HASH_CHUNK_SIZE),
                        b""):
        hasher.update(chunk)
    return hasher.hexdigest()

  def _cached_location(self, sha256):
    sha_prefix = sha256[0:2]
    return FileSystems.join(
        self._root_staging_location,
        DataflowApplicationClient._GCS_CACHE_PREFIX,
        sha_prefix,
        sha256)

  def _gcs_file_copy(self, from_path, to_path, sha256):
    if self._enable_caching and sha256:
      self._cached_gcs_file_copy(from_path, to_path, sha256)
    else:
      self._uncached_gcs_file_copy(from_path, to_path)

  def _cached_gcs_file_copy(self, from_path, to_path, sha256):
    cached_path = self._cached_location(sha256)
    if FileSystems.exists(cached_path):
      _LOGGER.info(
          'Skipping upload of %s because it already exists at %s',
          to_path,
          cached_path)
    else:
      self._uncached_gcs_file_copy(from_path, cached_path)

    FileSystems.copy(
        source_file_names=[cached_path], destination_file_names=[to_path])
    _LOGGER.info('Copied cached artifact from %s to %s', from_path, to_path)

  def _uncached_gcs_file_copy(self, from_path, to_path):
    to_folder, to_name = os.path.split(to_path)
    total_size = os.path.getsize(from_path)
    self.stage_file_with_retry(
        to_folder, to_name, from_path, total_size=total_size)

  def _stage_resources(self, pipeline, options):
    google_cloud_options = options.view_as(GoogleCloudOptions)
    if google_cloud_options.staging_location is None:
      raise RuntimeError('The --staging_location option must be specified.')
    if google_cloud_options.temp_location is None:
      raise RuntimeError('The --temp_location option must be specified.')

    resources = []
    staged_paths = {}
    staged_hashes = {}
    for _, env in sorted(pipeline.components.environments.items(),
                         key=lambda kv: kv[0]):
      for dep in env.dependencies:
        if dep.type_urn != common_urns.artifact_types.FILE.urn:
          raise RuntimeError('unsupported artifact type %s' % dep.type_urn)
        type_payload = beam_runner_api_pb2.ArtifactFilePayload.FromString(
            dep.type_payload)

        if dep.role_urn == common_urns.artifact_roles.STAGING_TO.urn:
          remote_name = (
              beam_runner_api_pb2.ArtifactStagingToRolePayload.FromString(
                  dep.role_payload)).staged_name
          is_staged_role = True
        else:
          remote_name = os.path.basename(type_payload.path)
          is_staged_role = False

        if self._enable_caching and not type_payload.sha256:
          type_payload.sha256 = self._compute_sha256(type_payload.path)

        if type_payload.sha256 and type_payload.sha256 in staged_hashes:
          _LOGGER.info(
              'Found duplicated artifact sha256: %s (%s)',
              type_payload.path,
              type_payload.sha256)
          remote_name = staged_hashes[type_payload.sha256]
          if is_staged_role:
            # We should not be overriding this, as dep.role_payload.staged_name
            # refers to the desired name on the worker, whereas staged_name
            # refers to its placement in a distributed filesystem.
            # TODO(heejong): Clean this up.
            dep.role_payload = beam_runner_api_pb2.ArtifactStagingToRolePayload(
                staged_name=remote_name).SerializeToString()
        elif type_payload.path and type_payload.path in staged_paths:
          _LOGGER.info(
              'Found duplicated artifact path: %s (%s)',
              type_payload.path,
              type_payload.sha256)
          remote_name = staged_paths[type_payload.path]
          if is_staged_role:
            # We should not be overriding this, as dep.role_payload.staged_name
            # refers to the desired name on the worker, whereas staged_name
            # refers to its placement in a distributed filesystem.
            # TODO(heejong): Clean this up.
            dep.role_payload = beam_runner_api_pb2.ArtifactStagingToRolePayload(
                staged_name=remote_name).SerializeToString()
        else:
          resources.append(
              (type_payload.path, remote_name, type_payload.sha256))
          staged_paths[type_payload.path] = remote_name
          staged_hashes[type_payload.sha256] = remote_name

        if FileSystems.get_scheme(
            google_cloud_options.staging_location) == GCSFileSystem.scheme():
          dep.type_urn = common_urns.artifact_types.URL.urn
          dep.type_payload = beam_runner_api_pb2.ArtifactUrlPayload(
              url=FileSystems.join(
                  google_cloud_options.staging_location, remote_name),
              sha256=type_payload.sha256).SerializeToString()
        else:
          dep.type_payload = beam_runner_api_pb2.ArtifactFilePayload(
              path=FileSystems.join(
                  google_cloud_options.staging_location, remote_name),
              sha256=type_payload.sha256).SerializeToString()

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
    from google.cloud.exceptions import Forbidden
    from google.cloud.exceptions import NotFound
    if not gcs_or_local_path.startswith('gs://'):
      local_path = FileSystems.join(gcs_or_local_path, file_name)
      _LOGGER.info('Staging file locally to %s', local_path)
      with open(local_path, 'wb') as f:
        f.write(stream.read())
      return
    gcs_location = FileSystems.join(gcs_or_local_path, file_name)
    bucket_name, blob_name = gcs_location[5:].split('/', 1)
    start_time = time.time()
    _LOGGER.info('Starting GCS upload to %s...', gcs_location)
    try:
      from google.cloud.storage import Blob
      from google.cloud.storage.fileio import BlobWriter
      bucket = self._storage_client.get_bucket(bucket_name)
      blob = bucket.get_blob(blob_name)
      if not blob:
        blob = Blob(blob_name, bucket)
      with BlobWriter(blob) as f:
        f.write(stream.read())
      _LOGGER.info(
          'Completed GCS upload to %s in %s seconds.',
          gcs_location,
          int(time.time() - start_time))
      return
    except Exception as e:
      reportable_errors = [
          Forbidden,
          NotFound,
      ]
      if type(e) in reportable_errors:
        raise IOError((
            'Could not upload to GCS path %s: %s. Please verify '
            'that credentials are valid, that the specified path '
            'exists, and that you have write access to it.') %
                      (gcs_or_local_path, e))
      raise

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def stage_file_with_retry(
      self,
      gcs_or_local_path,
      file_name,
      stream_or_path,
      mime_type='application/octet-stream',
      total_size=None):

    if isinstance(stream_or_path, str):
      path = stream_or_path
      with open(path, 'rb') as stream:
        self.stage_file(
            gcs_or_local_path, file_name, stream, mime_type, total_size)
    elif isinstance(stream_or_path, io.IOBase):
      stream = stream_or_path
      try:
        self.stage_file(
            gcs_or_local_path, file_name, stream, mime_type, total_size)
      except Exception as exn:
        if stream.seekable():
          # reset cursor for possible retrying
          stream.seek(0)
          raise exn
        else:
          raise retry.PermanentException(
              "Skip retrying because we caught exception:" +
              ''.join(traceback.format_exception_only(exn.__class__, exn)) +
              ', but the stream is not seekable.')
    else:
      raise retry.PermanentException(
          "Skip retrying because type " + str(type(stream_or_path)) +
          "stream_or_path is unsupported.")

  @retry.no_retries  # Using no_retries marks this as an integration point.
  def create_job(self, job: Job):
    """Creates job description. May stage and/or submit for remote execution."""
    self.create_job_description(job)

    # Stage and submit the job when necessary
    dataflow_job_file = job.options.view_as(DebugOptions).dataflow_job_file
    template_location = (
        job.options.view_as(GoogleCloudOptions).template_location)

    if job.options.view_as(DebugOptions).lookup_experiment('upload_graph'):
      self.stage_file_with_retry(
          job.options.view_as(GoogleCloudOptions).staging_location,
          "dataflow_graph.json",
          io.BytesIO(job.json().encode('utf-8')))
      del job.proto.steps[:]
      job.proto.steps_location = FileSystems.join(
          job.options.view_as(GoogleCloudOptions).staging_location,
          "dataflow_graph.json")

    # template file generation should be placed immediately before the
    # conditional API call.
    job_location = template_location or dataflow_job_file
    if job_location:
      gcs_or_local_path = os.path.dirname(job_location)
      file_name = os.path.basename(job_location)
      self.stage_file_with_retry(
          gcs_or_local_path, file_name, io.BytesIO(job.json().encode('utf-8')))

    if not template_location:
      return self.submit_job_description(job)

    _LOGGER.info(
        'A template was just created at location %s', template_location)
    return None

  @staticmethod
  def _update_container_image_for_dataflow(beam_container_image_url):
    # By default Dataflow pipelines use containers hosted in Dataflow GCR
    # instead of Docker Hub.
    image_suffix = beam_container_image_url.rsplit('/', 1)[1]

    # trim "RCX" as release candidate tag exists on Docker Hub but not GCR
    check_rc = image_suffix.lower().split('rc')
    if len(check_rc) == 2:
      image_suffix = image_suffix[:-2 - len(check_rc[1])]

    return names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/' + image_suffix

  @staticmethod
  def _apply_sdk_environment_overrides(
      proto_pipeline, sdk_overrides, pipeline_options):
    # Updates container image URLs for Dataflow.
    # For a given container image URL
    # * If a matching override has been provided that will be used.
    # * For improved performance, External Apache Beam container images that are
    #   not explicitly overridden will be
    #   updated to use GCR copies instead of directly downloading from the
    #   Docker Hub.

    current_sdk_container_image = get_container_image_from_options(
        pipeline_options)

    for environment in proto_pipeline.components.environments.values():
      docker_payload = proto_utils.parse_Bytes(
          environment.payload, beam_runner_api_pb2.DockerPayload)
      overridden = False
      new_container_image = docker_payload.container_image
      for pattern, override in sdk_overrides.items():
        new_container_image = re.sub(pattern, override, new_container_image)
        if new_container_image != docker_payload.container_image:
          overridden = True

      # Container of the current (Python) SDK is overridden separately, hence
      # not updated here.
      if (is_apache_beam_container(new_container_image) and not overridden and
          new_container_image != current_sdk_container_image):
        new_container_image = (
            DataflowApplicationClient._update_container_image_for_dataflow(
                docker_payload.container_image))

      if not new_container_image:
        raise ValueError(
            'SDK Docker container image has to be a non-empty string')

      new_payload = copy(docker_payload)
      new_payload.container_image = new_container_image
      environment.payload = new_payload.SerializeToString()

  def create_job_description(self, job: Job):
    """Creates a job described by the workflow proto."""
    DataflowApplicationClient._apply_sdk_environment_overrides(
        job.proto_pipeline, self._sdk_image_overrides, job.options)

    # Stage other resources for the SDK harness
    resources = self._stage_resources(job.proto_pipeline, job.options)

    # Stage proto pipeline.
    self.stage_file_with_retry(
        job.google_cloud_options.staging_location,
        shared_names.STAGED_PIPELINE_FILENAME,
        io.BytesIO(job.proto_pipeline.SerializeToString()))

    job.proto.environment = Environment(
        proto_pipeline_staged_url=FileSystems.join(
            job.google_cloud_options.staging_location,
            shared_names.STAGED_PIPELINE_FILENAME),
        packages=resources,
        options=job.options,
        environment_version=self.environment_version,
        proto_pipeline=job.proto_pipeline).proto
    _LOGGER.debug('JOB: %s', job)

  @retry.with_exponential_backoff(num_retries=3, initial_delay_secs=3)
  def get_job_metrics(self, job_id) -> dataflow.JobMetrics:
    request = dataflow.GetJobMetricsRequest()
    request.job_id = job_id
    request.location = self.google_cloud_options.region
    request.project_id = self.google_cloud_options.project
    try:
      response = self._metrics_client.get_job_metrics(request)
    except exceptions.GoogleAPICallError as e:
      _LOGGER.error('HTTP status %d. Unable to query metrics', e.code)
      raise
    return response

  @retry.with_exponential_backoff(num_retries=3)
  def submit_job_description(self, job: Job) -> dataflow.Job:
    """Creates and excutes a job request."""
    request = dataflow.CreateJobRequest()
    request.project_id = self.google_cloud_options.project
    request.location = self.google_cloud_options.region
    request.job = job.proto

    try:
      response = self._jobs_client.create_job(request=request)
    except exceptions.GoogleAPICallError as e:
      _LOGGER.error(
          'HTTP status %d trying to create job'
          ' at dataflow service endpoint %s',
          e.code,
          self.google_cloud_options.dataflow_endpoint)
      _LOGGER.fatal('details of server error: %s', e)
      raise

    if response.client_request_id and \
        response.client_request_id != job.proto.client_request_id:
      if self.google_cloud_options.update:
        raise DataflowJobAlreadyExistsError(
            "The job named %s with id: %s has already been updated into job "
            "id: %s and cannot be updated again." %
            (response.name, job.proto.replace_job_id, response.id))
      else:
        raise DataflowJobAlreadyExistsError(
            'There is already active job named %s with id: %s. If you want to '
            'submit a second job, try again by setting a different name using '
            '--job_name.' % (response.name, response.id))

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
  def modify_job_state(self, job_id, new_state) -> bool:
    """Modify the run state of the job.

    Args:
      job_id: The id of the job.
      new_state: A string representing the new desired state. It could be set to
      either 'JOB_STATE_DONE', 'JOB_STATE_CANCELLED' or 'JOB_STATE_DRAINING'.

    Returns:
      True if the job was modified successfully.
    """
    if new_state == 'JOB_STATE_DONE':
      new_state = dataflow.JobState.JOB_STATE_DONE
    elif new_state == 'JOB_STATE_CANCELLED':
      new_state = dataflow.JobState.JOB_STATE_CANCELLED
    elif new_state == 'JOB_STATE_DRAINING':
      new_state = dataflow.JobState.JOB_STATE_DRAINING
    else:
      # Other states could only be set by the service.
      return False

    request = dataflow.UpdateJobRequest()
    request.job_id = job_id
    request.project_id = self.google_cloud_options.project
    request.location = self.google_cloud_options.region
    request.job = dataflow.Job(requested_state=new_state)

    self._jobs_client.update_job(request=request)
    return True

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_notfound_filter)
  def get_job(self, job_id) -> dataflow.Job:
    """Gets the job status for a submitted job.

    Args:
      job_id: A string representing the job_id for the workflow as returned
        by the create_job() request.

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
    request = dataflow.GetJobRequest()
    request.job_id = job_id
    request.project_id = self.google_cloud_options.project
    request.location = self.google_cloud_options.region
    response = self._jobs_client.get_job(request)
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
        by the create_job() request.
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
    request = dataflow.ListJobMessagesRequest(
        job_id=job_id,
        location=self.google_cloud_options.region,
        project_id=self.google_cloud_options.project)
    if page_token is not None:
      request.page_token = page_token
    if start_time is not None:
      request.start_time = start_time
    if end_time is not None:
      request.end_time = end_time
    if minimum_importance is not None:
      if minimum_importance == 'JOB_MESSAGE_DEBUG':
        request.minimum_importance = (
            dataflow.JobMessageImportance.JOB_MESSAGE_DEBUG)
      elif minimum_importance == 'JOB_MESSAGE_DETAILED':
        request.minimumImportance = (
            dataflow.JobMessageImportance.JOB_MESSAGE_DETAILED)
      elif minimum_importance == 'JOB_MESSAGE_BASIC':
        request.minimumImportance = (
            dataflow.JobMessageImportance.JOB_MESSAGE_BASIC)
      elif minimum_importance == 'JOB_MESSAGE_WARNING':
        request.minimumImportance = (
            dataflow.JobMessageImportance.JOB_MESSAGE_WARNING)
      elif minimum_importance == 'JOB_MESSAGE_ERROR':
        request.minimumImportance = (
            dataflow.JobMessageImportance.JOB_MESSAGE_ERROR)
      else:
        raise RuntimeError(
            'Unexpected value for minimum_importance argument: %r' %
            minimum_importance)
    response = self._messages_client.list_job_messages(request=request)
    return response.job_messages, response.next_page_token

  def job_id_for_name(self, job_name):
    token = None
    while True:
      request = dataflow.ListJobsRequest(
          project_id=self.google_cloud_options.project,
          location=self.google_cloud_options.region,
          page_token=token)
      response = self._jobs_client.list_jobs(request)
      for job in response:
        if (job.name == job_name and
            job.current_state in [dataflow.JobState.JOB_STATE_RUNNING,
                                  dataflow.JobState.JOB_STATE_DRAINING]):
          return job.id
      token = response.next_page_token
      if token is None:
        raise ValueError("No running job found with name '%s'" % job_name)


class MetricUpdateTranslators(object):
  """Translators between accumulators and dataflow metric updates."""
  @staticmethod
  def translate_boolean(
      accumulator, metric_update_proto: dataflow.MetricUpdate):
    metric_update_proto.scalar = accumulator.value

  @staticmethod
  def translate_scalar_mean_int(
      accumulator, metric_update_proto: dataflow.MetricUpdate):
    if accumulator.count:
      metric_update_proto.kind = 'Mean'
      metric_update_proto.mean_sum = accumulator.sum
      metric_update_proto.mean_count = accumulator.count
    else:
      metric_update_proto.kind = None

  @staticmethod
  def translate_scalar_mean_float(
      accumulator, metric_update_proto: dataflow.MetricUpdate):
    if accumulator.count:
      metric_update_proto.kind = 'Mean'
      metric_update_proto.mean_sum = accumulator.sum
      metric_update_proto.mean_count = accumulator.count
    else:
      metric_update_proto.kind = None

  @staticmethod
  def translate_scalar_counter_int(
      accumulator, metric_update_proto: dataflow.MetricUpdate):
    metric_update_proto.scalar = accumulator.value

  @staticmethod
  def translate_scalar_counter_float(
      accumulator, metric_update_proto: dataflow.MetricUpdate):
    metric_update_proto.scalar = accumulator.value


class _LegacyDataflowStager(Stager):
  def __init__(self, dataflow_application_client: DataflowApplicationClient):
    super().__init__()
    self._dataflow_application_client = dataflow_application_client

  def stage_artifact(self, local_path_to_artifact, artifact_name, sha256):
    self._dataflow_application_client._gcs_file_copy(
        local_path_to_artifact, artifact_name, sha256)

  def commit_manifest(self):
    pass

  @staticmethod
  def get_sdk_package_name():
    """For internal use only; no backwards-compatibility guarantees.

          Returns the PyPI package name to be staged to Google Cloud Dataflow.
    """
    return shared_names.BEAM_PACKAGE_NAME


class DataflowJobAlreadyExistsError(retry.PermanentException):
  """A non-retryable exception that a job with the given name already exists."""
  # Inherits retry.PermanentException to avoid retry in
  # DataflowApplicationClient.submit_job_description
  pass


# TODO: Used in legacy batch worker. Move under MetricUpdateTranslators
# after Runner V2 transition.
def translate_distribution(
    distribution_update, metric_update_proto: dataflow.MetricUpdate):
  """Translate metrics DistributionUpdate to dataflow distribution update.

  Args:
    distribution_update: Instance of DistributionData,
    DistributionInt64Accumulator or DataflowDistributionCounter.
    metric_update_proto: Used for report metrics.
  """
  dist_update_proto = Struct()
  dist_update_proto.update({"min": distribution_update.min})
  dist_update_proto.update({"max": distribution_update.max})
  dist_update_proto.update({"count": distribution_update.count})
  dist_update_proto.update({"sum": distribution_update.sum})
  # DataflowDistributionCounter needs to translate histogram
  # if isinstance(distribution_update, DataflowDistributionCounter):
  #   dist_update_proto.histogram = dataflow.Histogram()
  #   distribution_update.translate_to_histogram(dist_update_proto.histogram)
  metric_update_proto.distribution = dist_update_proto


# TODO: Used in legacy batch worker. Delete after Runner V2 transition.
def translate_value(value, metric_update_proto: dataflow.MetricUpdate):
  metric_update_proto.scalar = value


def _get_container_image_tag():
  base_version = version.parse(beam_version.__version__).base_version
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
  if worker_options.sdk_container_image:
    return worker_options.sdk_container_image

  # Legacy and runner v2 exist in different repositories.
  # Set to legacy format, override if runner v2
  container_repo = names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY
  image_name = '{repository}/beam_python{major}.{minor}_sdk'.format(
      repository=container_repo,
      major=sys.version_info[0],
      minor=sys.version_info[1])

  image_tag = _get_required_container_version()
  return image_name + ':' + image_tag


def _get_required_container_version():
  """For internal use only; no backwards-compatibility guarantees.

    Returns:
      str: The tag of worker container images in GCR that corresponds to
        current version of the SDK.
    """
  if 'dev' in beam_version.__version__:
    return names.BEAM_DEV_SDK_CONTAINER_TAG
  else:
    return _get_container_image_tag()


def get_response_encoding():
  """Encoding to use to decode HTTP response from Google APIs."""
  return 'utf8'


def _verify_interpreter_version_is_supported(pipeline_options):
  if ('%s.%s' % (sys.version_info[0], sys.version_info[1])
      in _PYTHON_VERSIONS_SUPPORTED_BY_DATAFLOW):
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
# TODO(https://github.com/apache/beam/issues/19433) : Remove this antipattern,
# legacy dataflow python pipelines will break whenever a new cy_combiner type
# is used.
structured_counter_translations = {
    cy_combiners.CountCombineFn: (
        'Sum', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.SumInt64Fn: (
        'Sum', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MinInt64Fn: (
        'Min', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MaxInt64Fn: (
        'Max', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MeanInt64Fn: (
        'Mean', MetricUpdateTranslators.translate_scalar_mean_int),
    cy_combiners.SumFloatFn: (
        'Sum', MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MinFloatFn: (
        'Min', MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MaxFloatFn: (
        'Max', MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MeanFloatFn: (
        'Mean', MetricUpdateTranslators.translate_scalar_mean_float),
    cy_combiners.AllCombineFn: (
        'And', MetricUpdateTranslators.translate_boolean),
    cy_combiners.AnyCombineFn: (
        'Or', MetricUpdateTranslators.translate_boolean),
    cy_combiners.DataflowDistributionCounterFn: (
        'Distribution', translate_distribution),
    cy_combiners.DistributionInt64Fn: ('Distribution', translate_distribution),
}

counter_translations = {
    cy_combiners.CountCombineFn: (
        'Sum', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.SumInt64Fn: (
        'Sum', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MinInt64Fn: (
        'Min', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MaxInt64Fn: (
        'Max', MetricUpdateTranslators.translate_scalar_counter_int),
    cy_combiners.MeanInt64Fn: (
        'Mean', MetricUpdateTranslators.translate_scalar_mean_int),
    cy_combiners.SumFloatFn: (
        'Sum', MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MinFloatFn: (
        'Min', MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MaxFloatFn: (
        'Max', MetricUpdateTranslators.translate_scalar_counter_float),
    cy_combiners.MeanFloatFn: (
        'Mean', MetricUpdateTranslators.translate_scalar_mean_float),
    cy_combiners.AllCombineFn: (
        'And', MetricUpdateTranslators.translate_boolean),
    cy_combiners.AnyCombineFn: (
        'Or', MetricUpdateTranslators.translate_boolean),
    cy_combiners.DataflowDistributionCounterFn: (
        'Distribution', translate_distribution),
    cy_combiners.DistributionInt64Fn: ('Distribution', translate_distribution),
}
