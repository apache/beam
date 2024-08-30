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

"""SdkContainerImageBuilder builds the portable SDK container with dependencies.

It copies the right boot dependencies, namely: apache beam sdk, python packages
from requirements.txt, python packages from extra_packages.txt, workflow
tarball, into the latest public python sdk container image, and run the
dependencies installation in advance with the boot program in setup only mode
to build the new image.
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from typing import Type

from google.protobuf.json_format import MessageToJson

from apache_beam import version as beam_version
from apache_beam.internal.gcp.auth import get_service_credentials
from apache_beam.internal.http_client import get_new_http
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions  # pylint: disable=unused-import
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.dataflow.internal.clients import cloudbuild
from apache_beam.runners.portability.stager import Stager
from apache_beam.utils import plugin

ARTIFACTS_CONTAINER_DIR = '/opt/apache/beam/artifacts'
ARTIFACTS_MANIFEST_FILE = 'artifacts_info.json'
SDK_CONTAINER_ENTRYPOINT = '/opt/apache/beam/boot'
DOCKERFILE_TEMPLATE = (
    """FROM {base_image}
RUN mkdir -p {workdir}
COPY ./* {workdir}/
RUN {entrypoint} --setup_only --artifacts {workdir}/{manifest_file}
""")

SOURCE_FOLDER = 'source'
_LOGGER = logging.getLogger(__name__)


class SdkContainerImageBuilder(plugin.BeamPlugin):
  def __init__(self, options):
    self._options = options
    self._docker_registry_push_url = self._options.view_as(
        SetupOptions).docker_registry_push_url
    version = (
        beam_version.__version__
        if 'dev' not in beam_version.__version__ else 'latest')
    self._base_image = (
        self._options.view_as(WorkerOptions).sdk_container_image or
        'apache/beam_python%s.%s_sdk:%s' %
        (sys.version_info[0], sys.version_info[1], version))
    self._temp_src_dir = None

  def _build(self):
    container_image_tag = str(uuid.uuid4())
    container_image_name = os.path.join(
        self._docker_registry_push_url or '',
        'beam_python_prebuilt_sdk:%s' % container_image_tag)
    with tempfile.TemporaryDirectory() as temp_folder:
      self._temp_src_dir = temp_folder
      self._prepare_dependencies()
      self._invoke_docker_build_and_push(container_image_name)

    return container_image_name

  def _prepare_dependencies(self):
    with tempfile.TemporaryDirectory() as tmp:
      artifacts = Stager.create_job_resources(self._options, tmp)
      resources = Stager.extract_staging_tuple_iter(artifacts)
      # make a copy of the staged artifacts into the temp source folder.
      file_names = []
      for path, name, _ in resources:
        shutil.copyfile(path, os.path.join(self._temp_src_dir, name))
        file_names.append(name)
      with open(os.path.join(self._temp_src_dir, 'Dockerfile'), 'w') as file:
        file.write(
            DOCKERFILE_TEMPLATE.format(
                base_image=self._base_image,
                workdir=ARTIFACTS_CONTAINER_DIR,
                manifest_file=ARTIFACTS_MANIFEST_FILE,
                entrypoint=SDK_CONTAINER_ENTRYPOINT))
      self._generate_artifacts_manifests_json_file(
          file_names, self._temp_src_dir)

  def _invoke_docker_build_and_push(self, container_image_name):
    raise NotImplementedError

  @classmethod
  def _builder_key(cls) -> str:
    return f'{cls.__module__}.{cls.__name__}'

  @staticmethod
  def _generate_artifacts_manifests_json_file(file_names, temp_dir):
    infos = []
    for name in file_names:
      info = beam_runner_api_pb2.ArtifactInformation(
          type_urn=common_urns.StandardArtifacts.Types.FILE.urn,
          type_payload=beam_runner_api_pb2.ArtifactFilePayload(
              path=name).SerializeToString(),
      )
      infos.append(json.dumps(MessageToJson(info)))
    with open(os.path.join(temp_dir, ARTIFACTS_MANIFEST_FILE), 'w') as file:
      file.write('[\n' + ',\n'.join(infos) + '\n]')

  @classmethod
  def build_container_image(cls, pipeline_options: PipelineOptions) -> str:
    setup_options = pipeline_options.view_as(SetupOptions)
    container_build_engine = setup_options.prebuild_sdk_container_engine
    builder_cls = cls._get_subclass_by_key(container_build_engine)
    builder = builder_cls(pipeline_options)
    return builder._build()

  @classmethod
  def _get_subclass_by_key(cls, key: str) -> Type['SdkContainerImageBuilder']:
    available_builders = [
        subclass for subclass in cls.get_all_subclasses()
        if subclass._builder_key() == key
    ]
    if not available_builders:
      available_builder_keys = [
          subclass._builder_key() for subclass in cls.get_all_subclasses()
      ]
      raise ValueError(
          f'Cannot find SDK builder type {key} in '
          f'{available_builder_keys}')
    elif len(available_builders) > 1:
      raise ValueError(f'Found multiple builders under key {key}')
    return available_builders[0]


class _SdkContainerImageLocalBuilder(SdkContainerImageBuilder):
  """SdkContainerLocalBuilder builds the sdk container image with local
  docker."""
  @classmethod
  def _builder_key(cls):
    return 'local_docker'

  def _invoke_docker_build_and_push(self, container_image_name):
    try:
      _LOGGER.info("Building sdk container, this may take a few minutes...")
      now = time.time()
      subprocess.run(['docker', 'build', '.', '-t', container_image_name],
                     check=True,
                     cwd=self._temp_src_dir)
    except subprocess.CalledProcessError as err:
      raise RuntimeError(
          'Failed to build sdk container with local docker, '
          'stderr:\n %s.' % err.stderr)
    else:
      _LOGGER.info(
          "Successfully built %s in %.2f seconds" %
          (container_image_name, time.time() - now))

    if self._docker_registry_push_url:
      _LOGGER.info("Pushing prebuilt sdk container...")
      try:
        subprocess.run(['docker', 'push', container_image_name], check=True)
      except subprocess.CalledProcessError as err:
        raise RuntimeError(
            'Failed to push prebuilt sdk container %s, stderr: \n%s' %
            (container_image_name, err.stderr))
      _LOGGER.info(
          "Successfully pushed %s in %.2f seconds" %
          (container_image_name, time.time() - now))
    else:
      _LOGGER.info(
          "no --docker_registry_push_url option is specified in pipeline "
          "options, specify it if the new image is intended to be "
          "pushed to a registry.")


class _SdkContainerImageCloudBuilder(SdkContainerImageBuilder):
  """SdkContainerLocalBuilder builds the sdk container image with google cloud
  build."""
  def __init__(self, options):
    super().__init__(options)
    self._google_cloud_options = options.view_as(GoogleCloudOptions)
    self._cloud_build_machine_type = self._get_cloud_build_machine_type_enum(
        options.view_as(SetupOptions).cloud_build_machine_type)
    if self._google_cloud_options.no_auth:
      credentials = None
    else:
      credentials = get_service_credentials(options)
    from apache_beam.io.gcp.gcsio import create_storage_client
    self._storage_client = create_storage_client(
        options, not self._google_cloud_options.no_auth)
    self._cloudbuild_client = cloudbuild.CloudbuildV1(
        credentials=credentials,
        get_credentials=(not self._google_cloud_options.no_auth),
        http=get_new_http(),
        response_encoding='utf8')
    if not self._docker_registry_push_url:
      self._docker_registry_push_url = (
          'gcr.io/%s/prebuilt_beam_sdk' % self._google_cloud_options.project)

  @classmethod
  def _builder_key(cls):
    return 'cloud_build'

  def _invoke_docker_build_and_push(self, container_image_name):
    project_id = self._google_cloud_options.project
    temp_location = self._google_cloud_options.temp_location
    # google cloud build service expects all the build source file to be
    # compressed into a tarball.
    tarball_path = os.path.join(self._temp_src_dir, '%s.tgz' % SOURCE_FOLDER)
    self._make_tarfile(tarball_path, self._temp_src_dir)
    _LOGGER.info(
        "Compressed source files for building sdk container at %s" %
        tarball_path)

    container_image_tag = container_image_name.split(':')[-1]
    gcs_location = os.path.join(
        temp_location, '%s-%s.tgz' % (SOURCE_FOLDER, container_image_tag))
    self._upload_to_gcs(tarball_path, gcs_location)

    build = cloudbuild.Build()
    if self._cloud_build_machine_type:
      build.options = cloudbuild.BuildOptions()
      build.options.machineType = self._cloud_build_machine_type
    build.steps = []
    step = cloudbuild.BuildStep()
    step.name = 'gcr.io/kaniko-project/executor:latest'
    # Disable compression caching to allow for large images to be cached.
    # See: https://github.com/GoogleContainerTools/kaniko/issues/1669
    step.args = [
        '--destination=' + container_image_name,
        '--cache=true',
        '--compressed-caching=false',
    ]
    step.dir = SOURCE_FOLDER

    build.steps.append(step)

    source = cloudbuild.Source()
    source.storageSource = cloudbuild.StorageSource()
    gcs_bucket, gcs_object = self._get_gcs_bucket_and_name(gcs_location)
    source.storageSource.bucket = os.path.join(gcs_bucket)
    source.storageSource.object = gcs_object
    build.source = source
    # TODO(zyichi): make timeout configurable
    build.timeout = '7200s'

    now = time.time()
    # operation = client.create_build(project_id=project_id, build=build)
    request = cloudbuild.CloudbuildProjectsBuildsCreateRequest(
        projectId=project_id, build=build)
    build = self._cloudbuild_client.projects_builds.Create(request)
    build_id, log_url = self._get_cloud_build_id_and_log_url(build.metadata)
    _LOGGER.info(
        'Building sdk container with Google Cloud Build, this may '
        'take a few minutes, you may check build log at %s' % log_url)

    # block until build finish, if build fails exception will be raised and
    # stops the job submission.
    response = self._cloudbuild_client.projects_builds.Get(
        cloudbuild.CloudbuildProjectsBuildsGetRequest(
            id=build_id, projectId=project_id))
    while response.status in [cloudbuild.Build.StatusValueValuesEnum.QUEUED,
                              cloudbuild.Build.StatusValueValuesEnum.PENDING,
                              cloudbuild.Build.StatusValueValuesEnum.WORKING]:
      time.sleep(10)
      response = self._cloudbuild_client.projects_builds.Get(
          cloudbuild.CloudbuildProjectsBuildsGetRequest(
              id=build_id, projectId=project_id))

    if response.status != cloudbuild.Build.StatusValueValuesEnum.SUCCESS:
      raise RuntimeError(
          'Failed to build python sdk container image on google cloud build, '
          'please check build log for error.')

    _LOGGER.info(
        "Python SDK container pre-build finished in %.2f seconds" %
        (time.time() - now))
    _LOGGER.info(
        "Python SDK container built and pushed as %s." % container_image_name)

  def _upload_to_gcs(self, local_file_path, gcs_location):
    bucket_name, blob_name = self._get_gcs_bucket_and_name(gcs_location)
    _LOGGER.info('Starting GCS upload to %s...', gcs_location)
    from google.cloud import storage
    from google.cloud.exceptions import Forbidden
    from google.cloud.exceptions import NotFound
    try:
      bucket = self._storage_client.get_bucket(bucket_name)
      blob = bucket.get_blob(blob_name)
      if not blob:
        blob = storage.Blob(name=blob_name, bucket=bucket)
      blob.upload_from_filename(local_file_path)
    except Exception as e:
      if isinstance(e, (Forbidden, NotFound)):
        raise IOError((
            'Could not upload to GCS path %s: %s. Please verify '
            'that credentials are valid and that you have write '
            'access to the specified path.') % (gcs_location, e.message))
      raise
    _LOGGER.info('Completed GCS upload to %s.', gcs_location)

  def _get_cloud_build_id_and_log_url(self, metadata):
    id = None
    log_url = None
    for item in metadata.additionalProperties:
      if item.key == 'build':
        for field in item.value.object_value.properties:
          if field.key == 'logUrl':
            log_url = field.value.string_value
          if field.key == 'id':
            id = field.value.string_value
    return id, log_url

  @staticmethod
  def _get_gcs_bucket_and_name(gcs_location):
    return gcs_location[5:].split('/', 1)

  @staticmethod
  def _make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
      tar.add(source_dir, arcname=SOURCE_FOLDER)

  @staticmethod
  def _get_cloud_build_machine_type_enum(machine_type: str):
    if not machine_type:
      return None
    mappings = {
        'n1-highcpu-8': cloudbuild.BuildOptions.MachineTypeValueValuesEnum.
        N1_HIGHCPU_8,
        'n1-highcpu-32': cloudbuild.BuildOptions.MachineTypeValueValuesEnum.
        N1_HIGHCPU_32,
        'e2-highcpu-8': cloudbuild.BuildOptions.MachineTypeValueValuesEnum.
        E2_HIGHCPU_8,
        'e2-highcpu-32': cloudbuild.BuildOptions.MachineTypeValueValuesEnum.
        E2_HIGHCPU_32
    }
    if machine_type.lower() in mappings:
      return mappings[machine_type.lower()]
    else:
      raise ValueError(
          'Unknown Cloud Build Machine Type option, please specify one of '
          '[n1-highcpu-8, n1-highcpu-32, e2-highcpu-8, e2-highcpu-32].')
