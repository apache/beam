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

"""SdkContainerBuilder builds the portable SDK container with dependencies.

It copies the right boot dependencies, namely: apache beam sdk, python packages
from requirements.txt, python packages from extra_packages.txt, workflow
tarball, into the latest public python sdk container image, and run the
dependencies installation in advance with the boot program in setup only mode
to build the new image.
"""

from __future__ import absolute_import

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

from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToJson

from apache_beam.internal.gcp.auth import get_service_credentials
from apache_beam.internal.http_client import get_new_http
from apache_beam.io.gcp.internal.clients import storage
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions  # pylint: disable=unused-import
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability.stager import Stager

ARTIFACTS_CONTAINER_DIR = '/opt/apache/beam/artifacts'
ARTIFACTS_MANIFEST_FILE = 'artifacts_info.json'
SDK_CONTAINER_ENTRYPOINT = '/opt/apache/beam/boot'
DOCKERFILE_TEMPLATE = (
    """FROM apache/beam_python{major}.{minor}_sdk:latest
RUN mkdir -p {workdir}
COPY ./* {workdir}/
RUN {entrypoint} --setup_only --artifacts {workdir}/{manifest_file}
""")

SOURCE_FOLDER = 'source'
_LOGGER = logging.getLogger(__name__)


class SdkContainerBuilder(object):
  def __init__(self, options):
    self._options = options
    self._temp_src_dir = tempfile.mkdtemp()
    self._docker_registry_push_url = self._options.view_as(
        DebugOptions).lookup_experiment('docker_registry_push_url')

  def build(self):
    container_id = str(uuid.uuid4())
    container_tag = os.path.join(
        self._docker_registry_push_url or '',
        'beam_python_prebuilt_sdk:%s' % container_id)
    self.prepare_dependencies()
    self.invoke_docker_build_and_push(container_id, container_tag)

    return container_tag

  def prepare_dependencies(self):
    tmp = tempfile.mkdtemp()
    resources = Stager.create_job_resources(self._options, tmp)
    # make a copy of the staged artifacts into the temp source folder.
    for path, name in resources:
      shutil.copyfile(path, os.path.join(self._temp_src_dir, name))
    with open(os.path.join(self._temp_src_dir, 'Dockerfile'), 'w') as file:
      file.write(
          DOCKERFILE_TEMPLATE.format(
              major=sys.version_info[0],
              minor=sys.version_info[1],
              workdir=ARTIFACTS_CONTAINER_DIR,
              manifest_file=ARTIFACTS_MANIFEST_FILE,
              entrypoint=SDK_CONTAINER_ENTRYPOINT))
    self.generate_artifacts_manifests_json_file(resources, self._temp_src_dir)

  def invoke_docker_build_and_push(self, container_id, container_tag):
    raise NotImplementedError

  @staticmethod
  def generate_artifacts_manifests_json_file(resources, temp_dir):
    infos = []
    for _, name in resources:
      info = beam_runner_api_pb2.ArtifactInformation(
          type_urn=common_urns.StandardArtifacts.Types.FILE.urn,
          type_payload=beam_runner_api_pb2.ArtifactFilePayload(
              path=name).SerializeToString(),
      )
      infos.append(json.dumps(MessageToJson(info)))
    with open(os.path.join(temp_dir, ARTIFACTS_MANIFEST_FILE), 'w') as file:
      file.write('[\n' + ',\n'.join(infos) + '\n]')

  @classmethod
  def build_container_imge(cls, pipeline_options):
    # type: (PipelineOptions) -> str
    debug_options = pipeline_options.view_as(DebugOptions)
    container_build_engine = debug_options.lookup_experiment(
        'prebuild_sdk_container')
    if container_build_engine:
      if container_build_engine == 'local_docker':
        builder = _SdkContainerLocalBuilder(
            pipeline_options)  # type: SdkContainerBuilder
      elif container_build_engine == 'cloud_build':
        builder = _SdkContainerCloudBuilder(pipeline_options)
      else:
        raise ValueError(
            'Only prebuild_sdk_container=local_docker and '
            'prebuild_sdk_container=cloud_build is supported')
    else:
      raise ValueError('No prebuild_sdk_container flag specified.')
    return builder.build()


class _SdkContainerLocalBuilder(SdkContainerBuilder):
  """SdkContainerLocalBuilder builds the sdk container image with local
  docker."""
  def invoke_docker_build_and_push(self, container_id, container_tag):
    try:
      subprocess.run(['docker', 'build', '.', '-t', container_tag],
                     capture_output=True,
                     check=True,
                     cwd=self._temp_src_dir)
      now = time.time()
      _LOGGER.info("Building sdk container, this may take a few minutes...")
    except subprocess.CalledProcessError as err:
      raise RuntimeError(
          'Failed to build sdk container with local docker, '
          'stderr:\n %s.' % err.stderr)
    else:
      _LOGGER.info(
          "Successfully built %s in %.2f seconds" %
          (container_tag, time.time() - now))

    if self._docker_registry_push_url:
      _LOGGER.info("Pushing prebuilt sdk container...")
      try:
        subprocess.run(['docker', 'push', container_tag],
                       capture_output=True,
                       check=True)
      except subprocess.CalledProcessError as err:
        raise RuntimeError(
            'Failed to push prebuilt sdk container %s, stderr: \n%s' %
            (container_tag, err.stderr))
      _LOGGER.info(
          "Successfully pushed %s in %.2f seconds" %
          (container_tag, time.time() - now))
    else:
      _LOGGER.info(
          "no --docker_registry_push_url option is specified in pipeline "
          "options, specify it if the new image is intended to be "
          "pushed to a registry.")


class _SdkContainerCloudBuilder(SdkContainerBuilder):
  """SdkContainerLocalBuilder builds the sdk container image with google cloud
  build."""
  def __init__(self, options):
    super().__init__(options)
    self._google_cloud_options = options.view_as(GoogleCloudOptions)
    if self._google_cloud_options.no_auth:
      credentials = None
    else:
      credentials = get_service_credentials()
    self._storage_client = storage.StorageV1(
        url='https://www.googleapis.com/storage/v1',
        credentials=credentials,
        get_credentials=(not self._google_cloud_options.no_auth),
        http=get_new_http(),
        response_encoding='utf8')
    if not self._docker_registry_push_url:
      self._docker_registry_push_url = (
          'gcr.io/%s' % self._google_cloud_options.project)

  def invoke_docker_build_and_push(self, container_id, container_tag):
    project_id = self._google_cloud_options.project
    temp_location = self._google_cloud_options.temp_location
    # google cloud build service expects all the build source file to be
    # compressed into a tarball.
    tarball_path = os.path.join(self._temp_src_dir, '%s.tgz' % SOURCE_FOLDER)
    self._make_tarfile(tarball_path, self._temp_src_dir)
    _LOGGER.info(
        "Compressed source files for building sdk container at %s" %
        tarball_path)

    gcs_location = os.path.join(
        temp_location, '%s-%s.tgz' % (SOURCE_FOLDER, container_id))
    self._upload_to_gcs(tarball_path, gcs_location)

    from google.cloud.devtools import cloudbuild_v1
    client = cloudbuild_v1.CloudBuildClient()
    build = cloudbuild_v1.Build()
    build.steps = []
    step = cloudbuild_v1.BuildStep()
    step.name = 'gcr.io/cloud-builders/docker'
    step.args = ['build', '-t', container_tag, '.']
    step.dir = SOURCE_FOLDER

    build.steps.append(step)
    build.images = [container_tag]

    source = cloudbuild_v1.Source()
    source.storage_source = cloudbuild_v1.StorageSource()
    gcs_bucket, gcs_object = self._get_gcs_bucket_and_name(gcs_location)
    source.storage_source.bucket = os.path.join(gcs_bucket)
    source.storage_source.object = gcs_object
    build.source = source
    # TODO(zyichi): make timeout configurable
    build.timeout = Duration().FromSeconds(seconds=1800)

    now = time.time()
    _LOGGER.info('Building sdk container, this may take a few minutes...')
    operation = client.create_build(project_id=project_id, build=build)
    # if build fails exception will be raised and stops the job submission.
    result = operation.result()
    _LOGGER.info(
        "Python SDK container pre-build finished in %.2f seconds, "
        "check build log at %s" % (time.time() - now, result.log_url))
    _LOGGER.info("Python SDK container built and pushed as %s." % container_tag)

  def _upload_to_gcs(self, local_file_path, gcs_location):
    gcs_bucket, gcs_object = self._get_gcs_bucket_and_name(gcs_location)
    request = storage.StorageObjectsInsertRequest(
        bucket=gcs_bucket, name=gcs_object)
    _LOGGER.info('Starting GCS upload to %s...', gcs_location)
    total_size = os.path.getsize(local_file_path)
    from apitools.base.py import exceptions
    try:
      with open(local_file_path, 'rb') as stream:
        upload = storage.Upload(stream, 'application/octet-stream', total_size)
        self._storage_client.objects.Insert(request, upload=upload)
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
                      (gcs_location, reportable_errors[e.status_code]))
      raise
    _LOGGER.info('Completed GCS upload to %s.', gcs_location)

  @staticmethod
  def _get_gcs_bucket_and_name(gcs_location):
    return gcs_location[5:].split('/', 1)

  @staticmethod
  def _make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
      tar.add(source_dir, arcname=SOURCE_FOLDER)
