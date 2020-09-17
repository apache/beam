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

"""SdkContainerCloudBuilder leverages google cloud build for building sdk
container image with dependencies.
"""

from __future__ import absolute_import

import logging
import os
import tarfile
import time

from google.cloud.devtools.cloudbuild_v1 import Build
from google.cloud.devtools.cloudbuild_v1 import BuildStep
from google.cloud.devtools.cloudbuild_v1 import CloudBuildClient
from google.cloud.devtools.cloudbuild_v1 import Source
from google.cloud.devtools.cloudbuild_v1 import StorageSource
from google.protobuf.duration_pb2 import Duration

from apache_beam.internal.gcp.auth import get_service_credentials
from apache_beam.internal.http_client import get_new_http
from apache_beam.io.gcp.internal.clients import storage
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.portability.sdk_container_builder import SdkContainerBuilder
from apitools.base.py import exceptions

_LOGGER = logging.getLogger(__name__)

SOURCE_FOLDER = 'source'


class SdkContainerCloudBuilder(SdkContainerBuilder):
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
    if not self._docker_registry:
      self._docker_registry = 'gcr.io/%s' % self._google_cloud_options.project

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

    client = CloudBuildClient()
    build = Build()
    build.steps = []
    step = BuildStep()
    step.name = 'gcr.io/cloud-builders/docker'
    step.args = ['build', '-t', container_tag, '.']
    step.dir = SOURCE_FOLDER

    build.steps.append(step)
    build.images = [container_tag]

    source = Source()
    source.storage_source = StorageSource()
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
