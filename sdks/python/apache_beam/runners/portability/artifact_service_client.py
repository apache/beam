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
"""A :class:`FileHandler` to work with :class:`ArtifactStagingServiceStub`.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.runners.portability.stager import FileHandler


class ArtifactStagingFileHandler(FileHandler):
  """:class:`FileHandler` to push files to ArtifactStagingService.

  The class keeps track of pushed files and commit manifest once all files are
  uploaded.

  Note: This class is not thread safe and user of this class should ensure
  thread safety.
  """

  def __init__(self, artifact_service_channel):
    """Creates a new FileHandler to upload file to ArtifactStagingService.

    Args:
      artifact_service_channel: Channel used to interact with
        ArtifactStagingService.User owns the channel and should close it when
        finished.
    """
    super(ArtifactStagingFileHandler, self).__init__()
    self._artifact_staging_stub = beam_artifact_api_pb2_grpc.\
        ArtifactStagingServiceStub(channel=artifact_service_channel)
    self._artifacts = []
    self._closed = False

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if not exc_type:
      self._check_closed()
      self._closed = True
      manifest = beam_artifact_api_pb2.Manifest(artifact=self._artifacts)
      self._artifact_staging_stub.CommitManifest(
          beam_artifact_api_pb2.CommitManifestRequest(manifest=manifest))

  def file_copy(self, from_path, to_path):
    """Uploads a file to ArtifactStagingService.

    Note: Downloading/copying file from remote server is not supported.
    Args:
      from_path: Path of file to be uploaded.
      to_path: File name on the artifact server.
    """
    self._check_closed()
    if not os.path.isfile(from_path):
      raise ValueError(
          'Can only copy local file to artifact server. from_path: {0} '
          'to_path: {1}'.format(from_path, to_path))

    def artifact_request_generator():
      metadata = beam_artifact_api_pb2.ArtifactMetadata(name=to_path)
      request = beam_artifact_api_pb2.PutArtifactRequest(metadata=metadata)
      yield request
      with open(from_path, 'rb') as f:
        while True:
          chunk = f.read(2 << 20)  # 2MB
          if not chunk:
            break
          request = beam_artifact_api_pb2.PutArtifactRequest(
              data=beam_artifact_api_pb2.ArtifactChunk(data=chunk))
          yield request
      self._artifacts.append(metadata)

    self._artifact_staging_stub.PutArtifact(artifact_request_generator())

  def file_download(self, from_url, to_path):
    self._check_closed()
    return super(ArtifactStagingFileHandler, self).file_download(
        from_url, to_path)

  def _check_closed(self):
    if self._closed:
      raise ValueError('This file handler is commited and can not be used.')
