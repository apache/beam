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
"""Implementation of an Artifact{Staging,Retrieval}Service.

The staging service here can be backed by any beam filesystem.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import random
import re

from apache_beam.io import filesystems
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc


class BeamFilesystemArtifactService(
    beam_artifact_api_pb2_grpc.ArtifactStagingServiceServicer,
    beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceServicer):

  _DEFAULT_CHUNK_SIZE = 2 << 20  # 2mb

  def __init__(self, root, chunk_size=_DEFAULT_CHUNK_SIZE):
    self._root = root
    self._chunk_size = chunk_size

  def _sha256(self, string):
    return hashlib.sha256(string.encode('utf-8')).hexdigest()

  def _mkdir(self, retrieval_token):
    dir = filesystems.FileSystems.join(self._root, retrieval_token)
    if not filesystems.FileSystems.exists(dir):
      try:
        filesystems.FileSystems.mkdirs(dir)
      except Exception:
        pass

  def retrieval_token(self, staging_session_token):
    return self._sha256(staging_session_token)

  def _artifact_path(self, retrieval_token, name):
    # These are user-provided, best to check.
    assert re.match('^[0-9a-f]+$', retrieval_token)
    self._mkdir(retrieval_token)
    return filesystems.FileSystems.join(
        self._root, retrieval_token, self._sha256(name))

  def _manifest_path(self, retrieval_token):
    # These are user-provided, best to check.
    assert re.match('^[0-9a-f]+$', retrieval_token)
    self._mkdir(retrieval_token)
    return filesystems.FileSystems.join(
        self._root, retrieval_token, 'manifest.proto')

  def PutArtifact(self, request_iterator, context=None):
    first = True
    for request in request_iterator:
      if first:
        first = False
        metadata = request.metadata.metadata
        retrieval_token = self.retrieval_token(
            request.metadata.staging_session_token)
        self._mkdir(retrieval_token)
        temp_path = filesystems.FileSystems.join(
            self._root,
            retrieval_token,
            '%x.tmp' % random.getrandbits(128))
        fout = filesystems.FileSystems.create(temp_path)
        hasher = hashlib.sha256()
      else:
        hasher.update(request.data.data)
        fout.write(request.data.data)
    fout.close()
    data_hash = hasher.hexdigest()
    if metadata.sha256 and metadata.sha256 != data_hash:
      filesystems.FileSystems.delete([temp_path])
      raise ValueError('Bad metadata hash: %s vs %s' % (
          metadata.metadata.sha256, data_hash))
    filesystems.FileSystems.rename(
        [temp_path], [self._artifact_path(retrieval_token, metadata.name)])
    return beam_artifact_api_pb2.PutArtifactResponse()

  def CommitManifest(self, request, context=None):
    retrieval_token = self.retrieval_token(request.staging_session_token)
    with filesystems.FileSystems.create(
        self._manifest_path(retrieval_token)) as fout:
      fout.write(request.manifest.SerializeToString())
    return beam_artifact_api_pb2.CommitManifestResponse(
        retrieval_token=retrieval_token)

  def GetManifest(self, request, context=None):
    with filesystems.FileSystems.open(
        self._manifest_path(request.retrieval_token)) as fin:
      return beam_artifact_api_pb2.GetManifestResponse(
          manifest=beam_artifact_api_pb2.Manifest.FromString(
              fin.read()))

  def GetArtifact(self, request, context=None):
    with filesystems.FileSystems.open(
        self._artifact_path(request.retrieval_token, request.name)) as fin:
      # This value is not emitted, but lets us yield a single empty
      # chunk on an empty file.
      chunk = True
      while chunk:
        chunk = fin.read(self._chunk_size)
        yield beam_artifact_api_pb2.ArtifactChunk(data=chunk)
