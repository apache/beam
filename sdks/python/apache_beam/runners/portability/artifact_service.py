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
import threading
import zipfile

from apache_beam.io import filesystems
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc


class AbstractArtifactService(
    beam_artifact_api_pb2_grpc.ArtifactStagingServiceServicer,
    beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceServicer):

  _DEFAULT_CHUNK_SIZE = 2 << 20  # 2mb

  def __init__(self, root, chunk_size=None):
    self._root = root
    self._chunk_size = chunk_size or _DEFAULT_CHUNK_SIZE

  def _sha256(self, string):
    return hashlib.sha256(string.encode('utf-8')).hexdigest()

  def retrieval_token(self, staging_session_token):
    return self._sha256(staging_session_token)

  def _mkdir(self, retrieval_token):
    raise NotImplementedError(type(self))

  def _join(self, *args):
    raise NotImplementedError(type(self))

  def _temp_path(self, path):
    return path + '.tmp'

  def _rename(self, src, dest):
    raise NotImplementedError(type(self))

  def _delete(self, path):
    raise NotImplementedError(type(self))

  def _artifact_path(self, retrieval_token, name):
    # These are user-provided, best to check.
    assert re.match('^[0-9a-f]+$', retrieval_token)
    self._mkdir(retrieval_token)
    return self._join(self._root, retrieval_token, self._sha256(name))

  def _manifest_path(self, retrieval_token):
    # These are user-provided, best to check.
    assert re.match('^[0-9a-f]+$', retrieval_token)
    self._mkdir(retrieval_token)
    return self._join(self._root, retrieval_token, 'manifest.proto')

  def _open(self, path, mode):
    raise NotImplementedError(type(self))

  def PutArtifact(self, request_iterator, context=None):
    first = True
    for request in request_iterator:
      if first:
        first = False
        metadata = request.metadata.metadata
        retrieval_token = self.retrieval_token(
            request.metadata.staging_session_token)
        artifact_path = self._artifact_path(retrieval_token, metadata.name)
        temp_path = self._temp_path(artifact_path)
        self._mkdir(retrieval_token)
        fout = self._open(temp_path, 'w')
        hasher = hashlib.sha256()
      else:
        hasher.update(request.data.data)
        fout.write(request.data.data)
    fout.close()
    data_hash = hasher.hexdigest()
    if metadata.sha256 and metadata.sha256 != data_hash:
      self._delete(temp_path)
      raise ValueError('Bad metadata hash: %s vs %s' % (
          metadata.sha256, data_hash))
    self._rename(temp_path, artifact_path)
    return beam_artifact_api_pb2.PutArtifactResponse()

  def CommitManifest(self, request, context=None):
    retrieval_token = self.retrieval_token(request.staging_session_token)
    with self._open(self._manifest_path(retrieval_token), 'w') as fout:
      fout.write(request.manifest.SerializeToString())
    return beam_artifact_api_pb2.CommitManifestResponse(
        retrieval_token=retrieval_token)

  def GetManifest(self, request, context=None):
    with self._open(self._manifest_path(request.retrieval_token), 'r') as fin:
      return beam_artifact_api_pb2.GetManifestResponse(
          manifest=beam_artifact_api_pb2.Manifest.FromString(
              fin.read()))

  def GetArtifact(self, request, context=None):
    with self._open(
        self._artifact_path(request.retrieval_token, request.name), 'r') as fin:
      # This value is not emitted, but lets us yield a single empty
      # chunk on an empty file.
      chunk = True
      while chunk:
        chunk = fin.read(self._chunk_size)
        yield beam_artifact_api_pb2.ArtifactChunk(data=chunk)


class ZipFileArtifactService(AbstractArtifactService):

  def __init__(self, path, chunk_size=None):
    super(ZipFileArtifactService, self).__init__('', chunk_size)
    self._zipfile = zipfile.ZipFile(path, 'a')
    self._lock = threading.Lock()

  def _mkdir(self, retrieval_token):
    pass

  def _join(self, *args):
    return '/'.join(args)

  def _temp_path(self, path):
    return path  # ZipFile offers no move operation.

  def _rename(self, src, dest):
    assert src == dest

  def _delete(self, path):
    # ZipFile offers no delete operation: b/...
    if self._exists(path):
      while self._exists(path + '.deleted.deleted'):
        path += path + '.deleted.deleted'
      with self._open(path + '.deleted', 'w'):
        pass

  def _exists(self, path):
    return (path in self._zipfile.namelist()
        and not self._exists(path + '.deleted'))

  def _open(self, path, mode):
    if path + '.deleted' in self._zipfile.namelist():
      if 'r' in mode:
        raise ValueError('Deleted file.')
      else:
        self._delete(path + '.deleted')
    return self._zipfile.open(path, mode, force_zip64=True)

  def close(self):
    self._zipflie.close()

  def PutArtifact(self, request_iterator, context=None):
    # ZipFile only supports one writable channel at a time.
    with self._lock:
      return super(
          ZipFileArtifactService, self).PutArtifact(request_iterator, context)

  def CommitManifest(self, request, context=None):
    # ZipFile only supports one writable channel at a time.
    with self._lock:
      return super(
          ZipFileArtifactService, self).CommitManifest(request, context)


class BeamFilesystemArtifactService(AbstractArtifactService):

  def _mkdir(self, retrieval_token):
    dir = filesystems.FileSystems.join(self._root, retrieval_token)
    if not filesystems.FileSystems.exists(dir):
      try:
        filesystems.FileSystems.mkdirs(dir)
      except Exception:
        pass

  def _join(self, *args):
    return filesystems.FileSystems.join(*args)

  def _rename(self, src, dest):
    filesystems.FileSystems.rename([src], [dest])

  def _delete(self, path):
    filesystems.FileSystems.delete([path])

  def _open(self, path, mode='r'):
    if 'w' in mode:
      return filesystems.FileSystems.create(path)
    else:
      return filesystems.FileSystems.open(path)
