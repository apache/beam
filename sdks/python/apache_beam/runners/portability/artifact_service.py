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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import sys
import threading
import zipfile
from typing import Iterator

from google.protobuf import json_format

from apache_beam.io import filesystems
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc


class AbstractArtifactService(
    beam_artifact_api_pb2_grpc.ArtifactStagingServiceServicer,
    beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceServicer):

  _DEFAULT_CHUNK_SIZE = 2 << 20  # 2mb

  def __init__(self, root, chunk_size=None):
    self._root = root
    self._chunk_size = chunk_size or self._DEFAULT_CHUNK_SIZE

  def _sha256(self, string):
    return hashlib.sha256(string.encode('utf-8')).hexdigest()

  def _join(self, *args):
    # type: (*str) -> str
    raise NotImplementedError(type(self))

  def _dirname(self, path):
    # type: (str) -> str
    raise NotImplementedError(type(self))

  def _temp_path(self, path):
    # type: (str) -> str
    return path + '.tmp'

  def _open(self, path, mode):
    raise NotImplementedError(type(self))

  def _rename(self, src, dest):
    # type: (str, str) -> None
    raise NotImplementedError(type(self))

  def _delete(self, path):
    # type: (str) -> None
    raise NotImplementedError(type(self))

  def _artifact_path(self, retrieval_token, name):
    # type: (str, str) -> str
    return self._join(self._dirname(retrieval_token), self._sha256(name))

  def _manifest_path(self, retrieval_token):
    # type: (str) -> str
    return retrieval_token

  def _get_manifest_proxy(self, retrieval_token):
    # type: (str) -> beam_artifact_api_pb2.ProxyManifest
    with self._open(self._manifest_path(retrieval_token), 'r') as fin:
      return json_format.Parse(
          fin.read().decode('utf-8'), beam_artifact_api_pb2.ProxyManifest())

  def retrieval_token(self, staging_session_token):
    # type: (str) -> str
    return self._join(
        self._root, self._sha256(staging_session_token), 'MANIFEST')

  def PutArtifact(self, request_iterator, context=None):
    # type: (...) -> beam_artifact_api_pb2.PutArtifactResponse
    first = True
    for request in request_iterator:
      if first:
        first = False
        metadata = request.metadata.metadata
        retrieval_token = self.retrieval_token(
            request.metadata.staging_session_token)
        artifact_path = self._artifact_path(retrieval_token, metadata.name)
        temp_path = self._temp_path(artifact_path)
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

  def CommitManifest(self,
                     request,  # type: beam_artifact_api_pb2.CommitManifestRequest
                     context=None):
    # type: (...) -> beam_artifact_api_pb2.CommitManifestResponse
    retrieval_token = self.retrieval_token(request.staging_session_token)
    proxy_manifest = beam_artifact_api_pb2.ProxyManifest(
        manifest=request.manifest,
        location=[
            beam_artifact_api_pb2.ProxyManifest.Location(
                name=metadata.name,
                uri=self._artifact_path(retrieval_token, metadata.name))
            for metadata in request.manifest.artifact])
    with self._open(self._manifest_path(retrieval_token), 'w') as fout:
      fout.write(json_format.MessageToJson(proxy_manifest).encode('utf-8'))
    return beam_artifact_api_pb2.CommitManifestResponse(
        retrieval_token=retrieval_token)

  def GetManifest(self,
                  request,  # type: beam_artifact_api_pb2.GetManifestRequest
                  context=None):
    # type: (...) -> beam_artifact_api_pb2.GetManifestResponse
    return beam_artifact_api_pb2.GetManifestResponse(
        manifest=self._get_manifest_proxy(request.retrieval_token).manifest)

  def GetArtifact(self,
                  request,  # type: beam_artifact_api_pb2.GetArtifactRequest
                  context=None):
    # type: (...) -> Iterator[beam_artifact_api_pb2.ArtifactChunk]
    for artifact in self._get_manifest_proxy(request.retrieval_token).location:
      if artifact.name == request.name:
        with self._open(artifact.uri, 'r') as fin:
          # This value is not emitted, but lets us yield a single empty
          # chunk on an empty file.
          chunk = b'1'
          while chunk:
            chunk = fin.read(self._chunk_size)
            yield beam_artifact_api_pb2.ArtifactChunk(data=chunk)
        break
    else:
      raise ValueError('Unknown artifact: %s' % request.name)


class ZipFileArtifactService(AbstractArtifactService):
  """Stores artifacts in a zip file.

  This is particularly useful for storing artifacts as part of an UberJar for
  submitting to an upstream runner's cluster.

  Writing to zip files requires Python 3.6+.
  """

  def __init__(self, path, internal_root, chunk_size=None):
    if sys.version_info < (3, 6):
      raise RuntimeError(
          'Writing to zip files requires Python 3.6+, '
          'but current version is %s' % sys.version)
    super(ZipFileArtifactService, self).__init__(internal_root, chunk_size)
    self._zipfile = zipfile.ZipFile(path, 'a')
    self._lock = threading.Lock()

  def _join(self, *args):
    # type: (*str) -> str
    return '/'.join(args)

  def _dirname(self, path):
    # type: (str) -> str
    return path.rsplit('/', 1)[0]

  def _temp_path(self, path):
    # type: (str) -> str
    return path  # ZipFile offers no move operation.

  def _rename(self, src, dest):
    # type: (str, str) -> None
    assert src == dest

  def _delete(self, path):
    # type: (str) -> None
    # ZipFile offers no delete operation: https://bugs.python.org/issue6818
    pass

  def _open(self, path, mode):
    if path.startswith('/'):
      raise ValueError(
          'ZIP file entry %s invalid: '
          'path must not contain a leading slash.' % path)
    return self._zipfile.open(path, mode, force_zip64=True)

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

  def GetManifest(self, request, context=None):
    # ZipFile appears to not be threadsafe on some platforms.
    with self._lock:
      return super(ZipFileArtifactService, self).GetManifest(request, context)

  def GetArtifact(self, request, context=None):
    # ZipFile appears to not be threadsafe on some platforms.
    with self._lock:
      for chunk in super(ZipFileArtifactService, self).GetArtifact(
          request, context):
        yield chunk

  def close(self):
    self._zipfile.close()


class BeamFilesystemArtifactService(AbstractArtifactService):

  def _join(self, *args):
    # type: (*str) -> str
    return filesystems.FileSystems.join(*args)

  def _dirname(self, path):
    # type: (str) -> str
    return filesystems.FileSystems.split(path)[0]

  def _rename(self, src, dest):
    # type: (str, str) -> None
    filesystems.FileSystems.rename([src], [dest])

  def _delete(self, path):
    # type: (str) -> None
    filesystems.FileSystems.delete([path])

  def _open(self, path, mode='r'):
    dir = self._dirname(path)
    if not filesystems.FileSystems.exists(dir):
      try:
        filesystems.FileSystems.mkdirs(dir)
      except Exception:
        pass

    if 'w' in mode:
      return filesystems.FileSystems.create(path)
    else:
      return filesystems.FileSystems.open(path)
