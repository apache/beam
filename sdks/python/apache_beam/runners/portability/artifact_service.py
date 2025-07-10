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

import concurrent.futures
import hashlib
import os
import queue
import sys
import tempfile
import threading
from io import BytesIO
from typing import Any
from typing import BinaryIO  # pylint: disable=unused-import
from typing import Callable
from typing import Dict
from typing import List
from typing import MutableMapping
from typing import Optional
from typing import Tuple
from urllib.request import urlopen

import grpc

from apache_beam.io import filesystems
from apache_beam.io.filesystems import CompressionTypes
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.utils import proto_utils


class ArtifactRetrievalService(
    beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceServicer):

  _DEFAULT_CHUNK_SIZE = 2 << 20

  def __init__(
      self,
      file_reader: Callable[[str], BinaryIO],
      chunk_size=None,
  ):
    self._file_reader = file_reader
    self._chunk_size = chunk_size or self._DEFAULT_CHUNK_SIZE

  def ResolveArtifacts(self, request, context=None):
    return beam_artifact_api_pb2.ResolveArtifactsResponse(
        replacements=request.artifacts)

  def GetArtifact(self, request, context=None):
    if request.artifact.type_urn == common_urns.artifact_types.FILE.urn:
      payload = proto_utils.parse_Bytes(
          request.artifact.type_payload,
          beam_runner_api_pb2.ArtifactFilePayload)
      read_handle = self._file_reader(payload.path)
    elif request.artifact.type_urn == common_urns.artifact_types.URL.urn:
      payload = proto_utils.parse_Bytes(
          request.artifact.type_payload, beam_runner_api_pb2.ArtifactUrlPayload)
      read_handle = urlopen(payload.url)
    elif request.artifact.type_urn == common_urns.artifact_types.EMBEDDED.urn:
      payload = proto_utils.parse_Bytes(
          request.artifact.type_payload,
          beam_runner_api_pb2.EmbeddedFilePayload)
      read_handle = BytesIO(payload.data)
    else:
      raise NotImplementedError(request.artifact.type_urn)

    with read_handle as fin:
      while True:
        chunk = fin.read(self._chunk_size)
        if not chunk:
          break
        yield beam_artifact_api_pb2.GetArtifactResponse(data=chunk)


class ArtifactStagingService(
    beam_artifact_api_pb2_grpc.ArtifactStagingServiceServicer):
  def __init__(
      self,
      file_writer: Callable[[str, Optional[str]], Tuple[BinaryIO, str]],
  ):
    self._lock = threading.Lock()
    self._jobs_to_stage: Dict[
        str,
        Tuple[Dict[Any, List[beam_runner_api_pb2.ArtifactInformation]],
              threading.Event]] = {}
    self._file_writer = file_writer

  def register_job(
      self,
      staging_token: str,
      dependency_sets: MutableMapping[
          Any, List[beam_runner_api_pb2.ArtifactInformation]]):
    if staging_token in self._jobs_to_stage:
      raise ValueError('Already staging %s' % staging_token)
    with self._lock:
      self._jobs_to_stage[staging_token] = (
          dict(dependency_sets), threading.Event())

  def resolved_deps(self, staging_token, timeout=None):
    with self._lock:
      dependency_sets, event = self._jobs_to_stage[staging_token]
    try:
      if not event.wait(timeout):
        raise concurrent.futures.TimeoutError()
      return dependency_sets
    finally:
      with self._lock:
        del self._jobs_to_stage[staging_token]

  def ReverseArtifactRetrievalService(self, responses, context=None):
    staging_token = next(responses).staging_token
    with self._lock:
      try:
        dependency_sets, event = self._jobs_to_stage[staging_token]
      except KeyError:
        if context:
          context.set_code(grpc.StatusCode.NOT_FOUND)
          context.set_details('No such staging token: %r' % staging_token)
        raise

    requests = _QueueIter()

    class ForwardingRetrievalService(object):
      def ResolveArtifactss(self, request):
        requests.put(
            beam_artifact_api_pb2.ArtifactRequestWrapper(
                resolve_artifact=request))
        return next(responses).resolve_artifact_response

      def GetArtifact(self, request):
        requests.put(
            beam_artifact_api_pb2.ArtifactRequestWrapper(get_artifact=request))
        while True:
          response = next(responses)
          yield response.get_artifact_response
          if response.is_last:
            break

    def resolve():
      try:
        for key, dependencies in dependency_sets.items():
          dependency_sets[key] = list(
              resolve_as_files(
                  ForwardingRetrievalService(), lambda name: self._file_writer(
                      os.path.join(staging_token, name)),
                  dependencies))
        requests.done()
      except:  # pylint: disable=bare-except
        requests.abort()
        raise
      finally:
        event.set()

    t = threading.Thread(target=resolve)
    t.daemon = True
    t.start()

    return requests


def resolve_as_files(retrieval_service, file_writer, dependencies):
  """Translates a set of dependencies into file-based dependencies."""
  # Resolve until nothing changes.  This ensures that they can be fetched.
  resolution = retrieval_service.ResolveArtifactss(
      beam_artifact_api_pb2.ResolveArtifactsRequest(
          artifacts=dependencies,
          # Anything fetchable will do.
          # TODO(robertwb): Take advantage of shared filesystems, urls.
          preferred_urns=[],
      ))
  dependencies = resolution.replacements

  # Fetch each of the dependencies, using file_writer to store them as
  # file-based artifacts.
  # TODO(robertwb): Consider parallelizing the actual writes.
  for dep in dependencies:
    if dep.role_urn == common_urns.artifact_roles.STAGING_TO.urn:
      base_name = os.path.basename(
          proto_utils.parse_Bytes(
              dep.role_payload,
              beam_runner_api_pb2.ArtifactStagingToRolePayload).staged_name)
    else:
      base_name = None
    unique_name = '-'.join(
        filter(
            None,
            [hashlib.sha256(dep.SerializeToString()).hexdigest(), base_name]))
    file_handle, path = file_writer(unique_name)
    with file_handle as fout:
      for chunk in retrieval_service.GetArtifact(
          beam_artifact_api_pb2.GetArtifactRequest(artifact=dep)):
        fout.write(chunk.data)
    yield beam_runner_api_pb2.ArtifactInformation(
        type_urn=common_urns.artifact_types.FILE.urn,
        type_payload=beam_runner_api_pb2.ArtifactFilePayload(
            path=path).SerializeToString(),
        role_urn=dep.role_urn,
        role_payload=dep.role_payload)


def offer_artifacts(
    artifact_staging_service, artifact_retrieval_service, staging_token):
  """Offers a set of artifacts to an artifact staging service, via the
  ReverseArtifactRetrievalService API.

  The given artifact_retrieval_service should be able to resolve/get all
  artifacts relevant to this job.
  """
  responses = _QueueIter()
  responses.put(
      beam_artifact_api_pb2.ArtifactResponseWrapper(
          staging_token=staging_token))
  requests = artifact_staging_service.ReverseArtifactRetrievalService(responses)
  try:
    for request in requests:
      if request.HasField('resolve_artifact'):
        responses.put(
            beam_artifact_api_pb2.ArtifactResponseWrapper(
                resolve_artifact_response=artifact_retrieval_service.
                ResolveArtifacts(request.resolve_artifact)))
      elif request.HasField('get_artifact'):
        for chunk in artifact_retrieval_service.GetArtifact(
            request.get_artifact):
          responses.put(
              beam_artifact_api_pb2.ArtifactResponseWrapper(
                  get_artifact_response=chunk))
        responses.put(
            beam_artifact_api_pb2.ArtifactResponseWrapper(
                get_artifact_response=beam_artifact_api_pb2.GetArtifactResponse(
                    data=b''),
                is_last=True))
    responses.done()
  except:  # pylint: disable=bare-except
    responses.abort()
    raise


class BeamFilesystemHandler(object):
  def __init__(self, root):
    self._root = root

  def file_reader(self, path):
    return filesystems.FileSystems.open(
        path, compression_type=CompressionTypes.UNCOMPRESSED)

  def file_writer(self, name=None):
    full_path = filesystems.FileSystems.join(self._root, name)
    return filesystems.FileSystems.create(
        full_path, compression_type=CompressionTypes.UNCOMPRESSED), full_path


def resolve_artifacts(artifacts, service, dest_dir):
  if not artifacts:
    return artifacts
  else:
    return [
        maybe_store_artifact(artifact, service,
                             dest_dir) for artifact in service.ResolveArtifacts(
                                 beam_artifact_api_pb2.ResolveArtifactsRequest(
                                     artifacts=artifacts)).replacements
    ]


def maybe_store_artifact(artifact, service, dest_dir):
  if artifact.type_urn in (common_urns.artifact_types.URL.urn,
                           common_urns.artifact_types.EMBEDDED.urn):
    return artifact
  elif artifact.type_urn == common_urns.artifact_types.FILE.urn:
    payload = beam_runner_api_pb2.ArtifactFilePayload.FromString(
        artifact.type_payload)
    # pylint: disable=condition-evals-to-constant
    if os.path.exists(
        payload.path) and payload.sha256 and payload.sha256 == sha256(
            payload.path) and False:
      return artifact
    else:
      return store_artifact(artifact, service, dest_dir)
  else:
    return store_artifact(artifact, service, dest_dir)


def store_artifact(artifact, service, dest_dir):
  hasher = hashlib.sha256()
  with tempfile.NamedTemporaryFile(dir=dest_dir, delete=False) as fout:
    for block in service.GetArtifact(
        beam_artifact_api_pb2.GetArtifactRequest(artifact=artifact)):
      hasher.update(block.data)
      fout.write(block.data)
  return beam_runner_api_pb2.ArtifactInformation(
      type_urn=common_urns.artifact_types.FILE.urn,
      type_payload=beam_runner_api_pb2.ArtifactFilePayload(
          path=fout.name, sha256=hasher.hexdigest()).SerializeToString(),
      role_urn=artifact.role_urn,
      role_payload=artifact.role_payload)


def sha256(path):
  hasher = hashlib.sha256()
  with open(path, 'rb') as fin:
    for block in iter(lambda: fin.read(4 << 20), b''):
      hasher.update(block)
  return hasher.hexdigest()


class _QueueIter(object):

  _END = object()

  def __init__(self):
    self._queue = queue.Queue()

  def put(self, item):
    self._queue.put(item)

  def done(self):
    self._queue.put(self._END)
    self._queue.put(StopIteration)

  def abort(self, exn=None):
    if exn is None:
      exn = sys.exc_info()[1]
    self._queue.put(self._END)
    self._queue.put(exn)

  def __iter__(self):
    return self

  def __next__(self):
    item = self._queue.get()
    if item is self._END:
      raise self._queue.get()
    else:
      return item
