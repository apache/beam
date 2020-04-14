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

"""Test cases for :module:`artifact_service_client`."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import contextlib
import hashlib
import io
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import unittest

import grpc
from future.moves.urllib.parse import quote

from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import artifact_service
from apache_beam.utils import proto_utils
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor


class AbstractArtifactServiceTest(unittest.TestCase):
  def setUp(self):
    self._staging_dir = tempfile.mkdtemp()
    self._service = self.create_service(self._staging_dir)

  def tearDown(self):
    if self._staging_dir:
      shutil.rmtree(self._staging_dir)

  def create_service(self, staging_dir):
    raise NotImplementedError(type(self))

  @staticmethod
  def put_metadata(staging_token, name, sha256=None):
    return beam_artifact_api_pb2.PutArtifactRequest(
        metadata=beam_artifact_api_pb2.PutArtifactMetadata(
            staging_session_token=staging_token,
            metadata=beam_artifact_api_pb2.ArtifactMetadata(
                name=name, sha256=sha256)))

  @staticmethod
  def put_data(chunk):
    return beam_artifact_api_pb2.PutArtifactRequest(
        data=beam_artifact_api_pb2.ArtifactChunk(data=chunk))

  @staticmethod
  def retrieve_artifact(retrieval_service, retrieval_token, name):
    return b''.join(
        chunk.data for chunk in retrieval_service.GetArtifact(
            beam_artifact_api_pb2.LegacyGetArtifactRequest(
                retrieval_token=retrieval_token, name=name)))

  def test_basic(self):
    self._run_staging(self._service, self._service)

  def test_with_grpc(self):
    server = grpc.server(UnboundedThreadPoolExecutor())
    try:
      beam_artifact_api_pb2_grpc.add_LegacyArtifactStagingServiceServicer_to_server(
          self._service, server)
      beam_artifact_api_pb2_grpc.add_LegacyArtifactRetrievalServiceServicer_to_server(
          self._service, server)
      port = server.add_insecure_port('[::]:0')
      server.start()
      channel = grpc.insecure_channel('localhost:%d' % port)
      self._run_staging(
          beam_artifact_api_pb2_grpc.LegacyArtifactStagingServiceStub(channel),
          beam_artifact_api_pb2_grpc.LegacyArtifactRetrievalServiceStub(
              channel))
      channel.close()
    finally:
      server.stop(1)

  def _run_staging(self, staging_service, retrieval_service):

    staging_session_token = '/session_staging_token \n\0*'

    # First stage some files.
    staging_service.PutArtifact(
        iter([
            self.put_metadata(staging_session_token, 'name'),
            self.put_data(b'data')
        ]))

    staging_service.PutArtifact(
        iter([
            self.put_metadata(staging_session_token, 'many_chunks'),
            self.put_data(b'a'),
            self.put_data(b'b'),
            self.put_data(b'c')
        ]))

    staging_service.PutArtifact(
        iter([
            self.put_metadata(staging_session_token, 'long'),
            self.put_data(b'a' * 1000)
        ]))

    staging_service.PutArtifact(
        iter([
            self.put_metadata(
                staging_session_token,
                'with_hash',
                hashlib.sha256(b'data...').hexdigest()),
            self.put_data(b'data'),
            self.put_data(b'...')
        ]))

    with self.assertRaises(Exception):
      staging_service.PutArtifact(
          iter([
              self.put_metadata(staging_session_token, 'bad_hash', 'bad_hash'),
              self.put_data(b'data')
          ]))

    manifest = beam_artifact_api_pb2.Manifest(
        artifact=[
            beam_artifact_api_pb2.ArtifactMetadata(name='name'),
            beam_artifact_api_pb2.ArtifactMetadata(name='many_chunks'),
            beam_artifact_api_pb2.ArtifactMetadata(name='long'),
            beam_artifact_api_pb2.ArtifactMetadata(name='with_hash'),
        ])

    retrieval_token = staging_service.CommitManifest(
        beam_artifact_api_pb2.CommitManifestRequest(
            staging_session_token=staging_session_token,
            manifest=manifest)).retrieval_token

    # Now attempt to retrieve them.

    retrieved_manifest = retrieval_service.GetManifest(
        beam_artifact_api_pb2.GetManifestRequest(
            retrieval_token=retrieval_token)).manifest
    self.assertEqual(manifest, retrieved_manifest)

    self.assertEqual(
        b'data',
        self.retrieve_artifact(retrieval_service, retrieval_token, 'name'))

    self.assertEqual(
        b'abc',
        self.retrieve_artifact(
            retrieval_service, retrieval_token, 'many_chunks'))

    self.assertEqual(
        b'a' * 1000,
        self.retrieve_artifact(retrieval_service, retrieval_token, 'long'))

    self.assertEqual(
        b'data...',
        self.retrieve_artifact(retrieval_service, retrieval_token, 'with_hash'))

    with self.assertRaises(Exception):
      self.retrieve_artifact(retrieval_service, retrieval_token, 'bad_hash')

    with self.assertRaises(Exception):
      self.retrieve_artifact(retrieval_service, retrieval_token, 'missing')

  def test_concurrent_requests(self):

    num_sessions = 7
    artifacts = collections.defaultdict(list)

    def name(index):
      # Overlapping names across sessions.
      return 'name%d' % (index // num_sessions)

    def session(index):
      return 'session%d' % (index % num_sessions)

    def delayed_data(data, index, max_msecs=1):
      time.sleep(max_msecs / 1000.0 * random.random())
      return ('%s_%d' % (data, index)).encode('ascii')

    def put(index):
      artifacts[session(index)].append(
          beam_artifact_api_pb2.ArtifactMetadata(name=name(index)))
      self._service.PutArtifact([
          self.put_metadata(session(index), name(index)),
          self.put_data(delayed_data('a', index)),
          self.put_data(delayed_data('b' * 20, index, 2))
      ])
      return session(index)

    def commit(session):
      return session, self._service.CommitManifest(
          beam_artifact_api_pb2.CommitManifestRequest(
              staging_session_token=session,
              manifest=beam_artifact_api_pb2.Manifest(
                  artifact=artifacts[session]))).retrieval_token

    def check(index):
      self.assertEqual(
          delayed_data('a', index) + delayed_data('b' * 20, index, 2),
          self.retrieve_artifact(
              self._service, tokens[session(index)], name(index)))

    # pylint: disable=range-builtin-not-iterating
    pool = UnboundedThreadPoolExecutor()
    sessions = set(pool.map(put, range(100)))
    tokens = dict(pool.map(commit, sessions))
    # List forces materialization.
    _ = list(pool.map(check, range(100)))


@unittest.skipIf(sys.version_info < (3, 6), "Requires Python 3.6+")
class ZipFileArtifactServiceTest(AbstractArtifactServiceTest):
  def create_service(self, staging_dir):
    return artifact_service.ZipFileArtifactService(
        os.path.join(staging_dir, 'test.zip'), 'root', chunk_size=10)


class BeamFilesystemArtifactServiceTest(AbstractArtifactServiceTest):
  def create_service(self, staging_dir):
    return artifact_service.BeamFilesystemArtifactService(
        staging_dir, chunk_size=10)


class InMemoryFileManager(object):
  def __init__(self, contents=()):
    self._contents = dict(contents)

  def get(self, path):
    return self._contents[path]

  def file_reader(self, path):
    return io.BytesIO(self._contents[path])

  def file_writer(self, name):
    path = 'prefix:' + name

    @contextlib.contextmanager
    def writable():
      buffer = io.BytesIO()
      yield buffer
      buffer.seek(0)
      self._contents[path] = buffer.read()

    return writable(), path


class ArtifactServiceTest(unittest.TestCase):
  def file_artifact(self, path):
    return beam_runner_api_pb2.ArtifactInformation(
        type_urn=common_urns.artifact_types.FILE.urn,
        type_payload=beam_runner_api_pb2.ArtifactFilePayload(
            path=path).SerializeToString())

  def embedded_artifact(self, data, name=None):
    return beam_runner_api_pb2.ArtifactInformation(
        type_urn=common_urns.artifact_types.EMBEDDED.urn,
        type_payload=beam_runner_api_pb2.EmbeddedFilePayload(
            data=data).SerializeToString(),
        role_urn=common_urns.artifact_roles.STAGING_TO.urn if name else None,
        role_payload=beam_runner_api_pb2.ArtifactStagingToRolePayload(
            staged_name=name).SerializeToString() if name else None)

  def test_file_retrieval(self):
    file_manager = InMemoryFileManager({
        'path/to/a': b'a', 'path/to/b': b'b' * 37
    })
    retrieval_service = artifact_service.ArtifactRetrievalService(
        file_manager.file_reader, chunk_size=10)
    dep_a = self.file_artifact('path/to/a')
    self.assertEqual(
        retrieval_service.ResolveArtifacts(
            beam_artifact_api_pb2.ResolveArtifactsRequest(artifacts=[dep_a])),
        beam_artifact_api_pb2.ResolveArtifactsResponse(replacements=[dep_a]))

    self.assertEqual(
        list(
            retrieval_service.GetArtifact(
                beam_artifact_api_pb2.GetArtifactRequest(artifact=dep_a))),
        [beam_artifact_api_pb2.GetArtifactResponse(data=b'a')])
    self.assertEqual(
        list(
            retrieval_service.GetArtifact(
                beam_artifact_api_pb2.GetArtifactRequest(
                    artifact=self.file_artifact('path/to/b')))),
        [
            beam_artifact_api_pb2.GetArtifactResponse(data=b'b' * 10),
            beam_artifact_api_pb2.GetArtifactResponse(data=b'b' * 10),
            beam_artifact_api_pb2.GetArtifactResponse(data=b'b' * 10),
            beam_artifact_api_pb2.GetArtifactResponse(data=b'b' * 7)
        ])

  def test_embedded_retrieval(self):
    retrieval_service = artifact_service.ArtifactRetrievalService(None)
    embedded_dep = self.embedded_artifact(b'some_data')
    self.assertEqual(
        list(
            retrieval_service.GetArtifact(
                beam_artifact_api_pb2.GetArtifactRequest(
                    artifact=embedded_dep))),
        [beam_artifact_api_pb2.GetArtifactResponse(data=b'some_data')])

  def test_url_retrieval(self):
    retrieval_service = artifact_service.ArtifactRetrievalService(None)
    url_dep = beam_runner_api_pb2.ArtifactInformation(
        type_urn=common_urns.artifact_types.URL.urn,
        type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
            url='file:' + quote(__file__)).SerializeToString())
    content = b''.join([
        r.data for r in retrieval_service.GetArtifact(
            beam_artifact_api_pb2.GetArtifactRequest(artifact=url_dep))
    ])
    with open(__file__, 'rb') as fin:
      self.assertEqual(content, fin.read())

  def test_push_artifacts(self):
    unresolved = beam_runner_api_pb2.ArtifactInformation(type_urn='unresolved')
    resolved_a = self.embedded_artifact(data=b'a', name='a.txt')
    resolved_b = self.embedded_artifact(data=b'bb', name='b.txt')
    dep_big = self.embedded_artifact(data=b'big ' * 100, name='big.txt')

    class TestArtifacts(object):
      def ResolveArtifacts(self, request):
        replacements = []
        for artifact in request.artifacts:
          if artifact.type_urn == 'unresolved':
            replacements += [resolved_a, resolved_b]
          else:
            replacements.append(artifact)
        return beam_artifact_api_pb2.ResolveArtifactsResponse(
            replacements=replacements)

      def GetArtifact(self, request):
        if request.artifact.type_urn == common_urns.artifact_types.EMBEDDED.urn:
          content = proto_utils.parse_Bytes(
              request.artifact.type_payload,
              beam_runner_api_pb2.EmbeddedFilePayload).data
          for k in range(0, len(content), 13):
            yield beam_artifact_api_pb2.GetArtifactResponse(
                data=content[k:k + 13])
        else:
          raise NotImplementedError

    file_manager = InMemoryFileManager()
    server = artifact_service.ArtifactStagingService(file_manager.file_writer)

    server.register_job('staging_token', {'env': [unresolved, dep_big]})

    # "Push" artifacts as if from a client.
    t = threading.Thread(
        target=lambda: artifact_service.offer_artifacts(
            server, TestArtifacts(), 'staging_token'))
    t.daemon = True
    t.start()

    resolved_deps = server.resolved_deps('staging_token', timeout=5)['env']
    expected = {
        'a.txt': b'a',
        'b.txt': b'bb',
        'big.txt': b'big ' * 100,
    }
    for dep in resolved_deps:
      self.assertEqual(dep.type_urn, common_urns.artifact_types.FILE.urn)
      self.assertEqual(dep.role_urn, common_urns.artifact_roles.STAGING_TO.urn)
      type_payload = proto_utils.parse_Bytes(
          dep.type_payload, beam_runner_api_pb2.ArtifactFilePayload)
      role_payload = proto_utils.parse_Bytes(
          dep.role_payload, beam_runner_api_pb2.ArtifactStagingToRolePayload)
      self.assertTrue(
          type_payload.path.endswith(role_payload.staged_name),
          type_payload.path)
      self.assertEqual(
          file_manager.get(type_payload.path),
          expected.pop(role_payload.staged_name))
    self.assertEqual(expected, {})


# Don't discover/test the abstract base class.
del AbstractArtifactServiceTest

if __name__ == '__main__':
  unittest.main()
