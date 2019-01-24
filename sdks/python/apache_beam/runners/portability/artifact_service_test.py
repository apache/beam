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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import random
import shutil
import tempfile
import time
import unittest
from concurrent import futures

import grpc

from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.runners.portability import artifact_service


class BeamFilesystemArtifactServiceTest(unittest.TestCase):

  def setUp(self):
    self._staging_dir = tempfile.mkdtemp()
    self._service = (
        artifact_service.BeamFilesystemArtifactService(
            self._staging_dir, chunk_size=10))

  def tearDown(self):
    if self._staging_dir:
      shutil.rmtree(self._staging_dir)

  @staticmethod
  def put_metadata(staging_token, name, sha256=None):
    return beam_artifact_api_pb2.PutArtifactRequest(
        metadata=beam_artifact_api_pb2.PutArtifactMetadata(
            staging_session_token=staging_token,
            metadata=beam_artifact_api_pb2.ArtifactMetadata(
                name=name,
                sha256=sha256)))

  @staticmethod
  def put_data(chunk):
    return beam_artifact_api_pb2.PutArtifactRequest(
        data=beam_artifact_api_pb2.ArtifactChunk(
            data=chunk))

  @staticmethod
  def retrieve_artifact(retrieval_service, retrieval_token, name):
    return b''.join(chunk.data for chunk in retrieval_service.GetArtifact(
        beam_artifact_api_pb2.GetArtifactRequest(
            retrieval_token=retrieval_token,
            name=name)))

  def test_basic(self):
    self._run_staging(self._service, self._service)

  def test_with_grpc(self):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    try:
      beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
          self._service, server)
      beam_artifact_api_pb2_grpc.add_ArtifactRetrievalServiceServicer_to_server(
          self._service, server)
      port = server.add_insecure_port('[::]:0')
      server.start()
      channel = grpc.insecure_channel('localhost:%d' % port)
      self._run_staging(
          beam_artifact_api_pb2_grpc.ArtifactStagingServiceStub(
              channel),
          beam_artifact_api_pb2_grpc.ArtifactRetrievalServiceStub(
              channel))
      channel.close()
    finally:
      server.stop(1)

  def _run_staging(self, staging_service, retrieval_service):

    staging_session_token = '/session_staging_token \n\0*'

    # First stage some files.
    staging_service.PutArtifact(iter([
        self.put_metadata(staging_session_token, 'name'),
        self.put_data(b'data')]))

    staging_service.PutArtifact(iter([
        self.put_metadata(staging_session_token, 'many_chunks'),
        self.put_data(b'a'),
        self.put_data(b'b'),
        self.put_data(b'c')]))

    staging_service.PutArtifact(iter([
        self.put_metadata(staging_session_token, 'long'),
        self.put_data(b'a' * 1000)]))

    staging_service.PutArtifact(iter([
        self.put_metadata(staging_session_token,
                          'with_hash',
                          hashlib.sha256(b'data...').hexdigest()),
        self.put_data(b'data'),
        self.put_data(b'...')]))

    with self.assertRaises(Exception):
      staging_service.PutArtifact(iter([
          self.put_metadata(staging_session_token,
                            'bad_hash',
                            'bad_hash'),
          self.put_data(b'data')]))

    manifest = beam_artifact_api_pb2.Manifest(artifact=[
        beam_artifact_api_pb2.ArtifactMetadata(name='name'),
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

    def name(index):
      # Overlapping names across sessions.
      return 'name%d' % (index // num_sessions)

    def session(index):
      return 'session%d' % (index % num_sessions)

    def delayed_data(data, index, max_msecs=1):
      time.sleep(max_msecs / 1000.0 * random.random())
      return ('%s_%d' % (data, index)).encode('ascii')

    def put(index):
      self._service.PutArtifact([
          self.put_metadata(session(index), name(index)),
          self.put_data(delayed_data('a', index)),
          self.put_data(delayed_data('b' * 20, index, 2))])
      return session(index)

    def commit(session):
      return session, self._service.CommitManifest(
          beam_artifact_api_pb2.CommitManifestRequest(
              staging_session_token=session,
              manifest=beam_artifact_api_pb2.Manifest())).retrieval_token

    def check(index):
      self.assertEqual(
          delayed_data('a', index) + delayed_data('b' * 20, index, 2),
          self.retrieve_artifact(
              self._service, tokens[session(index)], name(index)))

    # pylint: disable=range-builtin-not-iterating
    pool = futures.ThreadPoolExecutor(max_workers=10)
    sessions = set(pool.map(put, range(100)))
    tokens = dict(pool.map(commit, sessions))
    # List forces materialization.
    _ = list(pool.map(check, range(100)))


if __name__ == '__main__':
  unittest.main()
