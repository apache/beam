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

import filecmp
import logging
import os
import random
import shutil
import string
import tempfile
import unittest
from concurrent import futures

import grpc

from apache_beam.portability.api import beam_artifact_api_pb2
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.runners.portability import portable_stager


class PortableStagerTest(unittest.TestCase):

  def setUp(self):
    self._temp_dir = tempfile.mkdtemp()
    self._remote_dir = tempfile.mkdtemp()

  def tearDown(self):
    if self._temp_dir:
      shutil.rmtree(self._temp_dir)
    if self._remote_dir:
      shutil.rmtree(self._remote_dir)

  def _stage_files(self, files):
    """Utility method to stage files.

      Args:
        files: a list of tuples of the form [(local_name, remote_name),...]
          describing the name of the artifacts in local temp folder and desired
          name in staging location.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    staging_service = TestLocalFileSystemArtifactStagingServiceServicer(
        self._remote_dir)
    beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
        staging_service, server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()
    stager = portable_stager.PortableStager(
        artifact_service_channel=grpc.insecure_channel(
            'localhost:%s' % test_port),
        staging_session_token='token')
    for from_file, to_file in files:
      stager.stage_artifact(
          local_path_to_artifact=os.path.join(self._temp_dir, from_file),
          artifact_name=to_file)
    stager.commit_manifest()
    return staging_service.manifest.artifact, staging_service.retrieval_tokens

  def test_stage_single_file(self):
    from_file = 'test_local.txt'
    to_file = 'test_remote.txt'

    with open(os.path.join(self._temp_dir, from_file), 'wb') as f:
      f.write(b'abc')

    copied_files, retrieval_tokens = self._stage_files([('test_local.txt',
                                                         'test_remote.txt')])
    self.assertTrue(
        filecmp.cmp(
            os.path.join(self._temp_dir, from_file),
            os.path.join(self._remote_dir, to_file)))
    self.assertEqual(
        [to_file],
        [staged_file_metadata.name for staged_file_metadata in copied_files])
    self.assertEqual(retrieval_tokens, frozenset(['token']))

  def test_stage_multiple_files(self):

    files = [
        ('test_local_100.txt', 'test_remote_100.txt', 100, 's'),  #
        ('test_local_100.binary', 'test_remote_100.binary', 100, 'b'),  #
        ('test_local_1k.txt', 'test_remote_1k.txt', 1 << 10, 's'),  #
        ('test_local_1k.binary', 'test_remote_1k.binary', 1 << 10, 'b'),  #
        ('test_local_1m.txt', 'test_remote_1m.txt', 1 << 20, 's'),
        ('test_local_1m.binary', 'test_remote_1m.binary', 1 << 20, 'b'),
        ('test_local_10m.txt', 'test_remote_10m.txt', 10 * (1 << 20), 's'),
        ('test_local_10m.binary', 'test_remote_10m.binary', 10 * (1 << 20), 'b')
    ]

    for (from_file, _, size, type) in files:
      chars = list(string.printable)
      random.shuffle(chars)
      chars = list(int(size / len(chars)) * chars + chars[0:size % len(chars)])
      if type == 's':
        with open(
            os.path.join(self._temp_dir, from_file), 'w',
            buffering=2 << 22) as f:
          f.write(''.join(chars))
      if type == 'b':
        chars = [char.encode('ascii') for char in chars]
        with open(
            os.path.join(self._temp_dir, from_file), 'wb',
            buffering=2 << 22) as f:
          f.write(b''.join(chars))

    copied_files, retrieval_tokens = self._stage_files(
        [(from_file, to_file) for (from_file, to_file, _, _) in files])

    for from_file, to_file, _, _ in files:
      from_file = os.path.join(self._temp_dir, from_file)
      to_file = os.path.join(self._remote_dir, to_file)
      self.assertTrue(
          filecmp.cmp(from_file, to_file),
          'Local file {0} and remote file {1} are not the same.'.format(
              from_file, to_file))
    self.assertEqual([to_file for _, to_file, _, _ in files].sort(), [
        staged_file_metadata.name for staged_file_metadata in copied_files
    ].sort())
    self.assertEqual(retrieval_tokens, frozenset(['token']))


class TestLocalFileSystemArtifactStagingServiceServicer(
    beam_artifact_api_pb2_grpc.ArtifactStagingServiceServicer):

  def __init__(self, temp_dir):
    super(TestLocalFileSystemArtifactStagingServiceServicer, self).__init__()
    self.temp_dir = temp_dir
    self.manifest = None
    self.retrieval_tokens = set()

  def PutArtifact(self, request_iterator, context):
    first = True
    file_name = None
    for request in request_iterator:
      if first:
        first = False
        self.retrieval_tokens.add(request.metadata.staging_session_token)
        file_name = request.metadata.metadata.name
      else:
        with open(os.path.join(self.temp_dir, file_name), 'ab') as f:
          f.write(request.data.data)

    return beam_artifact_api_pb2.PutArtifactResponse()

  def CommitManifest(self, request, context):
    self.manifest = request.manifest
    self.retrieval_tokens.add(request.staging_session_token)
    return beam_artifact_api_pb2.CommitManifestResponse(retrieval_token='token')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
