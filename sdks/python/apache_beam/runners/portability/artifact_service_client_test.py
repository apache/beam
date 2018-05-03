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
from apache_beam.runners.portability import artifact_service_client


class ArtifactStagingFileHandlerTest(unittest.TestCase):

  def setUp(self):
    self._temp_dir = tempfile.mkdtemp()
    self._remote_dir = tempfile.mkdtemp()

  def tearDown(self):
    if self._temp_dir:
      shutil.rmtree(self._temp_dir)
    if self._remote_dir:
      shutil.rmtree(self._remote_dir)

  def copy_files(self, files):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
        TestLocalFileSystemArtifactStagingServiceServicer(self._remote_dir),
        server)
    test_port = server.add_insecure_port('[::]:0')
    server.start()
    file_handler = artifact_service_client.ArtifactStagingFileHandler(
        grpc.insecure_channel('localhost:%s' % test_port))
    for from_file, to_file in files:
      file_handler.file_copy(
          from_path=os.path.join(self._temp_dir, from_file), to_path=to_file)
    file_handler.commit_manifest()
    return file_handler._artifacts

  def test_upload_single_file(self):
    from_file = 'test_local.txt'
    to_file = 'test_remote.txt'

    with open(os.path.join(self._temp_dir, from_file), 'wb') as f:
      f.write(b'abc')

    copied_files = self.copy_files([('test_local.txt', 'test_remote.txt')])
    self.assertTrue(
        filecmp.cmp(
            os.path.join(self._temp_dir, from_file),
            os.path.join(self._remote_dir, to_file)))
    self.assertEqual([to_file], [manifest.name for manifest in copied_files])

  def test_upload_multiple_files(self):

    files = [
        ('test_local_100.txt', 'test_remote_100.txt', 101, 's'),  #
        ('test_local_100.binary', 'test_remote_100.binary', 100, 'b'),  #
        ('test_local_1k.txt', 'test_remote_1k.txt', 2 << 10, 's'),  #
        ('test_local_1k.binary', 'test_remote_1k.binary', 2 << 10, 'b'),  #
        ('test_local_1m.txt', 'test_remote_1m.txt', 2 << 20, 's'),
        ('test_local_1m.binary', 'test_remote_1m.binary', 2 << 20, 'b'),
        ('test_local_10m.txt', 'test_remote_10m.txt', 10 * (2 << 20), 's'),
        ('test_local_10m.binary', 'test_remote_10m.binary', 10 * (2 << 20), 'b')
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
        with open(
            os.path.join(self._temp_dir, from_file), 'wb',
            buffering=2 << 22) as f:
          f.write(''.join(chars))

    copied_files = self.copy_files(
        [(from_file, to_file) for (from_file, to_file, _, _) in files])

    for from_file, to_file, _, _ in files:
      ff = os.path.join(self._temp_dir, from_file)
      rf = os.path.join(self._remote_dir, to_file)
      self.assertTrue(
          filecmp.cmp(ff, rf),
          'Local file {0} and remote file {1} are not the same.'.format(ff, rf))
    self.assertEqual([to_file for _, to_file, _, _ in files],
                     [manifest.name for manifest in copied_files])


class TestLocalFileSystemArtifactStagingServiceServicer(
    beam_artifact_api_pb2_grpc.ArtifactStagingServiceServicer):

  def __init__(self, temp_dir):
    super(TestLocalFileSystemArtifactStagingServiceServicer, self).__init__()
    self.temp_dir = temp_dir

  def PutArtifact(self, request_iterator, context):
    first = True
    file_name = None
    for request in request_iterator:
      if first:
        first = False
        file_name = request.metadata.name
      else:
        with open(os.path.join(self.temp_dir, file_name), 'ab') as f:
          f.write(request.data.data)

    return beam_artifact_api_pb2.PutArtifactResponse()

  def CommitManifest(self, request, context):
    return beam_artifact_api_pb2.CommitManifestResponse(staging_token='token')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
