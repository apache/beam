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

"""Unit tests for the processes module."""

# pytype: skip-file

from __future__ import absolute_import

import os
import re
import shutil
import socketserver
import tempfile
import threading
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import

from apache_beam.utils import subprocess_server


# TODO(Py3): Use tempfile.TemporaryDirectory
class TemporaryDirectory:
  def __enter__(self):
    self._path = tempfile.mkdtemp()
    return self._path

  def __exit__(self, *args):
    shutil.rmtree(self._path, ignore_errors=True)


class JavaJarServerTest(unittest.TestCase):
  def test_gradle_jar_release(self):
    self.assertEqual(
        'https://repo.maven.apache.org/maven2/org/apache/beam/'
        'beam-sdks-java-fake/VERSION/beam-sdks-java-fake-VERSION.jar',
        subprocess_server.JavaJarServer.path_to_beam_jar(
            ':sdks:java:fake:fatJar', version='VERSION'))
    self.assertEqual(
        'https://repo.maven.apache.org/maven2/org/apache/beam/'
        'beam-sdks-java-fake/VERSION/beam-sdks-java-fake-A-VERSION.jar',
        subprocess_server.JavaJarServer.path_to_beam_jar(
            ':sdks:java:fake:fatJar', appendix='A', version='VERSION'))

  def test_gradle_jar_dev(self):
    with self.assertRaisesRegex(
        Exception,
        re.escape(os.path.join('sdks',
                               'java',
                               'fake',
                               'build',
                               'libs',
                               'beam-sdks-java-fake-VERSION-SNAPSHOT.jar')) +
        ' not found.'):
      subprocess_server.JavaJarServer.path_to_beam_jar(
          ':sdks:java:fake:fatJar', version='VERSION.dev')
    with self.assertRaisesRegex(
        Exception,
        re.escape(os.path.join('sdks',
                               'java',
                               'fake',
                               'build',
                               'libs',
                               'beam-sdks-java-fake-A-VERSION-SNAPSHOT.jar')) +
        ' not found.'):
      subprocess_server.JavaJarServer.path_to_beam_jar(
          ':sdks:java:fake:fatJar', appendix='A', version='VERSION.dev')

  def test_beam_services(self):
    with subprocess_server.JavaJarServer.beam_services({':some:target': 'foo'}):
      self.assertEqual(
          'foo',
          subprocess_server.JavaJarServer.path_to_beam_jar(':some:target'))

  def test_local_jar(self):
    class Handler(socketserver.BaseRequestHandler):
      timeout = 1

      def handle(self):
        self.request.recv(1024)
        self.request.sendall(b'HTTP/1.1 200 OK\n\ndata')

    port, = subprocess_server.pick_port(None)
    server = socketserver.TCPServer(('localhost', port), Handler)
    t = threading.Thread(target=server.handle_request)
    t.daemon = True
    t.start()

    with TemporaryDirectory() as temp_dir:
      subprocess_server.JavaJarServer.local_jar(
          'http://localhost:%s/path/to/file.jar' % port, temp_dir)
      with open(os.path.join(temp_dir, 'file.jar')) as fin:
        self.assertEqual(fin.read(), 'data')


if __name__ == '__main__':
  unittest.main()
