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

import glob
import os
import re
import shutil
import socketserver
import subprocess
import tempfile
import threading
import unittest

from apache_beam.utils import subprocess_server


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
    self.assertEqual(
        'https://repo.maven.apache.org/maven2/org/apache/beam/'
        'beam-sdks-java-fake/VERSION/beam-sdks-java-fake-A-VERSION.jar',
        subprocess_server.JavaJarServer.path_to_beam_jar(
            ':gradle:target:doesnt:matter',
            appendix='A',
            version='VERSION',
            artifact_id='beam-sdks-java-fake'))

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
    with self.assertRaisesRegex(
        Exception,
        re.escape(os.path.join('sdks',
                               'java',
                               'fake',
                               'build',
                               'libs',
                               'fake-artifact-id-A-VERSION-SNAPSHOT.jar')) +
        ' not found.'):
      subprocess_server.JavaJarServer.path_to_beam_jar(
          ':sdks:java:fake:fatJar',
          appendix='A',
          version='VERSION.dev',
          artifact_id='fake-artifact-id')

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

    with tempfile.TemporaryDirectory() as temp_dir:
      subprocess_server.JavaJarServer.local_jar(
          'http://localhost:%s/path/to/file.jar' % port, temp_dir)
      with open(os.path.join(temp_dir, 'file.jar')) as fin:
        self.assertEqual(fin.read(), 'data')

  @unittest.skipUnless(shutil.which('javac'), 'missing java jdk')
  def test_classpath_jar(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      try:
        # Avoid having to prefix everything in our test strings.
        oldwd = os.getcwd()
        os.chdir(temp_dir)

        with open('Main.java', 'w') as fout:
          fout.write(
              """
public class Main {
  public static void main(String[] args) { Other.greet(); }
}
          """)

        with open('Other.java', 'w') as fout:
          fout.write(
              """
public class Other {
  public static void greet() { System.out.println("You got me!"); }
}
          """)

        os.mkdir('jars')
        # Using split just for readability/copyability.
        subprocess.check_call('javac Main.java Other.java'.split())
        subprocess.check_call('jar cfe jars/Main.jar Main Main.class'.split())
        subprocess.check_call('jar cf jars/Other.jar Other.class'.split())
        # Make sure the java and class files don't get picked up.
        for path in glob.glob('*.*'):
          os.unlink(path)

        # These should fail.
        self.assertNotEqual(
            subprocess.call('java -jar jars/Main.jar'.split()), 0)
        self.assertNotEqual(
            subprocess.call('java -jar jars/Other.jar'.split()), 0)

        os.mkdir('beam_temp')
        composite_jar = subprocess_server.JavaJarServer.make_classpath_jar(
            'jars/Main.jar', ['jars/Other.jar'], cache_dir='beam_temp')
        # This, however, should work.
        subprocess.check_call(f'java -jar {composite_jar}'.split())

      finally:
        os.chdir(oldwd)


if __name__ == '__main__':
  unittest.main()
