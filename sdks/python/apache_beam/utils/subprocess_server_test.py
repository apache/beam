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
import random
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
    self.assertEqual(
        'https://repository.apache.org/content/groups/staging/org/apache/beam/'
        'beam-sdks-java-fake/2.99.9/beam-sdks-java-fake-2.99.9.jar',
        subprocess_server.JavaJarServer.path_to_beam_jar(
            ':sdks:java:fake:fatJar', version='2.99.9rc2'))

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
        data = self.request.recv(1024)
        if 'User-Agent: Apache Beam SDK for Python' in str(data):
          self.request.sendall(b'HTTP/1.1 200 OK\n\ndata')
        else:
          self.request.sendall(b'HTTP/1.1 400 BAD REQUEST\n\n')

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

  def test_local_jar_fallback_to_google_maven_mirror(self):
    """Test that Google Maven mirror is used as fallback 
    when Maven Central fails."""
    class MavenCentralHandler(socketserver.BaseRequestHandler):
      timeout = 1

      def handle(self):
        # Simulate Maven Central returning 403 Forbidden
        self.request.sendall(b'HTTP/1.1 403 Forbidden\n\n')

    # Set up Maven Central server (will return 403)
    maven_port, = subprocess_server.pick_port(None)
    maven_server = socketserver.TCPServer(('localhost', maven_port),
                                          MavenCentralHandler)
    maven_thread = threading.Thread(target=maven_server.handle_request)
    maven_thread.daemon = True
    maven_thread.start()

    # Temporarily replace the Maven Central constant to use our test server
    original_maven_central = (
        subprocess_server.JavaJarServer.MAVEN_CENTRAL_REPOSITORY)

    try:
      subprocess_server.JavaJarServer.MAVEN_CENTRAL_REPOSITORY = (
          f'http://localhost:{maven_port}/maven2')

      with tempfile.TemporaryDirectory() as temp_dir:
        # Use a Maven Central URL that will trigger the fallback to real
        # Google mirror
        maven_url = (
            f'http://localhost:{maven_port}/maven2/org/apache/beam/'
            f'beam-sdks-java-extensions-schemaio-expansion-service/2.63.0/'
            f'beam-sdks-java-extensions-schemaio-expansion-service-2.63.0.jar')

        # This should fail on our mock Maven Central and fallback to the
        # real Google mirror
        jar_path = subprocess_server.JavaJarServer.local_jar(
            maven_url, temp_dir)

        # Verify the file was downloaded successfully (from the real Google
        # mirror)
        self.assertTrue(os.path.exists(jar_path))
        jar_size = os.path.getsize(jar_path)
        self.assertTrue(jar_size > 0)  # Should have actual content

    finally:
      # Restore original constants
      subprocess_server.JavaJarServer.MAVEN_CENTRAL_REPOSITORY = (
          original_maven_central)

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


class CacheTest(unittest.TestCase):
  @staticmethod
  def with_prefix(prefix):
    return '%s-%s' % (prefix, random.random())

  def test_memoization(self):
    cache = subprocess_server._SharedCache(self.with_prefix, lambda x: None)
    try:
      token = cache.register()
      a = cache.get('a')
      self.assertEqual(a[0], 'a')
      self.assertEqual(cache.get('a'), a)
      b = cache.get('b')
      self.assertEqual(b[0], 'b')
      self.assertEqual(cache.get('b'), b)
    finally:
      cache.purge(token)

  def test_purged(self):
    cache = subprocess_server._SharedCache(self.with_prefix, lambda x: None)
    try:
      token = cache.register()
      a = cache.get('a')
      self.assertEqual(cache.get('a'), a)
    finally:
      cache.purge(token)

    try:
      token = cache.register()
      new_a = cache.get('a')
      self.assertNotEqual(new_a, a)
    finally:
      cache.purge(token)

  def test_multiple_owners(self):
    cache = subprocess_server._SharedCache(self.with_prefix, lambda x: None)
    try:
      owner1 = cache.register()
      a = cache.get('a')
      try:
        self.assertEqual(cache.get('a'), a)
        owner2 = cache.register()
        b = cache.get('b')
        self.assertEqual(cache.get('b'), b)
      finally:
        cache.purge(owner2)
      self.assertEqual(cache.get('a'), a)
      self.assertEqual(cache.get('b'), b)
    finally:
      cache.purge(owner1)

    try:
      owner3 = cache.register()
      self.assertNotEqual(cache.get('a'), a)
      self.assertNotEqual(cache.get('b'), b)
    finally:
      cache.purge(owner3)

  def test_interleaved_owners(self):
    cache = subprocess_server._SharedCache(self.with_prefix, lambda x: None)
    owner1 = cache.register()
    a = cache.get('a')
    self.assertEqual(cache.get('a'), a)

    owner2 = cache.register()
    b = cache.get('b')
    self.assertEqual(cache.get('b'), b)

    cache.purge(owner1)
    self.assertNotEqual(cache.get('a'), a)
    self.assertEqual(cache.get('b'), b)

    cache.purge(owner2)
    owner3 = cache.register()
    self.assertNotEqual(cache.get('b'), b)
    cache.purge(owner3)


if __name__ == '__main__':
  unittest.main()
