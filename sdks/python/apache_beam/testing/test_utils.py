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

"""Utility methods for testing

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

import contextlib
import hashlib
import importlib
import importlib.abc
import os
import shutil
import struct
import sys
import tempfile

import numpy as np

from apache_beam.io.filesystems import FileSystems
from apache_beam.utils import retry

DEFAULT_HASHING_ALG = 'sha1'


class TempDir(object):
  """Context Manager to create and clean-up a temporary directory."""
  def __init__(self):
    self._tempdir = tempfile.mkdtemp()

  def __enter__(self):
    return self

  def __exit__(self, *args):
    if os.path.exists(self._tempdir):
      shutil.rmtree(self._tempdir)

  def get_path(self):
    """Returns the path to the temporary directory."""
    return self._tempdir

  def create_temp_file(self, suffix='', lines=None):
    """Creates a temporary file in the temporary directory.

    Args:
      suffix (str): The filename suffix of the temporary file (e.g. '.txt')
      lines (List[str]): A list of lines that will be written to the temporary
        file.
    Returns:
      The name of the temporary file created.
    """
    with tempfile.NamedTemporaryFile(delete=False,
                                     dir=self._tempdir,
                                     suffix=suffix) as f:
      if lines:
        for line in lines:
          f.write(line)

      return f.name


def compute_hash(content, hashing_alg=DEFAULT_HASHING_ALG):
  """Compute a hash value of a list of objects by hashing their string
  representations."""
  content = [
      str(x).encode('utf-8') if not isinstance(x, bytes) else x for x in content
  ]
  content.sort()
  m = hashlib.new(hashing_alg)
  for elem in content:
    m.update(elem)
  return m.hexdigest()


def patch_retry(testcase, module):
  """A function to patch retry module to use mock clock and logger.

  Clock and logger that defined in retry decorator will be replaced in test
  in order to skip sleep phase when retry happens.

  Args:
    testcase: An instance of unittest.TestCase that calls this function to
      patch retry module.
    module: The module that uses retry and need to be replaced with mock
      clock and logger in test.
  """
  # Import mock here to avoid execution time errors for other utilities
  from mock import Mock
  from mock import patch

  real_retry_with_exponential_backoff = retry.with_exponential_backoff

  def patched_retry_with_exponential_backoff(**kwargs):
    """A patch for retry decorator to use a mock dummy clock and logger."""
    kwargs.update(logger=Mock(), clock=Mock())
    return real_retry_with_exponential_backoff(**kwargs)

  patch.object(
      retry,
      'with_exponential_backoff',
      side_effect=patched_retry_with_exponential_backoff).start()

  # Reload module after patching.
  importlib.reload(module)

  def remove_patches():
    patch.stopall()
    # Reload module again after removing patch.
    importlib.reload(module)

  testcase.addCleanup(remove_patches)


class _ImportBlocker(importlib.abc.MetaPathFinder):
  """A meta path finder that blocks, and records, imports of given packages."""
  def __init__(self, packages):
    self._packages = tuple(packages)
    self.attempted_imports = []

  def blocks(self, module_name):
    return any(
        module_name == package or module_name.startswith(package + '.')
        for package in self._packages)

  def find_spec(self, fullname, path, target=None):
    if self.blocks(fullname):
      self.attempted_imports.append(fullname)
      raise ModuleNotFoundError(
          'Import of %s is blocked for testing.' % fullname, name=fullname)
    return None


@contextlib.contextmanager
def block_imports(*packages):
  """Context manager that makes the given packages unimportable.

  Within the context, importing any of the given packages, or their
  submodules, raises a ModuleNotFoundError as if they were not installed. This
  also applies to packages that were already imported. Such packages are
  restored when exiting the context.

  This is useful, for example, for verifying that constructing and submitting
  a pipeline neither requires, nor tries to import, a dependency that is only
  needed when executing the pipeline on workers::

    with block_imports('sentence_transformers') as attempted_imports:
      ...  # Construct and serialize the pipeline.
    assert not attempted_imports

  Args:
    *packages: names of the top-level packages, or modules, to block.

  Yields:
    A list with the names of all modules whose import was attempted, and
    blocked, within the context.
  """
  blocker = _ImportBlocker(packages)
  hidden_modules = {
      name: module
      for name, module in list(sys.modules.items()) if blocker.blocks(name)
  }
  for name in hidden_modules:
    del sys.modules[name]
  sys.meta_path.insert(0, blocker)
  try:
    yield blocker.attempted_imports
  finally:
    sys.meta_path.remove(blocker)
    sys.modules.update(hidden_modules)


@retry.with_exponential_backoff(
    num_retries=3, retry_filter=retry.retry_on_beam_io_error_filter)
def delete_files(file_paths):
  """A function to clean up files or directories using ``FileSystems``.

  Glob is supported in file path and directories will be deleted recursively.

  Args:
    file_paths: A list of strings contains file paths or directories.
  """
  if len(file_paths) == 0:
    raise RuntimeError('Clean up failed. Invalid file path: %s.' % file_paths)
  FileSystems.delete(file_paths)


def cleanup_subscriptions(sub_client, subs):
  """Cleanup PubSub subscriptions if exist."""
  for sub in subs:
    sub_client.delete_subscription(subscription=sub.name)


def cleanup_topics(pub_client, topics):
  """Cleanup PubSub topics if exist."""
  for topic in topics:
    pub_client.delete_topic(topic=topic.name)


class PullResponseMessage(object):
  """Data representing a pull request response.

  Utility class for ``create_pull_response``.
  """
  def __init__(
      self,
      data,
      attributes=None,
      publish_time_secs=None,
      publish_time_nanos=None,
      ack_id=None):
    self.data = data
    self.attributes = attributes
    self.publish_time_secs = publish_time_secs
    self.publish_time_nanos = publish_time_nanos
    self.ack_id = ack_id


def create_pull_response(responses):
  """Create an instance of ``google.cloud.pubsub.types.ReceivedMessage``.

  Used to simulate the response from pubsub.SubscriberClient().pull().

  Args:
    responses: list of ``PullResponseMessage``

  Returns:
    An instance of ``google.cloud.pubsub.types.PullResponse`` populated with
    responses.
  """
  from google.cloud import pubsub
  from google.protobuf import timestamp_pb2

  res = pubsub.types.PullResponse()
  for response in responses:
    received_message = pubsub.types.ReceivedMessage()

    message = received_message.message
    message.data = response.data
    if response.attributes is not None:
      for k, v in response.attributes.items():
        message.attributes[k] = v

    publish_time = timestamp_pb2.Timestamp()
    if response.publish_time_secs is not None:
      publish_time.seconds = response.publish_time_secs
    if response.publish_time_nanos is not None:
      publish_time.nanos = response.publish_time_nanos
    message.publish_time = publish_time

    if response.ack_id is not None:
      received_message.ack_id = response.ack_id

    res.received_messages.append(received_message)

  return res


def create_file(path, contents):
  """Create a file to use as input to test pipelines"""
  with FileSystems.create(path) as f:
    f.write(str.encode(contents, 'utf-8'))
  return path


def read_files_from_pattern(file_pattern):
  """Reads the files that match a pattern"""
  metadata_list = FileSystems.match([file_pattern])[0].metadata_list
  output = []
  for metadata in metadata_list:
    with FileSystems.open(metadata.path) as f:
      output.append(f.read().decode('utf-8').strip())
  return '\n'.join(output)


class LCGenerator:
  """A pure Python implementation of linear congruential generator."""
  def __init__(self, a=0x5DEECE66D, c=0xB, bits=48):
    self._a = a
    self._c = c
    self._bits = bits
    self._mask = (1 << self._bits) - 1
    self.seed(0)

  def seed(self, seed):
    """Set the seed for the generator."""

    # This implementation is made different than the JDK equivalent
    # self._seed = (seed ^ self._a) & self._mask
    # to avoid that close seed gives close first pseudo random number.
    self._seed = (seed * self._a + self._c) & self._mask

  def seed_jdk(self, seed):
    """
    Set the seed in the way equivalent to java.util.Random.seed.

    This method provide a JDK equivalent random generator. Note that the JDK
    implementation has a known issue that is close seed generating close first
    pseudo-random number.
    """
    self._seed = (seed ^ self._a) & self._mask

  def next_int(self):
    """
    Get the next integer value in the range of np.int32 ([0, 2^31-1]).

    Equivalent to java.util.Random.nextInt.
    """
    self.seed(self._seed)
    return np.int32(np.int64(self._seed) >> (self._bits - 32))

  def next_uint(self):
    """
    Get the next unsigned integer value in the range of np.uint32.

    Equivalent to java.util.Random.next(32).
    """
    self.seed(self._seed)
    return self._seed >> (self._bits - 32)

  def randbytes(self, length):
    """
    Get random bytes of given length.

    Equivalent to java.util.Random.nextBytes.
    """
    ints = (length + 3) // 4
    return struct.pack("<%iI" % ints,
                       *(self.next_uint() for _ in range(ints)))[:length]

  def random_sample(self):
    """
    Get random sample between [0.0, 1.0), single precision float value.

    Equivalent to java.util.Random.nextFloat.
    """
    return float(self.next_uint() >> 8) / (1 << 24)
