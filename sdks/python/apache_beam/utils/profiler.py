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

"""A profiler context manager based on cProfile.Profile and guppy.hpy objects.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file
# mypy: check-untyped-defs

import cProfile
import io
import logging
import os
import pstats
import random
import tempfile
import time
from typing import Callable
from typing import Optional

from apache_beam.io import filesystems

_LOGGER = logging.getLogger(__name__)


class Profile(object):
  """cProfile and Heapy wrapper context for saving and logging profiler
  results."""

  SORTBY = 'cumulative'

  profile_output = None  # type: str
  stats = None  # type: pstats.Stats

  def __init__(
      self,
      profile_id, # type: str
      profile_location=None, # type: Optional[str]
      log_results=False, # type: bool
      file_copy_fn=None, # type: Optional[Callable[[str, str], None]]
      time_prefix='%Y-%m-%d_%H_%M_%S-', # type: str
      enable_cpu_profiling=False, # type: bool
      enable_memory_profiling=False, # type: bool
  ):
    """Creates a Profile object.

    Args:
      profile_id: Unique id of the profiling session.
      profile_location: The file location where the profiling results will be
        stored.
      log_results: Log the result to console if true.
      file_copy_fn: Lambda function for copying files.
      time_prefix: Format of the timestamp prefix in profiling result files.
      enable_cpu_profiling: CPU profiler will be enabled during the profiling
        session.
      enable_memory_profiling: Memory profiler will be enabled during the
        profiling session, the profiler only records the newly allocated objects
        in this session.
    """
    self.profile_id = str(profile_id)
    self.profile_location = profile_location
    self.log_results = log_results
    self.file_copy_fn = file_copy_fn or self.default_file_copy_fn
    self.time_prefix = time_prefix
    self.enable_cpu_profiling = enable_cpu_profiling
    self.enable_memory_profiling = enable_memory_profiling

  def __enter__(self):
    _LOGGER.info('Start profiling: %s', self.profile_id)
    if self.enable_cpu_profiling:
      self.profile = cProfile.Profile()
      self.profile.enable()
    if self.enable_memory_profiling:
      try:
        from guppy import hpy
        self.hpy = hpy()
        self.hpy.setrelheap()
      except ImportError:
        _LOGGER.info("Unable to import guppy for memory profiling")
        self.hpy = None
    return self

  def __exit__(self, *args):
    _LOGGER.info('Stop profiling: %s', self.profile_id)

    if self.profile_location:
      if self.enable_cpu_profiling:
        self.profile.create_stats()
        self.profile_output = self._upload_profile_data(
            # typing: seems stats attr is missing from typeshed
            self.profile_location, 'cpu_profile', self.profile.stats)  # type: ignore[attr-defined]

      if self.enable_memory_profiling:
        if not self.hpy:
          pass
        else:
          h = self.hpy.heap()
          heap_dump_data = '%s\n%s' % (h, h.more)
          self._upload_profile_data(
              self.profile_location,
              'memory_profile',
              heap_dump_data,
              write_binary=False)

    if self.log_results:
      if self.enable_cpu_profiling:
        s = io.StringIO()
        self.stats = pstats.Stats(
            self.profile, stream=s).sort_stats(Profile.SORTBY)
        self.stats.print_stats()
        _LOGGER.info('Cpu profiler data: [%s]', s.getvalue())
      if self.enable_memory_profiling and self.hpy:
        _LOGGER.info('Memory profiler data: \n%s' % self.hpy.heap())

  @staticmethod
  def default_file_copy_fn(src, dest):
    dest_handle = filesystems.FileSystems.create(dest + '.tmp')
    try:
      with open(src, 'rb') as src_handle:
        dest_handle.write(src_handle.read())
    finally:
      dest_handle.close()
    filesystems.FileSystems.rename([dest + '.tmp'], [dest])

  @staticmethod
  def factory_from_options(options):
    # type: (...) -> Optional[Callable[..., Profile]]
    if options.profile_cpu or options.profile_memory:

      def create_profiler(profile_id, **kwargs):
        if random.random() < options.profile_sample_rate:
          return Profile(
              profile_id,
              options.profile_location,
              enable_cpu_profiling=options.profile_cpu,
              enable_memory_profiling=options.profile_memory,
              **kwargs)

      return create_profiler
    return None

  def _upload_profile_data(
      self, profile_location, dir, data, write_binary=True):
    # type: (...) -> str
    dump_location = os.path.join(
        profile_location,
        dir,
        time.strftime(self.time_prefix + self.profile_id))
    fd, filename = tempfile.mkstemp()
    try:
      os.close(fd)
      if write_binary:
        with open(filename, 'wb') as fb:
          import marshal
          marshal.dump(data, fb)
      else:
        with open(filename, 'w') as f:
          f.write(data)
      _LOGGER.info('Copying profiler data to: [%s]', dump_location)
      self.file_copy_fn(filename, dump_location)
    finally:
      os.remove(filename)

    return dump_location
