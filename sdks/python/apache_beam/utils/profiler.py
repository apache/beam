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

"""A profiler context manager based on cProfile.Profile objects.

For internal use only; no backwards-compatibility guarantees.
"""

import cProfile
import logging
import os
import pstats
import sys
import tempfile
import time
import warnings
from threading import Timer

if sys.version_info[0] < 3:
  import StringIO
else:
  from io import StringIO


class Profile(object):
  """cProfile wrapper context for saving and logging profiler results."""

  SORTBY = 'cumulative'

  def __init__(self, profile_id, profile_location=None, log_results=False,
               file_copy_fn=None):
    self.stats = None
    self.profile_id = str(profile_id)
    self.profile_location = profile_location
    self.log_results = log_results
    self.file_copy_fn = file_copy_fn

  def __enter__(self):
    logging.info('Start profiling: %s', self.profile_id)
    self.profile = cProfile.Profile()
    self.profile.enable()
    return self

  def __exit__(self, *args):
    self.profile.disable()
    logging.info('Stop profiling: %s', self.profile_id)

    if self.profile_location and self.file_copy_fn:
      dump_location = os.path.join(
          self.profile_location, 'profile',
          ('%s-%s' % (time.strftime('%Y-%m-%d_%H_%M_%S'), self.profile_id)))
      fd, filename = tempfile.mkstemp()
      self.profile.dump_stats(filename)
      logging.info('Copying profiler data to: [%s]', dump_location)
      self.file_copy_fn(filename, dump_location)  # pylint: disable=protected-access
      os.close(fd)
      os.remove(filename)

    if self.log_results:
      s = StringIO()
      self.stats = pstats.Stats(
          self.profile, stream=s).sort_stats(Profile.SORTBY)
      self.stats.print_stats()
      logging.info('Profiler data: [%s]', s.getvalue())


class MemoryReporter(object):
  """A memory reporter that reports the memory usage and heap profile.
  Usage:::

    mr = MemoryReporter(interval_second=30.0)
    mr.start()
    while ...
      <do something>
      # this will report continuously with 30 seconds between reports.
    mr.stop()

  NOTE: A reporter with start() should always stop(), or the parent process can
  never finish.

  Or simply the following which does star() and stop():
    with MemoryReporter(interval_second=100):
      while ...
        <do some thing>

  Also it could report on demand without continuous reporting.::

    mr = MemoryReporter()  # default interval 60s but not started.
    <do something>
    mr.report_once()
  """

  def __init__(self, interval_second=60.0):
    # guppy might not have installed. http://pypi.python.org/pypi/guppy/0.1.10
    # The reporter can be set up only when guppy is installed (and guppy cannot
    # be added to the required packages in setup.py, since it's not available
    # in all platforms).
    try:
      from guppy import hpy  # pylint: disable=import-error
      self._hpy = hpy
      self._interval_second = interval_second
      self._timer = None
    except ImportError:
      warnings.warn('guppy is not installed; MemoryReporter not available.')
      self._hpy = None
    self._enabled = False

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, *args):
    self.stop()

  def start(self):
    if self._enabled or not self._hpy:
      return
    self._enabled = True

    def report_with_interval():
      if not self._enabled:
        return
      self.report_once()
      self._timer = Timer(self._interval_second, report_with_interval)
      self._timer.start()

    self._timer = Timer(self._interval_second, report_with_interval)
    self._timer.start()

  def stop(self):
    if not self._enabled:
      return
    self._timer.cancel()
    self._enabled = False

  def report_once(self):
    if not self._hpy:
      return
    report_start_time = time.time()
    heap_profile = self._hpy().heap()
    logging.info('*** MemoryReport Heap:\n %s\n MemoryReport took %.1f seconds',
                 heap_profile, time.time() - report_start_time)
