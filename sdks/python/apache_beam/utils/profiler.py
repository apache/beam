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

"""A profiler context manager based on cProfile.Profile objects."""

import cProfile
import logging
import os
import pstats
import StringIO
import tempfile
import time


from apache_beam.utils.dependency import _dependency_file_copy


class Profile(object):
  """cProfile wrapper context for saving and logging profiler results."""

  SORTBY = 'cumulative'

  def __init__(self, profile_id, profile_location=None, log_results=False):
    self.stats = None
    self.profile_id = str(profile_id)
    self.profile_location = profile_location
    self.log_results = log_results

  def __enter__(self):
    logging.info('Start profiling: %s', self.profile_id)
    self.profile = cProfile.Profile()
    self.profile.enable()
    return self

  def __exit__(self, *args):
    self.profile.disable()
    logging.info('Stop profiling: %s', self.profile_id)

    if self.profile_location:
      dump_location = os.path.join(
          self.profile_location, 'profile',
          ('%s-%s' % (time.strftime('%Y-%m-%d_%H_%M_%S'), self.profile_id)))
      fd, filename = tempfile.mkstemp()
      self.profile.dump_stats(filename)
      logging.info('Copying profiler data to: [%s]', dump_location)
      _dependency_file_copy(filename, dump_location)  # pylint: disable=protected-access
      os.close(fd)
      os.remove(filename)

    if self.log_results:
      s = StringIO.StringIO()
      self.stats = pstats.Stats(
          self.profile, stream=s).sort_stats(Profile.SORTBY)
      self.stats.print_stats()
      logging.info('Profiler data: [%s]', s.getvalue())
