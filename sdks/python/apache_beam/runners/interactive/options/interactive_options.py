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

"""Module to expose options that control how Interactive Beam works.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

from dateutil import tz

from apache_beam.runners.interactive.options import capture_control


class InteractiveOptions(object):
  """An intermediate facade to query and configure options that guide how
  Interactive Beam works."""
  def __init__(self):
    self._capture_control = capture_control.CaptureControl()
    self._display_timestamp_format = '%Y-%m-%d %H:%M:%S.%f%z'
    self._display_timezone = tz.tzlocal()
    self._cache_root = None

  def __repr__(self):
    options_str = '\n'.join(
        '{} = {}'.format(k, getattr(self, k)) for k in dir(self)
        if k[0] != '_' and k != 'capture_control')
    return 'interactive_beam.options:\n{}'.format(options_str)

  @property
  def capture_control(self):
    return self._capture_control
