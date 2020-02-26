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

from __future__ import absolute_import

from apache_beam.runners.interactive.options import capture_control


class InteractiveOptions(object):
  """An intermediate facade to query and configure options that guide how
  Interactive Beam works."""
  def __init__(self):
    self._capture_control = capture_control.CaptureControl()

  def __repr__(self):
    return 'With current interactive_beam.options: {}'.format(
        self._capture_control)

  @property
  def capture_control(self):
    return self._capture_control
