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

"""A BeamPlugin base class.

For experimental usage only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

from builtins import object


class BeamPlugin(object):
  """Plugin base class to be extended by dependent users such as FileSystem.
  Any instantiated subclass will be imported at worker startup time."""

  @classmethod
  def get_all_subclasses(cls):
    """Get all the subclasses of the BeamPlugin class."""
    all_subclasses = []
    for subclass in cls.__subclasses__():
      all_subclasses.append(subclass)
      all_subclasses.extend(subclass.get_all_subclasses())
    return all_subclasses

  @classmethod
  def get_all_plugin_paths(cls):
    """Get full import paths of the BeamPlugin subclass."""
    def fullname(o):
      return o.__module__ + "." + o.__name__
    return [fullname(o) for o in cls.get_all_subclasses()]
