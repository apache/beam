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

"""A ValueProvider class to implement templates with both hard-coded
and dynamically provided values.
"""


class ValueProvider(object):
  def is_accessible(self):
    raise NotImplementedError(
        'ValueProvider.is_accessible implemented in derived classes'
    )

  def get(self):
    raise NotImplementedError(
        'ValueProvider.get implemented in derived classes'
    )


class StaticValueProvider(object):
  def __init__(self, value_class, value):
    self.value_class = value_class
    self.data = value_class(value)
    self.accessible = True

  def is_accessible(self):
    return self.accessible

  def get(self):
    return self.data

  def __str__(self):
    return '%s(type=%s, value=%s)' % (self.__class__.__name__,
                                      self.value_class.__name__,
                                      repr(self.data))
