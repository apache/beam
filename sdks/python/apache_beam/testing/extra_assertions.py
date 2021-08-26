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
# pytype: skip-file

import numpy as np


class ExtraAssertionsMixin(object):
  def assertUnhashableCountEqual(self, data1, data2):
    """Assert that two containers have the same items, with special treatment
    for numpy arrays.
    """
    try:
      self.assertCountEqual(data1, data2)
    except (TypeError, ValueError):
      data1 = [self._to_hashable(d) for d in data1]
      data2 = [self._to_hashable(d) for d in data2]
      self.assertCountEqual(data1, data2)

  def _to_hashable(self, element):
    try:
      hash(element)
      return element
    except TypeError:
      pass

    if isinstance(element, list):
      return tuple(self._to_hashable(e) for e in element)

    if isinstance(element, dict):
      hashable_elements = []
      for key, value in sorted(element.items(), key=lambda t: hash(t[0])):
        hashable_elements.append((key, self._to_hashable(value)))
      return tuple(hashable_elements)

    if isinstance(element, np.ndarray):
      return element.tobytes()

    raise AssertionError("Encountered unhashable element: {}.".format(element))
