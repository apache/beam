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

"""Cloud Datastore helper functions."""


def key_comparator(k1, k2):
  """A comparator for Datastore Keys."""

  if k1.partition_id != k2.partition_id:
    raise ValueError('Cannot compare keys with different partition ids.')

  k1_iter = iter(k1.path)
  k2_iter = iter(k2.path)

  for k1_path in k1_iter:
    k2_path = next(k2_iter, None)
    if not k2_path:
      return 1

    result = compare_path(k1_path, k2_path)

    if result != 0:
      return result

  k2_path = next(k2_iter, None)
  if k2_path:
    return -1
  else:
    return 0


def compare_path(p1, p2):
  res = str_compare(p1.kind, p2.kind)
  if res != 0:
    return res

  if p1.HasField('id'):
    if not p2.HasField('id'):
      return -1

    return p1.id - p2.id

  if p2.HasField('id'):
    return 1

  return str_compare(p1.name, p2.name)


def str_compare(s1, s2):
  if s1 == s2:
    return 0
  elif s1 < s2:
    return -1
  else:
    return 1
