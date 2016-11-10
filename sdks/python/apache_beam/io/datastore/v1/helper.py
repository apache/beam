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
  """A comparator for Datastore keys.

  Comparison is only valid for keys in the same partition. The comparison here
  is between the list of paths for each key.
  """

  if k1.partition_id != k2.partition_id:
    raise ValueError('Cannot compare keys with different partition ids.')

  k2_iter = iter(k2.path)

  for k1_path in k1.path:
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
  """A comparator for key path.

  A path has either an `id` or a `name` field defined. The
  comparison works with the following rules:

  1. If one path has `id` defined while the other doesn't, then the
  one with `id` defined is considered smaller.
  2. If both paths have `id` defined, then their ids are compared.
  3. If no `id` is defined for both paths, then their `names` are compared.
  """

  result = str_compare(p1.kind, p2.kind)
  if result != 0:
    return result

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
