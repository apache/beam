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


class ShardedKey(object):
  """
  A sharded key consisting of a user key and an opaque shard id represented by
  bytes.

  Attributes:
    key: The user key.
    shard_id: An opaque byte string that uniquely represents a shard of the key.
  """
  def __init__(
      self,
      key,
      shard_id: bytes,
  ) -> None:
    assert shard_id is not None
    self._key = key
    self._shard_id = shard_id

  @property
  def key(self):
    return self._key

  def __repr__(self):
    return '(%s, %s)' % (repr(self.key), self._shard_id)

  def __eq__(self, other):
    return (
        type(self) == type(other) and self.key == other.key and
        self._shard_id == other._shard_id)

  def __hash__(self):
    return hash((self.key, self._shard_id))

  def __reduce__(self):
    return ShardedKey, (self.key, self._shard_id)
