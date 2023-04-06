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

import hashlib

from apache_beam.coders import coders
from apache_beam.coders.typecoders import registry


def _get_coder_tag(c):
  # only use two bytes from the hash value
  hf = hashlib.sha256()
  hf.update(str(c).encode())
  return hf.digest()[0:2]


class UnionCoder(coders.FastCoder):
  def __init__(self):
    # assuming all custom coders are registered
    self._coder_registry = registry
    self._tag_to_type = {
        _get_coder_tag(str(t)): t
        for t in self._coder_registry._coders
    }

  def encode(self, value) -> bytes:
    """
      Encodes the given Union value into bytes.
      """
    coder = self._coder_registry.get_coder(type(value))
    if coder is None:
      raise ValueError(
          "Unknown type {} for UnionCoder with {}".format(type(value), value))

    return _get_coder_tag(coder.to_type_hint()) + coder.encode(value)

  def decode(self, encoded: bytes):
    """
      Decodes the given bytes into a Union value.
      """
    tag = encoded[:2]
    value_type = self._tag_to_type.get(tag, None)
    if value_type:
      coder = self._coder_registry.get_coder(value_type)
      if coder is None:
        raise ValueError(
            "Unknown tag for UnionCoder: {} with type {}".format(
                tag, value_type))
    return coder.decode(encoded[2:])

  def is_deterministic(self) -> bool:
    """
      Returns True if all sub-coders are deterministic.
      """
    return all(
        coder.is_deterministic() for _, coder in self._coders_for_tags.items())

  def to_type_hint(self) -> str:
    """
      Returns a type hint representing the Union type with the sub-coders.
      """
    return "Union[{}]".format(", ".join(self._coders_in_str))

  def __repr__(self):
    """
      Returns a string representation of the coder with its sub-coders.
      """
    return "UnionCoder({})".format(self._coders_in_str)
