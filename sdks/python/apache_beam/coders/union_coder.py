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
    self._tag_to_coders = {}
    for t, c in self._coder_registry._coders.items():
      try:
        if isinstance(t, str):
          if c and isinstance(c, type):
            c = c()
          if c.__module__ == "__main__":
            self._tag_to_coders[_get_coder_tag(t)] = c
          else:
            self._tag_to_coders[_get_coder_tag(type(c))] = c
          self._tag_to_coders[_get_coder_tag(t)] = c
        else:
          c = self._coder_registry.get_coder(t)
          self._tag_to_coders[_get_coder_tag(c.to_type_hint().__name__)] = c
      except Exception:  # pylint: disable=broad-except
        continue
    self._types_in_str = [
        c.to_type_hint().__name__ for _, c in self._tag_to_coders.items()
    ]

  def encode(self, value) -> bytes:
    """
        Encodes the given Union value into bytes.
        """
    typehint_type = type(value).__name__
    tag = _get_coder_tag(typehint_type)
    coder = self._tag_to_coders.get(tag, None)
    if coder is None:
      raise ValueError(
          "Unknown type {} for UnionCoder with {}. Expected tag is {}. Tags: {}"
          .format(typehint_type, value, tag, self._tag_to_coders))
    return tag + coder.encode(value)

  def decode(self, encoded: bytes):
    """
        Decodes the given bytes into a Union value.
        """
    tag = encoded[:2]
    coder = self._tag_to_coders.get(tag, None)

    if coder:
      return coder.decode(encoded[2:])
    else:
      raise ValueError(f"cannot decode {encoded}")

  def is_deterministic(self) -> bool:
    """
        Returns True if all sub-coders are deterministic.
        """
    return all(c.is_deterministic() for _, c in self._tag_to_coders.items())

  def to_type_hint(self) -> str:
    """
        Returns a type hint representing the Union type with the sub-coders.
        """
    return "Union[{}]".format(", ".join(self._types_in_str))

  def __repr__(self):
    """
        Returns a string representation of the coder with its sub-coders.
        """
    return "UnionCoder({})".format(", ".join(self._types_in_str))
