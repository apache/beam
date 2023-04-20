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

import struct

from typing import Iterable

from apache_beam.coders import Coder
from apache_beam.coders.coders import FastCoder
from apache_beam.typehints import typehints


class UnionCoder(FastCoder):
  def __init__(self, components):
    # type: (Iterable[Coder]) -> None
    if not components or not isinstance(components, list):
      raise ValueError('A valid list of Coders must be provided.')

    if len(components) > 255 or len(components) <= 1:
      raise ValueError(
          'The number of components for UnionCoder must be between 2 and 255.')

    self._coders = components
    self._coder_typehints = [c.to_type_hint() for c in self._coders]

  def encode(self, value) -> bytes:
    """
        Encodes the given Union value into bytes.
        """
    typehint_type = type(value)
    if typehint_type in self._coder_typehints:
      coder_index = self._coder_typehints.index(typehint_type)
      coder = self._coders[coder_index]
      return struct.pack("B", coder_index) + coder.encode(value)
    else:
      raise ValueError(
          "Unknown type {} for UnionCoder with the value {}. ".format(
              typehint_type, value))

  def decode(self, encoded: bytes):
    """
        Decodes the given bytes into a Union value.
        """
    try:
      coder_index = struct.unpack("B", encoded[:1])[0]
      coder = self._coders[coder_index]

      return coder.decode(encoded[1:])
    except Exception:  # pylint: disable=broad-except
      raise ValueError(f"cannot decode {encoded}")

  def is_deterministic(self) -> bool:
    """
        Returns True if all sub-coders are deterministic.
        """
    return all(c.is_deterministic() for c in self._coders)

  def to_type_hint(self) -> typehints.UnionConstraint:
    """
        Returns a type hint representing the Union type with the sub-coders.
        """
    return typehints.Union[self._coder_typehints]

  def __repr__(self) -> str:
    """
        Returns a string representation of the coder with its sub-coders.
        """
    return 'UnionCoder[%s]' % ', '.join(str(c) for c in self._coders)
