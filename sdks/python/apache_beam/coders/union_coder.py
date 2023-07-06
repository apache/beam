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
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple

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
    self._coder_typehints = {}  # type: Dict[Any, Tuple[bytes, Coder]]
    for i, c in enumerate(self._coders):
      type_hint = c.to_type_hint()
      if type_hint in self._coder_typehints:
        raise ValueError(
            f'One coder {self._coder_typehints.get(type_hint)} already '
            f'exists for this type {type_hint}.')
      else:
        self._coder_typehints[type_hint] = (struct.pack("B", i), c)

  def _get_coder(self, value):
    # have to linearly scan the typehints since type could be composite
    # simple type(value) does not work
    typehint_type = None
    for th in self._coder_typehints:
      try:
        typehints.check_constraint(th, value)
        typehint_type = th
        break
      except (typehints.CompositeTypeHintError, typehints.SimpleTypeHintError):
        pass
      except Exception as e:
        raise ValueError(
            f"cannot check the value {value} type and the error is {e}")

    if typehint_type in self._coder_typehints:
      return self._coder_typehints.get(typehint_type)
    raise ValueError(
        'Could not find a matching component coder '
        'in the coder {} to encode a value {} with a typehint {}.'.format(
            self, value, typehint_type))

  def encode(self, value):
    """
        Encodes the given Union value into bytes.
    """
    coder_tag, real_coder = self._get_coder(value)
    return coder_tag + real_coder.encode(value)

  def decode(self, encoded: bytes):
    """
        Decodes the given bytes into a Union value.
        """
    try:
      coder_index = struct.unpack("B", encoded[:1])[0]
      coder = self._coders[coder_index]

      return coder.decode(encoded[1:])
    except Exception:  # pylint: disable=broad-except
      raise ValueError(
          'cannot decode {!r} with the coder {}'.format(encoded, self))

  def is_deterministic(self) -> bool:
    """
        Returns True if all sub-coders are deterministic.
        """
    return all(c.is_deterministic() for c in self._coders)

  def to_type_hint(self) -> typehints.UnionConstraint:
    """
        Returns a type hint representing the Union type with the sub-coders.
        """
    return typehints.Union[list(self._coder_typehints.keys())]

  def estimate_size(self, value):
    _, real_coder = self._get_coder(value)
    return real_coder.estimate_size(value) + 1

  def coders(self):
    # type: () -> List[Coder]
    return self._coders

  def __eq__(self, other):
    return type(self) == type(other) and self._coders == other.coders()

  def __repr__(self) -> str:
    """
        Returns a string representation of the coder with its sub-coders.
        """
    return 'UnionCoder[%s]' % ', '.join(str(c) for c in self._coders)

  def __hash__(self):
    return hash(tuple(self._coders))
