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

from apache_beam.coders import coders

# define unique tags for the supported types
SUPPORTED_CODERS_FOR_UNION = [
    ("I ", coders.VarIntCoder),
    ("F ", coders.FloatCoder),
    ("B ", coders.BytesCoder),
    ("BL", coders.BooleanCoder),
    ("S ", coders.StrUtf8Coder),
]


class UnionCoder(coders.FastCoder):
  def __init__(self):
    self._coders = {
        c().to_type_hint(): (t, c())
        for t, c in SUPPORTED_CODERS_FOR_UNION
    }
    self._coders_for_tags = {t: c() for t, c in SUPPORTED_CODERS_FOR_UNION}
    self._coders_in_str = [
        c().to_type_hint().__name__ for _, c in SUPPORTED_CODERS_FOR_UNION
    ]

  def encode(self, value) -> bytes:
    """
      Encodes the given Union value into bytes.
      """
    coder = self._coders.get(type(value), None)
    if coder is None:
      raise ValueError(
          "Unknown type {} for UnionCoder with {}".format(type(value), value))

    return str(coder[0]).encode("utf-8") + coder[1].encode(value)

  def decode(self, encoded: bytes):
    """
      Decodes the given bytes into a Union value.
      """
    tag = encoded[:2].decode("utf-8")
    coder = self._coders_for_tags.get(tag, None)
    if coder is None:
      raise ValueError("Unknown tag for UnionCoder: {}".format(tag))
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
