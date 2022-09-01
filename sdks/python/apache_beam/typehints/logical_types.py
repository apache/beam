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

"""Standard and common logical type implementations.

This module is intended for internal use only. Nothing defined here provides
any backwards-compatibility guarantee.
"""

from typing import NamedTuple

import numpy as np

from apache_beam.portability import common_urns
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import NoArgumentLogicalType
from apache_beam.utils.python_callable import PythonCallableWithSource
from apache_beam.utils.timestamp import Timestamp

MicrosInstantRepresentation = NamedTuple(
    'MicrosInstantRepresentation', [('seconds', np.int64),
                                    ('micros', np.int64)])


@LogicalType.register_logical_type
class MillisInstant(NoArgumentLogicalType[Timestamp, np.int64]):
  """Millisecond-precision instant logical type handles values consistent with
  that encoded by ``InstantCoder`` in the Java SDK.

  This class handles :class:`apache_beam.utils.timestamp.Timestamp` language
  type as :class:`MicrosInstant`, but it only provides millisecond precision,
  because it is aimed to handle data encoded by Java sdk's InstantCoder which
  has same precision level.

  Timestamp is handled by `MicrosInstant` by default. In some scenario, such as
  read from cross-language transform with rows containing InstantCoder encoded
  timestamps, one may need to override the mapping of Timetamp to MillisInstant.
  To do this, re-register this class with
  :func:`~apache_beam.typehints.schemas.LogicalType.register_logical_type`.
  """
  @classmethod
  def representation_type(cls):
    # type: () -> type
    return np.int64

  @classmethod
  def urn(cls):
    return common_urns.millis_instant.urn

  @classmethod
  def language_type(cls):
    return Timestamp

  def to_language_type(self, value):
    # type: (np.int64) -> Timestamp

    # value shifted as in apache_beams.coders.coder_impl.TimestampCoderImpl
    if value < 0:
      millis = int(value) + (1 << 63)
    else:
      millis = int(value) - (1 << 63)

    return Timestamp(micros=millis * 1000)


# Make sure MicrosInstant is registered after MillisInstant so that it
# overwrites the mapping of Timestamp language type representation choice and
# thus does not lose microsecond precision inside python sdk.
@LogicalType.register_logical_type
class MicrosInstant(NoArgumentLogicalType[Timestamp,
                                          MicrosInstantRepresentation]):
  """Microsecond-precision instant logical type that handles ``Timestamp``."""
  @classmethod
  def urn(cls):
    return common_urns.micros_instant.urn

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return MicrosInstantRepresentation

  @classmethod
  def language_type(cls):
    return Timestamp

  def to_representation_type(self, value):
    # type: (Timestamp) -> MicrosInstantRepresentation
    return MicrosInstantRepresentation(
        value.micros // 1000000, value.micros % 1000000)

  def to_language_type(self, value):
    # type: (MicrosInstantRepresentation) -> Timestamp
    return Timestamp(seconds=int(value.seconds), micros=int(value.micros))


@LogicalType.register_logical_type
class PythonCallable(NoArgumentLogicalType[PythonCallableWithSource, str]):
  """A logical type for PythonCallableSource objects."""
  @classmethod
  def urn(cls):
    return common_urns.python_callable.urn

  @classmethod
  def representation_type(cls):
    # type: () -> type
    return str

  @classmethod
  def language_type(cls):
    return PythonCallableWithSource

  def to_representation_type(self, value):
    # type: (PythonCallableWithSource) -> str
    return value.get_source()

  def to_language_type(self, value):
    # type: (str) -> PythonCallableWithSource
    return PythonCallableWithSource(value)
