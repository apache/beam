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

"""A module for defining resource requirements for execution of transforms.

Pipeline authors can use resource hints to provide additional information to
runners about the desired aspects of the execution environment.

Resource hints can be specified on a transform level for parts of the pipeline,
or globally via --resource_hint pipeline option.

See also: PTransforms.with_resource_hints().
"""

from typing import Any
from typing import Callable
from typing import Dict

from apache_beam.portability.common_urns import resource_hints

__all__ = ['parse_resource_hints', 'get_merged_hint_value']


def _parse_str(value):
  if not isinstance(value, str):
    raise ValueError()
  return value.encode('ascii')


def _parse_int(value):
  if isinstance(value, str):
    value = int(value)
  if not isinstance(value, int):
    raise ValueError()
  return str(value).encode('ascii')


def _parse_any(_):
  # For hints where only a key is relevant and value is set to None or any value
  return b'1'


def _parse_storage_size_str(value):  # type: (str) -> bytes
  """Parses a human-friendly storage size string into a number of bytes.
  """
  if not isinstance(value, str):
    value = str(value)
  value = value.strip().replace(" ", "")
  units = {
      'PiB': 2**50,
      'TiB': 2**40,
      'GiB': 2**30,
      'MiB': 2**20,
      'KiB': 2**10,
      'PB': 10**15,
      'TB': 10**12,
      'GB': 10**9,
      'MB': 10**6,
      'KB': 10**3,
  }
  multiplier = 1
  for suffix in units:
    if value.endswith(suffix):
      multiplier = units[suffix]
      value = value[:-len(suffix)]
      break

  return str(round(float(value) * multiplier)).encode('ascii')


def _use_max(v1, v2):
  return str(max(int(v1), int(v2))).encode('ascii')


def get_merged_hint_value(
    hint_urn, outer_value, inner_value):  # type: (str, bytes, bytes) -> bytes
  """Reconciles values of a hint defined on a composite and its subtransform."""
  if (outer_value == inner_value or
      hint_urn not in _HINTS_WITH_CUSTOM_MERGING_LOGIC):
    return outer_value
  else:
    return _HINTS_WITH_CUSTOM_MERGING_LOGIC[hint_urn](outer_value, inner_value)


# Describes how to parse known resource hints, and which URNs to assign.
_KNOWN_HINTS = dict(
    accelerator=lambda value:
    {resource_hints.ACCELERATOR.urn: _parse_str(value)},
    min_ram_per_vcpu=lambda value:
    {resource_hints.MIN_RAM_PER_VCPU_BYTES.urn: _parse_storage_size_str(value)},
)  # type: Dict[str, Callable[[Any], Dict[str, bytes]]]

# Describes how resource hint values should be reconciled when the same hint
# is defined on a composite transform and its downstream parts.
# Note that hint values predefined by environments (such as values of
# command-line specified hints) will override pipeline-defined hints and are not
# subject to the merging logic.
_HINTS_WITH_CUSTOM_MERGING_LOGIC = {
    resource_hints.MIN_RAM_PER_VCPU_BYTES.urn: _use_max
}  # type: Dict[str, Callable[[bytes, bytes], bytes]]


def parse_resource_hints(hints):  # type: (Dict[Any, Any]) -> Dict[str, bytes]
  parsed_hints = {}
  for hint, value in hints.items():
    try:
      hint_parser = _KNOWN_HINTS[hint]
      try:
        parsed_hints.update(hint_parser(value))
      except ValueError:
        raise ValueError(f"Resource hint {hint} has invalid value {value}.")
    except KeyError:
      raise ValueError(f"Unknown resource hint: {hint}.")

  return parsed_hints
