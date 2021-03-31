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

"""A module for defining resource requirements for execution of transforms."""

from typing import Any
from typing import Dict

from apache_beam.portability.common_urns import resource_hints

__all__ = ['parse_resource_hints']


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


def _parse_identity(value):
  return value.encode('ascii')


# Describes how to parse known resource hints, and which URNs to assign.
_KNOWN_HINTS = dict(
    accelerator_type=lambda value:
    {resource_hints.ACCELERATOR_TYPE.urn: _parse_str(value)},
    accelerator_count=lambda value:
    {resource_hints.ACCELERATOR_COUNT.urn: _parse_int(value)},
    accelerator_metadata=lambda value:
    {resource_hints.ACCELERATOR_METADATA.urn: _parse_str(value)},
)


def parse_resource_hints(hints):
  # type: (Dict[Any, Any]) -> Dict[str, bytes]
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
