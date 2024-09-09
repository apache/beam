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

import re
from typing import Any
from typing import Dict
from typing import Mapping
from typing import Optional

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.portability.common_urns import resource_hints

__all__ = [
    'ResourceHint',
    'AcceleratorHint',
    'MinRamHint',
    'CpuCountHint',
    'merge_resource_hints',
    'parse_resource_hints',
    'resource_hints_from_options',
]


class ResourceHint:
  """A superclass to define resource hints."""
  # A unique URN, one per Resource Hint class.
  urn: Optional[str] = None

  _urn_to_known_hints: Dict[str, type] = {}
  _name_to_known_hints: Dict[str, type] = {}

  @classmethod
  def parse(cls, value: str) -> Dict[str, bytes]:
    """Describes how to parse the hint.
    Override to specify a custom parsing logic."""
    assert cls.urn is not None
    # Override this method to have a custom parsing logic.
    return {cls.urn: ResourceHint._parse_str(value)}

  @classmethod
  def get_merged_value(cls, outer_value: bytes, inner_value: bytes) -> bytes:
    """Reconciles values of a hint when the hint specified on a transform is
    also defined in an outer context, for example on a composite transform, or
    specified in the transform's execution environment.
    Override to specify a custom merging logic.
    """
    # Defaults to the inner value as it is the most specific one.
    return inner_value

  @staticmethod
  def get_by_urn(urn):
    return ResourceHint._urn_to_known_hints[urn]

  @staticmethod
  def get_by_name(name):
    return ResourceHint._name_to_known_hints[name]

  @staticmethod
  def is_registered(name):
    return name in ResourceHint._name_to_known_hints

  @staticmethod
  def register_resource_hint(hint_name: str, hint_class: type) -> None:
    assert issubclass(hint_class, ResourceHint)
    assert hint_class.urn is not None
    ResourceHint._name_to_known_hints[hint_name] = hint_class
    ResourceHint._urn_to_known_hints[hint_class.urn] = hint_class

  @staticmethod
  def _parse_str(value):
    if not isinstance(value, str):
      raise ValueError("Input must be a string.")
    return value.encode('ascii')

  @staticmethod
  def _parse_int(value):
    if isinstance(value, str):
      value = int(value)
    if not isinstance(value, int):
      raise ValueError("Input must be an integer.")
    return str(value).encode('ascii')

  @staticmethod
  def _parse_storage_size_str(value):
    """Parses a human-friendly storage size string into a number of bytes.
    """
    if isinstance(value, int):
      return ResourceHint._parse_int(value)

    if not isinstance(value, str):
      raise ValueError("Input must be a string or integer.")

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
        'B': 1,
    }
    match = re.match(r'.*?(\D+)$', value)
    if not match:
      raise ValueError("Unrecognized value pattern.")

    suffix = match.group(1)
    if suffix not in units:
      raise ValueError("Unrecognized unit.")
    multiplier = units[suffix]
    value = value[:-len(suffix)]

    return str(round(float(value) * multiplier)).encode('ascii')

  @staticmethod
  def _use_max(v1, v2):
    return str(max(int(v1), int(v2))).encode('ascii')


class AcceleratorHint(ResourceHint):
  """Describes desired hardware accelerators in execution environment."""
  urn = resource_hints.ACCELERATOR.urn


ResourceHint.register_resource_hint('accelerator', AcceleratorHint)


class MinRamHint(ResourceHint):
  """Describes min RAM requirements for transform's execution environment."""
  urn = resource_hints.MIN_RAM_BYTES.urn

  @classmethod
  def parse(cls, value: str) -> Dict[str, bytes]:
    return {cls.urn: ResourceHint._parse_storage_size_str(value)}

  @classmethod
  def get_merged_value(cls, outer_value: bytes, inner_value: bytes) -> bytes:
    return ResourceHint._use_max(outer_value, inner_value)


ResourceHint.register_resource_hint('min_ram', MinRamHint)
# Alias for interoperability with SDKs preferring camelCase.
ResourceHint.register_resource_hint('minRam', MinRamHint)


class CpuCountHint(ResourceHint):
  """Describes number of CPUs available in transform's execution environment."""
  urn = resource_hints.CPU_COUNT.urn

  @classmethod
  def get_merged_value(cls, outer_value: bytes, inner_value: bytes) -> bytes:
    return ResourceHint._use_max(outer_value, inner_value)


ResourceHint.register_resource_hint('cpu_count', CpuCountHint)
# Alias for interoperability with SDKs preferring camelCase.
ResourceHint.register_resource_hint('cpuCount', CpuCountHint)


def parse_resource_hints(hints: Dict[Any, Any]) -> Dict[str, bytes]:
  parsed_hints = {}
  for hint, value in hints.items():
    try:
      hint_cls = ResourceHint.get_by_name(hint)
      try:
        parsed_hints.update(hint_cls.parse(value))
      except ValueError:
        raise ValueError(f"Resource hint {hint} has invalid value {value}.")
    except KeyError:
      raise ValueError(f"Unknown resource hint: {hint}.")

  return parsed_hints


def resource_hints_from_options(
    options: Optional[PipelineOptions]) -> Dict[str, bytes]:
  if options is None:
    return {}
  hints = {}
  option_specified_hints = options.view_as(StandardOptions).resource_hints
  for hint in option_specified_hints:
    if '=' in hint:
      k, v = hint.split('=', maxsplit=1)
      hints[k] = v
    else:
      hints[hint] = None

  return parse_resource_hints(hints)


def merge_resource_hints(
    outer_hints: Mapping[str, bytes],
    inner_hints: Mapping[str, bytes]) -> Dict[str, bytes]:
  merged_hints = dict(inner_hints)
  for urn, outer_value in outer_hints.items():
    if urn in inner_hints:
      merged_value = ResourceHint.get_by_urn(urn).get_merged_value(
          outer_value=outer_value, inner_value=inner_hints[urn])
    else:
      merged_value = outer_value
    merged_hints[urn] = merged_value
  return merged_hints
