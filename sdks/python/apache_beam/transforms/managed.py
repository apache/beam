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

"""Managed Transforms.

This module builds and instantiates turnkey transforms that can be managed by
the underlying runner. This means the runner can upgrade the transform to a
more optimal/updated version without requiring the user to do anything. It may
also replace the transform with something entirely different if it chooses to.
By default, however, the specified transform will remain unchanged.

Using Managed Transforms
========================
Managed turnkey transforms have a defined configuration and can be built using
an inline :class:`dict` like so::

  results = p | beam.managed.Read(
                    beam.managed.ICEBERG,
                    config={"table": "foo",
                            "catalog_name": "bar",
                            "catalog_properties": {
                                "warehouse": "path/to/warehouse",
                                "catalog-impl": "org.apache.my.CatalogImpl"}})

A YAML configuration file can also be used to build a Managed transform. Say we
have the following `config.yaml` file::

  topic: "foo"
  bootstrap_servers: "localhost:1234"
  format: "AVRO"

Simply provide the location to the file like so::

  input_rows = p | beam.Create(...)
  input_rows | beam.managed.Write(
                    beam.managed.KAFKA,
                    config_url="path/to/config.yaml")

Available transforms
====================
Available transforms are:

- **Kafka Read and Write**
- **Iceberg Read and Write**

**Note:** inputs and outputs need to be PCollection(s) of Beam
:py:class:`apache_beam.pvalue.Row` elements.

**Note:** Today, all managed transforms are essentially cross-language
transforms, and Java's ManagedSchemaTransform is used under the hood.
"""

from typing import Any
from typing import Optional

import yaml

from apache_beam.portability.common_urns import ManagedTransforms
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.ptransform import PTransform

ICEBERG = "iceberg"
KAFKA = "kafka"
_MANAGED_IDENTIFIER = "beam:transform:managed:v1"
_EXPANSION_SERVICE_JAR_TARGETS = {
    "sdks:java:io:expansion-service:shadowJar": [KAFKA, ICEBERG],
}

__all__ = ["ICEBERG", "KAFKA", "Read", "Write"]


class Read(PTransform):
  """Read using Managed Transforms"""
  _READ_TRANSFORMS = {
      ICEBERG: ManagedTransforms.Urns.ICEBERG_READ.urn,
      KAFKA: ManagedTransforms.Urns.KAFKA_READ.urn,
  }

  def __init__(
      self,
      source: str,
      config: Optional[dict[str, Any]] = None,
      config_url: Optional[str] = None,
      expansion_service=None):
    super().__init__()
    self._source = source
    identifier = self._READ_TRANSFORMS.get(source.lower())
    if not identifier:
      raise ValueError(
          f"An unsupported source was specified: '{source}'. Please specify "
          f"one of the following sources: {list(self._READ_TRANSFORMS.keys())}")

    self._expansion_service = _resolve_expansion_service(
        source, identifier, expansion_service)
    self._underlying_identifier = identifier
    self._yaml_config = yaml.dump(config)
    self._config_url = config_url

  def expand(self, input):
    return input | SchemaAwareExternalTransform(
        identifier=_MANAGED_IDENTIFIER,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        transform_identifier=self._underlying_identifier,
        config=self._yaml_config,
        config_url=self._config_url)

  def default_label(self) -> str:
    return "Managed Read(%s)" % self._source.upper()


class Write(PTransform):
  """Write using Managed Transforms"""
  _WRITE_TRANSFORMS = {
      ICEBERG: ManagedTransforms.Urns.ICEBERG_WRITE.urn,
      KAFKA: ManagedTransforms.Urns.KAFKA_WRITE.urn,
  }

  def __init__(
      self,
      sink: str,
      config: Optional[dict[str, Any]] = None,
      config_url: Optional[str] = None,
      expansion_service=None):
    super().__init__()
    self._sink = sink
    identifier = self._WRITE_TRANSFORMS.get(sink.lower())
    if not identifier:
      raise ValueError(
          f"An unsupported sink was specified: '{sink}'. Please specify "
          f"one of the following sinks: {list(self._WRITE_TRANSFORMS.keys())}")

    self._expansion_service = _resolve_expansion_service(
        sink, identifier, expansion_service)
    self._underlying_identifier = identifier
    self._yaml_config = yaml.dump(config)
    self._config_url = config_url

  def expand(self, input):
    return input | SchemaAwareExternalTransform(
        identifier=_MANAGED_IDENTIFIER,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        transform_identifier=self._underlying_identifier,
        config=self._yaml_config,
        config_url=self._config_url)

  def default_label(self) -> str:
    return "Managed Write(%s)" % self._sink.upper()


def _resolve_expansion_service(
    transform_name: str, identifier: str, expansion_service):
  if expansion_service:
    return expansion_service

  default_target = None
  for gradle_target, transforms in _EXPANSION_SERVICE_JAR_TARGETS.items():
    if transform_name.lower() in transforms:
      default_target = gradle_target
      break
  if not default_target:
    raise ValueError(
        "No expansion service was specified and could not find a "
        f"default expansion service for {transform_name}: '{identifier}'.")
  return BeamJarExpansionService(default_target)
