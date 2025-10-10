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

Available transforms
====================
Please check the Managed IO configuration page:
https://beam.apache.org/documentation/io/managed-io/

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


**Note:** inputs and outputs need to be PCollection(s) of Beam
:py:class:`apache_beam.pvalue.Row` elements.

Runner specific features
========================
Google Cloud Dataflow supports additional management features for `managed`
including automatically upgrading transforms to the latest supported version.
For more details and examples, please see
Dataflow managed I/O https://cloud.google.com/dataflow/docs/guides/managed-io.
"""

from typing import Any
from typing import Optional

import yaml

from apache_beam.portability.common_urns import ManagedTransforms
from apache_beam.transforms.external import MANAGED_SCHEMA_TRANSFORM_IDENTIFIER
from apache_beam.transforms.external import MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.ptransform import PTransform

ICEBERG = "iceberg"
# TODO(https://github.com/apache/beam/issues/34212): keep ICEBERG_CDC private
#  until we vet it with integration tests
_ICEBERG_CDC = "iceberg_cdc"
KAFKA = "kafka"
BIGQUERY = "bigquery"
POSTGRES = "postgres"
MYSQL = "mysql"
SQL_SERVER = "sqlserver"

__all__ = ["ICEBERG", "KAFKA", "BIGQUERY", "Read", "Write"]


class Read(PTransform):
  """Read using Managed Transforms"""
  _READ_TRANSFORMS = {
      ICEBERG: ManagedTransforms.Urns.ICEBERG_READ.urn,
      _ICEBERG_CDC: ManagedTransforms.Urns.ICEBERG_CDC_READ.urn,
      KAFKA: ManagedTransforms.Urns.KAFKA_READ.urn,
      BIGQUERY: ManagedTransforms.Urns.BIGQUERY_READ.urn,
      POSTGRES: ManagedTransforms.Urns.POSTGRES_READ.urn,
      MYSQL: ManagedTransforms.Urns.MYSQL_READ.urn,
      SQL_SERVER: ManagedTransforms.Urns.SQL_SERVER_READ.urn,
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

    # Store parameters for deferred expansion service creation
    self._identifier = identifier
    self._provided_expansion_service = expansion_service
    self._underlying_identifier = identifier
    self._yaml_config = yaml.dump(config)
    self._config_url = config_url

  def expand(self, input):
    # Create expansion service with access to pipeline options
    expansion_service = _resolve_expansion_service(
        self._source,
        self._identifier,
        self._provided_expansion_service,
        pipeline_options=input.pipeline._options)

    return input | SchemaAwareExternalTransform(
        identifier=MANAGED_SCHEMA_TRANSFORM_IDENTIFIER,
        expansion_service=expansion_service,
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
      BIGQUERY: ManagedTransforms.Urns.BIGQUERY_WRITE.urn,
      POSTGRES: ManagedTransforms.Urns.POSTGRES_WRITE.urn,
      MYSQL: ManagedTransforms.Urns.MYSQL_WRITE.urn,
      SQL_SERVER: ManagedTransforms.Urns.SQL_SERVER_WRITE.urn
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

    # Store parameters for deferred expansion service creation
    self._identifier = identifier
    self._provided_expansion_service = expansion_service
    self._underlying_identifier = identifier
    self._yaml_config = yaml.dump(config)
    self._config_url = config_url

  def expand(self, input):
    # Create expansion service with access to pipeline options
    expansion_service = _resolve_expansion_service(
        self._sink,
        self._identifier,
        self._provided_expansion_service,
        pipeline_options=input.pipeline._options)

    return input | SchemaAwareExternalTransform(
        identifier=MANAGED_SCHEMA_TRANSFORM_IDENTIFIER,
        expansion_service=expansion_service,
        rearrange_based_on_discovery=True,
        transform_identifier=self._underlying_identifier,
        config=self._yaml_config,
        config_url=self._config_url)

  def default_label(self) -> str:
    return "Managed Write(%s)" % self._sink.upper()


def _resolve_expansion_service(
    transform_name: str,
    identifier: str,
    expansion_service,
    pipeline_options=None):
  if expansion_service:
    return expansion_service

  gradle_target = None
  if identifier in MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING:
    gradle_target = MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING.get(identifier)
  if not gradle_target:
    raise ValueError(
        "No expansion service was specified and could not find a "
        f"default expansion service for {transform_name}: '{identifier}'.")

  # Extract maven_repository_url and user_agent from pipeline options if
  # available
  maven_repository_url = None
  user_agent = None
  if pipeline_options:
    from apache_beam.options import pipeline_options as po
    setup_options = pipeline_options.view_as(po.SetupOptions)
    maven_repository_url = setup_options.maven_repository_url
    user_agent = setup_options.user_agent

  return BeamJarExpansionService(
      gradle_target,
      maven_repository_url=maven_repository_url,
      user_agent=user_agent)
