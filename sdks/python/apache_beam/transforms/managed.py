from typing import Any
from typing import Dict
from typing import Optional

import yaml

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.ptransform import PTransform

ICEBERG = "iceberg"
KAFKA = "kafka"
BIGQUERY = "bigquery"
_MANAGED_IDENTIFIER = "beam:transform:managed:v1"
_GRADLE_TARGETS = {
    "sdks:java:io:expansion-service:shadowJar": [KAFKA, ICEBERG],
    "sdks:java:io:google-cloud-platform:expansion-service:shadowJar": [
        BIGQUERY
    ]
}

__all__ = ["ICEBERG", "KAFKA", "BIGQUERY", "Read", "Write"]


class _ManagedTransform(PTransform):
  def __init__(
      self,
      underlying_identifier: str,
      config: Optional[Dict[str, Any]] = None,
      config_url: Optional[str] = None,
      expansion_service=None):
    super().__init__()
    self._underlying_identifier = underlying_identifier
    self._yaml_config = yaml.dump(config)
    self._config_url = config_url
    self._expansion_service = expansion_service

  def expand(self, input):
    return input | SchemaAwareExternalTransform(
        identifier=_MANAGED_IDENTIFIER,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        transform_identifier=self._underlying_identifier,
        config=self._yaml_config,
        config_url=self._config_url)


class Read(_ManagedTransform):
  READ_TRANSFORMS = {
      ICEBERG: "beam:schematransform:org.apache.beam:iceberg_read:v1",
      KAFKA: "beam:schematransform:org.apache.beam:kafka_read:v1",
      BIGQUERY: "beam:schematransform:org.apache.beam:bigquery_storage_read:v1"
  }

  def __init__(
      self,
      source: str,
      config: Optional[Dict[str, Any]] = None,
      config_url: Optional[str] = None,
      expansion_service=None):
    self._source = source
    identifier = self.READ_TRANSFORMS.get(source.lower())
    if not identifier:
      raise ValueError(
          f"An unsupported source was specified: '{source}'. Please specify "
          f"one of the following sources: {self.READ_TRANSFORMS.keys()}")

    expansion_service = _resolve_expansion_service(
        source, identifier, expansion_service)
    super().__init__(identifier, config, config_url, expansion_service)

  def default_label(self) -> str:
    return "Managed Read(%s)" % self._source.upper()


class Write(_ManagedTransform):
  WRITE_TRANSFORMS = {
      ICEBERG: "beam:schematransform:org.apache.beam:iceberg_write:v1",
      KAFKA: "beam:schematransform:org.apache.beam:kafka_write:v1",
      BIGQUERY: "beam:schematransform:org.apache.beam:bigquery_storage_write:v2"
  }

  def __init__(
      self,
      sink: str,
      config: Optional[Dict[str, Any]] = None,
      config_url: Optional[str] = None,
      expansion_service=None):
    self._sink = sink
    identifier = self.WRITE_TRANSFORMS.get(sink.lower())
    if not identifier:
      raise ValueError(
          f"An unsupported source was specified: '{sink}'. Please specify "
          f"one of the following sources: {self.WRITE_TRANSFORMS.keys()}")

    expansion_service = _resolve_expansion_service(
        sink, identifier, expansion_service)
    super().__init__(identifier, config, config_url, expansion_service)

  def default_label(self) -> str:
    return "Managed Write(%s)" % self._sink.upper()


def _resolve_expansion_service(
    transform_name: str, identifier: str, expansion_service):
  if expansion_service:
    return expansion_service

  default_target = None
  for gradle_target, transforms in _GRADLE_TARGETS.items():
    if transform_name.lower() in transforms:
      default_target = gradle_target
      break
  if not default_target:
    raise ValueError(
        "No expansion service was specified and could not find a "
        f"default expansion service for {transform_name}: '{identifier}'.")
  return BeamJarExpansionService(default_target)
