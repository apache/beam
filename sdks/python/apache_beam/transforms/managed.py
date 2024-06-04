from typing import Any
from typing import Dict

import yaml

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.ptransform import PTransform

ICEBERG = "iceberg"
KAFKA = "kafka"
_MANAGED_IDENTIFIER = "beam:transform:managed:v1"
_GRADLE_TARGETS = {"sdks:java:io:expansion-service:shadowJar": [KAFKA, ICEBERG]}

__all__ = ["ICEBERG", "KAFKA", "Read", "Write"]


# type: ignore[assignment]


class _ManagedTransform(PTransform):
  def __init__(
      self,
      underlying_identifier: str,
      config: Dict[str, Any] = None,
      config_url: str = None,
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
      KAFKA: "beam:schematransform:org.apache.beam:kafka_read:v1"
  }

  def __init__(
      self,
      source: str,
      config: Dict[str, Any] = None,
      config_url: str = None,
      expansion_service=None):
    identifier = self.READ_TRANSFORMS.get(source.lower())
    if not identifier:
      raise ValueError(
          f"An unsupported source was specified: '{source}'. Please specify "
          f"one of the following sources: {self.READ_TRANSFORMS.keys()}")

    expansion_service = _resolve_expansion_service(
        source, identifier, expansion_service)
    super().__init__(identifier, config, config_url, expansion_service)


class Write(_ManagedTransform):
  WRITE_TRANSFORMS = {
      ICEBERG: "beam:schematransform:org.apache.beam:iceberg_write:v1",
      KAFKA: "beam:schematransform:org.apache.beam:kafka_write:v1"
  }

  def __init__(
      self,
      sink: str,
      config: Dict[str, Any] = None,
      config_url: str = None,
      expansion_service=None):
    identifier = self.WRITE_TRANSFORMS.get(sink.lower())
    if not identifier:
      raise ValueError(
          f"An unsupported source was specified: '{sink}'. Please specify "
          f"one of the following sources: {self.WRITE_TRANSFORMS.keys()}")

    expansion_service = _resolve_expansion_service(
        sink, identifier, expansion_service)
    super().__init__(identifier, config, config_url, expansion_service)


def _resolve_expansion_service(
    transform_name: str, identifier: str, expansion_service):
  if expansion_service:
    return expansion_service

  default_target = None
  for gradle_target in _GRADLE_TARGETS:
    if transform_name in gradle_target:
      default_target = gradle_target
      break
  if not default_target:
    raise ValueError(
        "No expansion service was specified and could not find a "
        f"default expansion service for {transform_name}: '{identifier}'.")
  return BeamJarExpansionService(default_target)
