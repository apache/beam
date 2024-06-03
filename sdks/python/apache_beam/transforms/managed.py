from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.external import BeamJarExpansionService
from typing import Any, Dict
import yaml

MANAGED_IDENTIFIER = "beam:transform:managed:v1"

class _ManagedTransform(PTransform):
  def __init__(self, underlying_identifier: str, config: Dict[str, Any] = None, config_url: str = None,
               expansion_service=None):
    super().__init__()
    self._underlying_identifier = underlying_identifier
    self._yaml_config = yaml.dump(config)
    self._config_url = config_url
    self._expansion_service = expansion_service

  def expand(self, input):
    return input | SchemaAwareExternalTransform(
      identifier=MANAGED_IDENTIFIER,
      expansion_service=self._expansion_service,
      rearrange_based_on_discovery=True,
      transform_identifier=self._underlying_identifier,
      config=self._yaml_config,
      config_url=self._config_url)


class Read(_ManagedTransform):
  READ_TRANSFORMS = {
    "iceberg": {
      "identifier": "beam:schematransform:org.apache.beam:iceberg_read:v1",
      "gradle_target": "sdks:java:io:expansion-service:shadowJar"
    },
    "kafka": {
      "identifier": "beam:schematransform:org.apache.beam:kafka_read:v1",
      "gradle_target": "sdks:java:io:expansion-service:shadowJar"
    }
  }

  def __init__(self, source: str, config: Dict[str, Any] = None, config_url: str = None, expansion_service=None):
    transform = self.READ_TRANSFORMS.get(source.lower())
    if not transform:
      raise ValueError(
        f"An unsupported source was specified: '{source}'. Please specify one of the following sources: {self.READ_TRANSFORMS.keys()}")
    expansion_service = expansion_service or BeamJarExpansionService(transform["gradle_target"])
    super().__init__(transform["identifier"], config, config_url, expansion_service)


class Write(_ManagedTransform):
  WRITE_TRANSFORMS = {
    "iceberg": {
      "identifier": "beam:schematransform:org.apache.beam:iceberg_write:v1",
      "gradle_target": "sdks:java:io:expansion-service:shadowJar"
    },
    "kafka": {
      "identifier": "beam:schematransform:org.apache.beam:kafka_write:v1",
      "gradle_target": "sdks:java:io:expansion-service:shadowJar"
    }
  }

  def __init__(self, sink: str, config: Dict[str, Any] = None, config_url: str = None, expansion_service=None):
    transform = self.WRITE_TRANSFORMS.get(sink.lower())
    if not transform:
      raise ValueError(
        f"An unsupported source was specified: '{sink}'. Please specify one of the following sources: {self.WRITE_TRANSFORMS.keys()}")
    expansion_service = expansion_service or BeamJarExpansionService(transform["gradle_target"])
    super().__init__(transform["identifier"], config, config_url, expansion_service)
