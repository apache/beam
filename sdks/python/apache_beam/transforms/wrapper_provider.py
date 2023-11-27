import logging

from apache_beam.transforms import PTransform
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.external import SchemaTransformsConfig
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.schemas import typing_from_runner_api

def snake_case_to_upper_camel_case(string):
  """Convert snake_case to UpperCamelCase"""
  components = string.split('_')
  output = ''.join(n.capitalize() for n in components)
  return output

def snake_case_to_lower_camel_case(string):
  """Convert snake_case to lowerCamelCase"""
  if len(string) <= 1:
    return string.lower()
  upper = snake_case_to_upper_camel_case(string)
  return upper[0].lower() + upper[1:]

def camel_case_to_snake_case(string):
  """Convert camelCase to snake_case"""
  arr = ['_' + n.lower() if n.isupper() else n for n in string]
  return ''.join(arr).lstrip('_')

def get_config_with_descriptions(schematransform: SchemaTransformsConfig):
  # Prepare a configuration schema that includes types and descriptions
  schema = named_tuple_to_schema(schematransform.configuration_schema)
  descriptions = schematransform.configuration_schema._field_descriptions
  fields_with_descriptions = {}
  for field in schema.fields:
    fields_with_descriptions[camel_case_to_snake_case(field.name)] = (typing_from_runner_api(field.type), descriptions[field.name], field.name)

  return fields_with_descriptions

STANDARD_EXPANSION_SERVICES = [
  BeamJarExpansionService('sdks:java:io:google-cloud-platform:expansion-service:build'),
  BeamJarExpansionService('sdks:java:io:expansion-service:build')
]

class Wrapper(PTransform):
  """Template for a SchemaTransform Python wrappeer"""

  # These attributes need to be set when a Wrapper type is created
  default_expansion_service = None
  identifier = None

  def __init__(self, expansion_service=None, **kwargs):
    self._kwargs = kwargs
    self._expansion_service = expansion_service or self.default_expansion_service
    self.schematransform: SchemaTransformsConfig = SchemaAwareExternalTransform.discover_config(self._expansion_service, self.identifier)

  @property
  def configuration_schema(self):
    """Returns a configuration schema that includes
    field names, types, and descriptions"""
    return get_config_with_descriptions(self.schematransform)

  def expand(self, input):
    camel_case_kwargs = {snake_case_to_lower_camel_case(k): v for k, v in self._kwargs.items()}

    external_schematransform = SchemaAwareExternalTransform(
      identifier=self.schematransform.identifier,
      expansion_service=self._expansion_service,
      rearrange_based_on_discovery=True,
      **camel_case_kwargs)

    input_tags = self.schematransform.inputs
    # TODO(ahmedabu98): how do we handle the case of multiple input pcolls?
    if input_tags and len(input_tags) == 1:
      return {input_tags[0]: input} | external_schematransform
    else:
      return input.pipeline | external_schematransform


class WrapperProvider:
  def __init__(self, additional_services=None):
    self.wrappers = {}
    self.urn_to_wrapper_name = {}

    if additional_services is None:
      additional_services = []
    if isinstance(additional_services, set):
      additional_services = list(additional_services)
    if not isinstance(additional_services, list):
      additional_services = [additional_services]

    # use standard and add additional expansion services
    self.expansion_services = list(set(STANDARD_EXPANSION_SERVICES + additional_services))

  def _create_wrappers(self):
    # multiple services can overlap and include the same URNs. If this happens, we prioritize
    # by the order of services in the list
    identifiers = set()
    for service in self.expansion_services:
      schematransform_configs = SchemaAwareExternalTransform.discover(service)

      for config in schematransform_configs:
        if config.identifier not in identifiers:
          identifiers.add(config.identifier)
          identifier_components = config.identifier.split(':')
          # We expect URNs like `beam:schematransform:org.apache.beam:my_transform:v1`
          if len(identifier_components) != 5:
            logging.warning("Skipping unexpected URN %s, please follow the "
                            "standard in https://beam.apache.org/documentation/programming-guide/#1314-defining-a-urn", config.identifier)
            continue
          name = snake_case_to_upper_camel_case(identifier_components[3])

          # add version number if there are multiple versions
          version = identifier_components[4]
          if version != 'v1':
            name += version.capitalize()

          self.wrappers[name] = type(name, (Wrapper,), dict(identifier=config.identifier, default_expansion_service=service, schematransform=config))
          self.urn_to_wrapper_name[config.identifier] = name

    for name, wrapper in self.wrappers.items():
      setattr(self, name, wrapper)


  def get_available(self):
    """Get a set of all available wrappers (by name)"""
    self._maybe_create_wrappers()
    return set(self.wrappers.keys())

  def get(self, name):
    """Get a wrapper by its name"""
    self._maybe_create_wrappers()
    return self.wrappers[name]

  def get_urn(self, identifier):
    """Get a wrapper by its URN"""
    self._maybe_create_wrappers()
    return self.wrappers[self.urn_to_wrapper_name[identifier]]

  def _maybe_create_wrappers(self):
    if not self.wrappers:
      self._create_wrappers()
