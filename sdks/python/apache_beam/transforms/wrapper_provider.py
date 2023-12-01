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

import logging
import typing
from collections import namedtuple

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


# Information regarding a Wrapper parameter.
ParamInfo = namedtuple('ParamInfo', ['type', 'description', 'original_name'])


def get_config_with_descriptions(schematransform: SchemaTransformsConfig):
  # Prepare a configuration schema that includes types and descriptions
  schema = named_tuple_to_schema(schematransform.configuration_schema)
  descriptions = schematransform.configuration_schema._field_descriptions
  fields_with_descriptions = {}
  for field in schema.fields:
    fields_with_descriptions[camel_case_to_snake_case(field.name)] = ParamInfo(
        typing_from_runner_api(field.type),
        descriptions[field.name],
        field.name)

  return fields_with_descriptions


class Wrapper(PTransform):
  """Template for a SchemaTransform Python wrappeer"""

  # These attributes need to be set when a Wrapper type is created
  default_expansion_service = None
  identifier = None

  def __init__(self, expansion_service=None, **kwargs):
    self._kwargs = kwargs
    self._expansion_service = \
        expansion_service or self.default_expansion_service
    self.schematransform: SchemaTransformsConfig = \
      SchemaAwareExternalTransform.discover_config(
        self._expansion_service, self.identifier)

  def expand(self, input):
    camel_case_kwargs = {
        snake_case_to_lower_camel_case(k): v
        for k, v in self._kwargs.items()
    }

    external_schematransform = SchemaAwareExternalTransform(
        identifier=self.identifier,
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
  def __init__(self, expansion_services=None):
    self.wrappers = {}
    self.urn_to_wrapper_name = {}

    if expansion_services is None:
      expansion_services = []
    if isinstance(expansion_services, set):
      expansion_services = list(expansion_services)
    if not isinstance(expansion_services, list):
      expansion_services = [expansion_services]
    self.expansion_services = expansion_services

  def _create_wrappers(self):
    # multiple services can overlap and include the same URNs. If this happens,
    # we prioritize by the order of services in the list
    identifiers = set()
    for service in self.expansion_services:
      target = service
      if isinstance(service, BeamJarExpansionService):
        target = service.gradle_target
      try:
        schematransform_configs = SchemaAwareExternalTransform.discover(service)
      except Exception as e:
        logging.exception(
            "Encountered an error while discovering expansion service %s:\n%s",
            target,
            e)
        continue
      skipped_urns = []
      for config in schematransform_configs:
        if config.identifier not in identifiers:
          identifiers.add(config.identifier)
          identifier_components = config.identifier.split(':')
          # We expect URNs like
          # `beam:schematransform:org.apache.beam:my_transform:v1`
          if len(identifier_components) != 5:
            skipped_urns.append(config.identifier)
            continue
          name = snake_case_to_upper_camel_case(identifier_components[3])

          # add version number if there are multiple versions
          version = identifier_components[4]
          if version != 'v1':
            name += version.capitalize()

          self.wrappers[name] = type(
              name, (Wrapper, ),
              dict(
                  identifier=config.identifier,
                  default_expansion_service=service,
                  schematransform=config,
                  configuration_schema=get_config_with_descriptions(config)))
          self.urn_to_wrapper_name[config.identifier] = name

      logging.debug(
          "Skipped URN(s) in %s that don't follow the standard in "
          "https://beam.apache.org/documentation/"
          "programming-guide/#1314-defining-a-urn: %s",
          target,
          skipped_urns)

    for name, wrapper in self.wrappers.items():
      setattr(self, name, wrapper)

  def get_available(self) -> typing.Set[str]:
    """Get a set of all available wrapper names"""
    self._maybe_create_wrappers()
    return set(self.wrappers.keys())

  def get(self, name) -> Wrapper:
    """Get a wrapper by its name"""
    self._maybe_create_wrappers()
    return self.wrappers[name]

  def get_urn(self, identifier) -> Wrapper:
    """Get a wrapper by its URN"""
    self._maybe_create_wrappers()
    return self.wrappers[self.urn_to_wrapper_name[identifier]]

  def _maybe_create_wrappers(self):
    if not self.wrappers:
      self._create_wrappers()
