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
import re
from collections import namedtuple
from inspect import Parameter
from inspect import Signature
from typing import Dict
from typing import List
from typing import Tuple

from apache_beam.transforms import PTransform
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import JavaJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.transforms.external import SchemaTransformsConfig
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.schemas import typing_from_runner_api

__all__ = ['ExternalTransform', 'ExternalTransformProvider']


def snake_case_to_upper_camel_case(string):
  """Convert snake_case to UpperCamelCase"""
  components = string.split('_')
  output = ''.join(n.capitalize() for n in components)
  return output


# Information regarding a Wrapper parameter.
ParamInfo = namedtuple('ParamInfo', ['type', 'description', 'original_name'])


def get_config_with_descriptions(
    schematransform: SchemaTransformsConfig) -> Dict[str, ParamInfo]:
  # Prepare a configuration schema that includes types and descriptions
  schema = named_tuple_to_schema(schematransform.configuration_schema)
  descriptions = schematransform.configuration_schema._field_descriptions
  fields_with_descriptions = {}
  for field in schema.fields:
    fields_with_descriptions[field.name] = ParamInfo(
        typing_from_runner_api(field.type),
        descriptions[field.name],
        field.name)

  return fields_with_descriptions


def _generate_signature(schematransform: SchemaTransformsConfig) -> Signature:
  schema = named_tuple_to_schema(schematransform.configuration_schema)
  descriptions = schematransform.configuration_schema._field_descriptions
  params: List[Parameter] = []
  for field in schema.fields:
    annotation = str(typing_from_runner_api(field.type))
    description = descriptions[field.name]
    if description:
      annotation = annotation + f": {description}"
    params.append(
        Parameter(
            field.name, Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation))

  return Signature(params)


class ExternalTransform(PTransform):
  """Template for a wrapper class of an external SchemaTransform

  This is a superclass for dynamically generated SchemaTransform wrappers and
  is not meant to be manually instantiated.

  Experimental; no backwards compatibility guarantees."""

  # These attributes need to be set when
  # creating an ExternalTransform type
  default_expansion_service = None
  identifier: str = ""
  configuration_schema: Dict[str, ParamInfo] = {}

  def __init__(self, expansion_service=None, **kwargs):
    self._kwargs = kwargs
    self._expansion_service = \
        expansion_service or self.default_expansion_service

  def expand(self, input):
    external_schematransform = SchemaAwareExternalTransform(
        identifier=self.identifier,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        **self._kwargs)

    return input | external_schematransform


STANDARD_URN_PATTERN = r"^beam:schematransform:org.apache.beam:([\w-]+):(\w+)$"


def infer_name_from_identifier(identifier: str, pattern: str):
  """Infer a class name from an identifier, adhering to the input pattern"""
  match = re.match(pattern, identifier)
  if not match:
    return None
  groups = match.groups()

  components = [snake_case_to_upper_camel_case(n) for n in groups]
  # Special handling for standard SchemaTransform identifiers:
  # We don't include the version number if it's the first version
  if (pattern == STANDARD_URN_PATTERN and components[1].lower() == 'v1'):
    return components[0]
  else:
    return ''.join(components)


class ExternalTransformProvider:
  """Dynamically discovers Schema-aware external transforms from a given list
  of expansion services and provides them as ready PTransforms.

  A :class:`ExternalTransform` subclass is generated for each external
  transform, and is named based on what can be inferred from the URN
  (see the `urn_pattern` parameter).

  These classes are generated when :class:`ExternalTransformProvider` is
  initialized. You can give it an expansion service address that is already
  up and running:

  >>> provider = ExternalTransformProvider("localhost:12345")

  Or you can give it the path to an expansion service Jar file:

  >>> provider = ExternalTransformProvider(JavaJarExpansionService(
          "path/to/expansion-service.jar"))

  Or you can give it the gradle target of a standard Beam expansion service:

  >>> provider = ExternalTransformProvider(BeamJarExpansionService(
          "sdks:java:io:google-cloud-platform:expansion-service:shadowJar"))

  Note that you can provide a list of these services:

  >>> provider = ExternalTransformProvider([
          "localhost:12345",
          JavaJarExpansionService("path/to/expansion-service.jar"),
          BeamJarExpansionService(
            "sdks:java:io:google-cloud-platform:expansion-service:shadowJar")])

  The output of :func:`get_available()` provides a list of available transforms
  in the provided expansion service(s):

  >>> provider.get_available()
  [('JdbcWrite', 'beam:schematransform:org.apache.beam:jdbc_write:v1'),
  ('BigtableRead', 'beam:schematransform:org.apache.beam:bigtable_read:v1'),
  ...]

  You can retrieve a transform with :func:`get()`, :func:`get_urn()`, or by
  directly accessing it as an attribute. The following lines all do the same
  thing:

  >>> provider.get('BigqueryStorageRead')
  >>> provider.get_urn(
            'beam:schematransform:org.apache.beam:bigquery_storage_read:v1')
  >>> provider.BigqueryStorageRead

  You can inspect the transform's documentation for more details. The following
  returns the documentation provided by the underlying SchemaTransform. If no
  such documentation is provided, this will be empty.

  >>> import inspect
  >>> inspect.getdoc(provider.BigqueryStorageRead)

  Similarly, you can inspect the transform's signature to know more about its
  parameters, including their names, types, and any documentation that the
  underlying SchemaTransform may provide:

  >>> inspect.signature(provider.BigqueryStorageRead)
  (query: 'typing.Union[str, NoneType]: The SQL query to be executed to...',
  row_restriction: 'typing.Union[str, NoneType]: Read only rows that match...',
  selected_fields: 'typing.Union[typing.Sequence[str], NoneType]: Read ...',
  table_spec: 'typing.Union[str, NoneType]: The fully-qualified name of ...')

  The retrieved external transform can be used as a normal PTransform like so::

    with Pipeline() as p:
      _ = (p
        | 'Read from BigQuery` >> provider.BigqueryStorageRead(
                query=query,
                row_restriction=restriction)
        | 'Some processing' >> beam.Map(...))
  """
  def __init__(self, expansion_services, urn_pattern=STANDARD_URN_PATTERN):
    f"""Initialize an ExternalTransformProvider

    :param expansion_services:
      A list of expansion services to discover transforms from.
      Supported forms:
      * a string representing the expansion service address
      * a :attr:`JavaJarExpansionService` pointing to the path of a Java Jar
      * a :attr:`BeamJarExpansionService` pointing to a gradle target
    :param urn_pattern:
      The regular expression used to match valid transforms. In addition to
      validating, the captured groups are used to infer a name for each class.
      By default, the following pattern is used: [{STANDARD_URN_PATTERN}]
    """
    self._urn_pattern = urn_pattern
    self._transforms: Dict[str, type(ExternalTransform)] = {}
    self._name_to_urn: Dict[str, str] = {}

    if isinstance(expansion_services, set):
      expansion_services = list(expansion_services)
    if not isinstance(expansion_services, list):
      expansion_services = [expansion_services]
    self.expansion_services = expansion_services
    self._create_wrappers()

  def _create_wrappers(self):
    # multiple services can overlap and include the same URNs. If this happens,
    # we prioritize by the order of services in the list
    identifiers = set()
    for service in self.expansion_services:
      target = service
      if isinstance(service, BeamJarExpansionService):
        target = service.gradle_target
      if isinstance(service, JavaJarExpansionService):
        target = service.path_to_jar
      try:
        schematransform_configs = SchemaAwareExternalTransform.discover(service)
      except Exception as e:
        logging.exception(
            "Encountered an error while discovering expansion service at '%s':\n%s",
            target,
            e)
        continue
      skipped_urns = []
      for config in schematransform_configs:
        identifier = config.identifier
        if identifier not in identifiers:
          identifiers.add(identifier)

          name = infer_name_from_identifier(identifier, self._urn_pattern)
          if name is None:
            skipped_urns.append(identifier)
            continue

          transform = type(
              name,
              (ExternalTransform, ),
              dict(
                  identifier=identifier,
                  default_expansion_service=service,
                  schematransform=config,
                  # configuration_schema is used by the auto-wrapper generator
                  configuration_schema=get_config_with_descriptions(config)))
          transform.__doc__ = config.description
          transform.__signature__ = _generate_signature(config)

          self._transforms[identifier] = transform
          self._name_to_urn[name] = identifier

      if skipped_urns:
        logging.info(
            "Skipped URN(s) in '%s' that don't follow the pattern \"%s\": %s",
            target,
            self._urn_pattern,
            skipped_urns)

    for transform in self._transforms.values():
      setattr(self, transform.__name__, transform)

  def get_available(self) -> List[Tuple[str, str]]:
    """Get a list of available ExternalTransform names and identifiers"""
    return list(self._name_to_urn.items())

  def get_all(self) -> Dict[str, ExternalTransform]:
    """Get all ExternalTransforms"""
    return self._transforms

  def get(self, name) -> ExternalTransform:
    """Get an ExternalTransform by its inferred class name"""
    return self._transforms[self._name_to_urn[name]]

  def get_urn(self, identifier) -> ExternalTransform:
    """Get an ExternalTransform by its SchemaTransform identifier"""
    return self._transforms[identifier]
