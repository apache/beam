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

"""
Generates Python wrappers for external transforms (specifically,
SchemaTransforms)
"""

import argparse
import logging
import os
import re
import subprocess
import typing
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import yaml
from jinja2 import Environment
from jinja2 import FileSystemLoader

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external_schematransform_provider import STANDARD_URN_PATTERN
from apache_beam.transforms.external_schematransform_provider import ExternalSchemaTransform
from apache_beam.transforms.external_schematransform_provider import ExternalSchemaTransformProvider
from apache_beam.transforms.external_schematransform_provider import camel_case_to_snake_case
from gen_protos import LICENSE_HEADER
from gen_protos import PYTHON_SDK_ROOT
from gen_protos import find_by_ext

SUPPORTED_SDK_DESTINATIONS = ['python']
PYTHON_SUFFIX = "_et.py"


def generate_transform_configs(input_services, output_file):
  """
  Generates a YAML file containing a list of transform configurations.

  Takes an input YAML file containing a list of expansion service gradle
  targets. Each service must provide a `destinations` field that specifies the
  default package (relative path) that generated wrappers should be written
  under. A default destination is specified for each SDK, like so::

    - gradle_target: 'sdks:java:io:expansion-service:shadowJar'
      destinations:
        python: 'apache_beam/io'


  Each service may also specify modifications for particular transform.
  Currently, one can modify the generated wrapper's name and destination file:

    - By default, the transform's identifier is used to generate the wrapper
      class name. This can be overriden by manually providing a name.
    - By default, generated wrappers are written to files within the package
      provided by the expansion service. This can be overridden by manually
      providing a relative file path.

  See the following example for what such modifications can look like::

    - gradle_target: 'sdks:java:io:expansion-service:shadowJar'
      destinations:
        python: 'apache_beam/io'
      transforms:
        'beam:schematransform:org.apache.beam:my_transform:v1':
          name: 'MyCustomTransformName'
          destinations:
            python: 'apache_beam/io/gcp/my_custom_module'

  For the above example, we would take the transform with identifier
  `beam:schematransform:org.apache.beam:my_transform:v1` and by default infer
  a wrapper class name of `MyTransform` and write the generated code to
  the module `apache_beam/io/my_transform_et.py`. With the modifications, we
  instead write the wrapper to `apache_beam/io/gcp/my_custom_module_et.py` and
  name the class `MyCustomTransformName`.

  Note: we add the prefix `"_et.py"` to the module name so that we can find
  these generated files later (e.g. to tell Git to ignore them, and to
  delete them when needed)

  To ignore a particular transform, simply list its identifier in the `ignore`
  field, like so::

    - gradle_target: 'sdks:java:io:expansion-service:shadowJar'
      destinations:
        python: 'apache_beam/io'
      ignore:
        - 'beam:schematransform:org.apache.beam:skip_me:v1':


  We use :class:`ExternalSchemaTransformProvider` to discover external
  transforms. Then, we extract the necessary details of each transform and
  compile them into a new YAML file, which is later used to generate wrappers.
  """
  transform_list: List[Dict[str, Any]] = []

  with open(input_services) as f:
    services = yaml.safe_load(f)
  for service in services:
    target = service['gradle_target']

    # validate expansion service destinations
    if "destinations" not in service:
      raise ValueError(
          f"Expansion service with target [{target}] does not "
          "specify any default destinations.")
    service_destinations: Dict[str, str] = service['destinations']
    for sdk in service_destinations.keys():
      if sdk not in SUPPORTED_SDK_DESTINATIONS:
        raise ValueError(
            f"Service with target {target} specifies a "
            f"destination for an invalid SDK: {sdk}. The "
            f"supported SDKs are {SUPPORTED_SDK_DESTINATIONS}")

    # get transforms to skip, if any
    ignore = service.get('ignore', [])

    # use dynamic provider to discover and populate wrapper details
    provider = ExternalSchemaTransformProvider(BeamJarExpansionService(target))
    discovered: Dict[str, ExternalSchemaTransform] = provider.get_all()
    for identifier, wrapper in discovered.items():
      if identifier in ignore:
        continue
      # We infer the destination from the URN and service destination.
      # For example, the Java IO expansion service defaults to Python
      # package apache_beam/io. Kafka Write is a transform in this service
      # with URN beam:schematransform:org.apache.beam:kafka_write:v1
      # In this case, we infer the destination apache_beam/io/kafka_write
      functionality_identifier = re.match(STANDARD_URN_PATTERN,
                                          identifier).groups()[0]
      destinations = {
          sdk: f"{destination}/{functionality_identifier}"
          for sdk,
          destination in service_destinations.items()
      }
      name = wrapper.__name__

      # apply any modifications
      modified_transform = {}
      if 'transforms' in service and identifier in service['transforms']:
        modified_transform = service['transforms'][identifier]
      if 'name' in modified_transform:
        name = modified_transform['name']  # override the name
      if 'destinations' in modified_transform:
        for sdk, destination in modified_transform['destinations'].items():
          if sdk not in SUPPORTED_SDK_DESTINATIONS:
            raise ValueError(
                f"Identifier {identifier} specifies a destination for "
                f"an invalid SDK: [{sdk}]. The supported SDKs "
                f"are {SUPPORTED_SDK_DESTINATIONS}")
          destinations[sdk] = destination  # override the destination

      # prepare information about parameters
      fields = {}
      for param in wrapper.configuration_schema.values():
        tp = param.type
        nullable = False
        # if type is typing.Optional[...]
        if (typing.get_origin(tp) is Union and
            type(None) in typing.get_args(tp)):
          nullable = True
          # unwrap and set type to the original
          args = typing.get_args(tp)
          if len(args) == 2:
            tp = args[0]

        # some logic for properly setting the type name
        # TODO(ahmedabu98): Find a way to make this logic more generic when
        # supporting other remote SDKs. Potentially use Runner API types
        if tp.__module__ == 'builtins':
          tp = tp.__name__
        elif tp.__module__ == 'typing':
          tp = str(tp).replace("typing.", "")
        elif tp.__module__ == 'numpy':
          tp = "%s.%s" % (tp.__module__, tp.__name__)
        field_info = {
            'type': str(tp),
            'description': param.description,
            'nullable': nullable
        }
        fields[param.original_name] = field_info

      transform = {
          'identifier': identifier,
          'name': name,
          'destinations': destinations,
          'default_service': target,
          'fields': fields,
          'description': wrapper.description
      }
      transform_list.append(transform)

  with open(output_file, 'w') as f:
    f.write(LICENSE_HEADER.lstrip())
    f.write(
        "# NOTE: This file is autogenerated and should "
        "not be edited by hand.\n\n")
    yaml.dump(transform_list, f)


def get_wrappers_from_transform_configs(config_file) -> Dict[str, List[str]]:
  """
  Generates code for external transform wrapper classes (subclasses of
  :class:`ExternalSchemaTransform`).

  Takes a YAML file containing a list of SchemaTransform configurations. For
  each configuration, the code for a wrapper class is generated, along with any
  documentation that may be included.

  Each configuration must include a destination file that the generated class
  will be written to.

  Returns the generated classes, grouped by destination.
  """
  # fetch wrapper template
  env = Environment(loader=FileSystemLoader("./"))
  python_wrapper_template = env.get_template("python_xlang_wrapper.template")

  # maintain a list of wrappers to write in each file. if modified destinations
  # are used, we may end up with multiple wrappers in one file.
  destinations: Dict[str, List[str]] = {}

  with open(config_file) as f:
    transforms = yaml.safe_load(f)
    for config in transforms:
      default_service = config['default_service']
      description = config['description']
      destination = config['destinations']['python']
      name = config['name']
      fields = config['fields']
      identifier = config['identifier']

      parameters = []
      for param, info in fields.items():
        pythonic_name = camel_case_to_snake_case(param)
        param_details = {
            "name": pythonic_name,
            "type": info['type'],
            "description": info['description'],
            "original_name": param
        }
        # for nullable fields, default to None
        if info['nullable']:
          param_details["default"] = None
        parameters.append(param_details)

      # Python syntax requires function definitions to have
      # non-default parameters first
      parameters = sorted(parameters, key=lambda p: 'default' in p)
      default_service = f"BeamJarExpansionService(\"{default_service}\")"

      # use jinja to generate the code
      python_wrapper_class = python_wrapper_template.render(
          class_name=name,
          identifier=identifier,
          parameters=parameters,
          description=description,
          default_expansion_service=default_service)

      if destination not in destinations:
        destinations[destination] = []
      destinations[destination].append(python_wrapper_class)

  return destinations


def write_wrappers_to_destinations(grouped_wrappers: Dict[str, List[str]]):
  """
  Takes a dictionary of generated wrapper code, grouped by destination.
  For each destination, create a new file containing the respective wrapper
  classes. Each file includes the Apache License header and relevant imports.
  Attempts to format the generated files afterward with yapf.
  """
  format_command = ["yapf", "--in-place", "--parallel"]

  for dest, wrappers in grouped_wrappers.items():
    dest += PYTHON_SUFFIX
    with open(dest, "w") as file:
      file.write(LICENSE_HEADER.lstrip())
      file.write(
          "# NOTE: This file contains autogenerated external transform(s)\n"
          "# and should not be edited by hand.\n\n")
      file.write(
          "from apache_beam.transforms.external import "
          "BeamJarExpansionService\n"
          "from apache_beam.transforms.external_schematransform_provider "
          "import ExternalSchemaTransform\n")
      for wrapper in wrappers:
        file.write("\n")
        file.write(wrapper + "\n")
    format_command.append(dest)

  # format the generated files
  # Note: the Jinja template should follow linting and formatting rules
  # already, but this is for good measure
  try:
    subprocess.run(format_command, check=True)
  except subprocess.CalledProcessError as err:
    logging.warning(
        "Could not format the generated external transform wrappers"
        "because of error: %s",
        err.stderr)


def delete_generated_files(root_dir):
  """Scans for and deletes generated wrapper files."""
  logging.info("Deleting external transform wrappers from dir %s", root_dir)
  for path in find_by_ext(root_dir, PYTHON_SUFFIX):
    os.remove(path)


def run_script(cleanup, input_expansion_services, output_transforms_config):
  # Cleanup first if requested. This is needed to remove outdated wrappers.
  if cleanup:
    delete_generated_files(os.path.join(PYTHON_SDK_ROOT, 'apache_beam'))

  # Find paths for expansion service YAML source and
  # transform config YAML output
  expansion_services_source = os.path.join(
      os.path.dirname(__file__), input_expansion_services)
  transforms_config_file = os.path.join(
      os.path.dirname(__file__), output_transforms_config)

  # Generate transform configs and output to the specified path
  generate_transform_configs(
      input_services=expansion_services_source,
      output_file=transforms_config_file)

  # Build and get the generated wrapper code
  grouped_wrappers = get_wrappers_from_transform_configs(transforms_config_file)

  # Write generated code to the appropriate destinations
  write_wrappers_to_destinations(grouped_wrappers)

  # Finally, delete the transform YAML config as we don't need it anymore
  os.remove(transforms_config_file)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--cleanup',
      dest='cleanup',
      action='store_true',
      help="Whether to cleanup existing generated wrappers first.")
  parser.add_argument(
      '--input-expansion-services',
      dest='input_expansion_services',
      default='standard_expansion_services.yaml',
      help=(
          "Relative path to the input YAML file that contains "
          "expansion service configs."))
  parser.add_argument(
      '--output-transforms-config',
      dest='output_transforms_config',
      default='standard_external_transforms.yaml',
      help=(
          "Relative  path to the output YAML file where external "
          "transform configs will be stored."))
  args = parser.parse_args()

  run_script(
      args.cleanup,
      args.input_expansion_services,
      args.output_transforms_config)
