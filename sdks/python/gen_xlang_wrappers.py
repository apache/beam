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
import datetime
import inspect
import logging
import os
import shutil
import subprocess
import typing
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import yaml

from gen_protos import LICENSE_HEADER
from gen_protos import PROJECT_ROOT
from gen_protos import PYTHON_SDK_ROOT

SUPPORTED_SDK_DESTINATIONS = ['python']
PYTHON_SUFFIX = "_et.py"
PY_WRAPPER_OUTPUT_DIR = os.path.join(
    PYTHON_SDK_ROOT, 'apache_beam', 'transforms', 'xlang')


def generate_transforms_config(input_services, output_file):
  """
  Generates a YAML file containing a list of transform configurations.

  Takes an input YAML file containing a list of expansion service gradle
  targets. Each service must provide a `destinations` field that specifies the
  default package (relative path) that generated wrappers should be imported
  to. A default destination package is specified for each SDK, like so::

    - gradle_target: 'sdks:java:io:expansion-service:shadowJar'
      destinations:
        python: 'apache_beam/io'

  We use :class:`ExternalTransformProvider` to discover external
  transforms. Then, we extract the necessary details of each transform and
  compile them into a new YAML file, which is later used to generate wrappers.

  Importing generated transforms to an existing package
  -----------------------------------------------------
  When running the script on the config above, a new module will be created at
  `apache_beam/transforms/xlang/io.py`. This contains all
  generated wrappers that are set to destination 'apache_beam/io'. Finally,
  to make these available to the `apache_beam.io` package (or any package
  really), just add the following line to the package's `__init__.py` file::
    from apache_beam.transforms.xlang.io import *

  Modifying a transform's name and destination
  --------------------------------------------
  Each service may also specify modifications for particular transform.
  Currently, one can modify the generated wrapper's **name** and
  **destination** package:
    - By default, the transform's identifier is used to generate the wrapper
      class name. This can be overriden by manually providing a name.
    - By default, generated wrappers are made available to the package provided
      by their respective expansion service. This can be overridden by
      providing a relative path to a different package.

  See the following example for what such modifications can look like::

    - gradle_target: 'sdks:java:io:expansion-service:shadowJar'
      destinations:
        python: 'apache_beam/io'
      transforms:
        'beam:schematransform:org.apache.beam:my_transform:v1':
          name: 'MyCustomTransformName'
          destinations:
            python: 'apache_beam/io/gcp'

  For the above example, we would take the transform with identifier
  `beam:schematransform:org.apache.beam:my_transform:v1` and by default infer
  a wrapper class name of `MyTransform` then write it to the module
  `apache_beam/transforms/xlang/io.py`. With these modifications
  however, we instead use the provided name `MyCustomTransformName` and write
  it to `apache_beam/transforms/xlang/io_gcp.py`.
  Similar to above, this can be made available by importing it in the
  `__init__.py` file like so::
    from apache_beam.transforms.xlang.io_gcp import *

  Skipping transforms
  -------------------
  To skip a particular transform, simply list its identifier in the
  `skip_transforms` field, like so::

    - gradle_target: 'sdks:java:io:expansion-service:shadowJar'
      destinations:
        python: 'apache_beam/io'
      skip_transforms:
        - 'beam:schematransform:org.apache.beam:some_transform:v1'
  """
  from apache_beam.transforms.external import BeamJarExpansionService
  from apache_beam.transforms.external_transform_provider import ExternalTransform
  from apache_beam.transforms.external_transform_provider import ExternalTransformProvider

  transform_list: List[Dict[str, Any]] = []

  with open(input_services) as f:
    services = yaml.safe_load(f)
  for service in services:
    target = service['gradle_target']

    if "destinations" not in service:
      raise ValueError(
          f"Expansion service with target '{target}' does not "
          "specify any default destinations.")
    service_destinations: Dict[str, str] = service['destinations']
    for sdk, dest in service_destinations.items():
      validate_sdks_destinations(sdk, dest, target)

    transforms_to_skip = service.get('skip_transforms', [])

    # use dynamic provider to discover and populate wrapper details
    provider = ExternalTransformProvider(BeamJarExpansionService(target))
    discovered: Dict[str, ExternalTransform] = provider.get_all()
    for identifier, wrapper in discovered.items():
      if identifier in transforms_to_skip:
        continue

      transform_destinations = service_destinations.copy()

      # apply any modifications
      modified_transform = {}
      if 'transforms' in service and identifier in service['transforms']:
        modified_transform = service['transforms'][identifier]
      for sdk, dest in modified_transform.get('destinations', {}).items():
        validate_sdks_destinations(sdk, dest, target, identifier)
        transform_destinations[sdk] = dest  # override the destination
      name = modified_transform.get('name', wrapper.__name__)

      fields = {}
      for param in wrapper.configuration_schema.values():
        (tp, nullable) = pretty_type(param.type)
        field_info = {
            'type': str(tp),
            'description': param.description,
            'nullable': nullable
        }
        fields[param.original_name] = field_info

      transform = {
          'identifier': identifier,
          'name': name,
          'destinations': transform_destinations,
          'default_service': target,
          'fields': fields,
          'description': inspect.getdoc(wrapper)
      }
      transform_list.append(transform)

  with open(output_file, 'w') as f:
    f.write(LICENSE_HEADER.lstrip())
    f.write(
        "# NOTE: This file is autogenerated and should "
        "not be edited by hand.\n")
    f.write(
        "# Configs are generated based on the expansion service\n"
        f"# configuration in {input_services.replace(PROJECT_ROOT, '')}.\n")
    f.write("# Refer to gen_xlang_wrappers.py for more info.\n")
    dt = datetime.datetime.now().date()
    f.write(f"#\n# Last updated on: {dt}\n\n")
    yaml.dump(transform_list, f)
  logging.info("Successfully wrote transform configs to file: %s", output_file)


def validate_sdks_destinations(sdk, dest, service, identifier=None):
  if identifier:
    message = f"Identifier '{identifier}'"
  else:
    message = f"Service '{service}'"
  if sdk not in SUPPORTED_SDK_DESTINATIONS:
    raise ValueError(
        message + " specifies a destination for an invalid SDK:"
        f" '{sdk}'. The supported SDKs are {SUPPORTED_SDK_DESTINATIONS}")
  if not os.path.isdir(os.path.join(PYTHON_SDK_ROOT, *dest.split('/'))):
    raise ValueError(
        message + f" specifies an invalid destination '{dest}'."
        " Please make sure the destination is an existing directory.")


def pretty_type(tp):
  """
  Takes a type and returns a tuple containing a pretty string representing it
  and a bool signifying if it is nullable or not.

  For optional types, the contained type is unwrapped and returned. This does
  not recurse however, so inner Optional types are not affected.
  E.g. the input typing.Optional[typing.Dict[int, typing.Optional[str]]] will
  return (Dict[int, Union[str, NoneType]], True)
  """
  nullable = False
  if (typing.get_origin(tp) is Union and type(None) in typing.get_args(tp)):
    nullable = True
    # only unwrap if it's a single nullable type. if the type is truly a union
    # of multiple types, leave it alone.
    args = typing.get_args(tp)
    if len(args) == 2:
      tp = list(filter(lambda t: not isinstance(t, type(None)), args))[0]

  # TODO(ahmedabu98): Make this more generic to support other remote SDKs
  # Potentially use Runner API types
  if tp.__module__ == 'builtins':
    tp = tp.__name__
  elif tp.__module__ == 'typing':
    tp = str(tp).replace("typing.", "")
  elif tp.__module__ == 'numpy':
    tp = "%s.%s" % (tp.__module__, tp.__name__)

  return (tp, nullable)


def get_wrappers_from_transform_configs(config_file) -> Dict[str, List[str]]:
  """
  Generates code for external transform wrapper classes (subclasses of
  :class:`ExternalTransform`).

  Takes a YAML file containing a list of SchemaTransform configurations. For
  each configuration, the code for a wrapper class is generated, along with any
  documentation that may be included.

  Each configuration must include a destination file that the generated class
  will be written to.

  Returns the generated classes, grouped by destination.
  """
  from jinja2 import Environment
  from jinja2 import FileSystemLoader

  env = Environment(loader=FileSystemLoader(PYTHON_SDK_ROOT))
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
        param_details = {
            "name": param,
            "type": info['type'],
            "description": info['description'],
        }

        if info['nullable']:
          param_details["default"] = None
        parameters.append(param_details)

      # Python syntax requires function definitions to have
      # non-default parameters first
      parameters = sorted(parameters, key=lambda p: 'default' in p)
      default_service = f"BeamJarExpansionService(\"{default_service}\")"

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


def write_wrappers_to_destinations(
    grouped_wrappers: Dict[str, List[str]],
    output_dir=PY_WRAPPER_OUTPUT_DIR,
    format_code=True):
  """
  Takes a dictionary of generated wrapper code, grouped by destination.
  For each destination, create a new file containing the respective wrapper
  classes. Each file includes the Apache License header and relevant imports.
  Note: the Jinja template should already follow linting and formatting rules.
  """
  written_files = []
  for dest, wrappers in grouped_wrappers.items():
    module_name = dest.replace('apache_beam/', '').replace('/', '_')
    module_path = os.path.join(output_dir, module_name) + ".py"
    with open(module_path, "w") as file:
      file.write(LICENSE_HEADER.lstrip())
      file.write(
          "\n# NOTE: This file contains autogenerated external transform(s)\n"
          "# and should not be edited by hand.\n"
          "# Refer to gen_xlang_wrappers.py for more info.\n\n")
      file.write(
          "\"\"\""
          "Cross-language transforms in this module can be imported from the\n"
          f":py:mod:`{dest.replace('/', '.')}` package."
          "\"\"\"\n\n")
      file.write(
          "# pylint:disable=line-too-long\n\n"
          "from apache_beam.transforms.external import "
          "BeamJarExpansionService\n"
          "from apache_beam.transforms.external_transform_provider "
          "import ExternalTransform\n")
      for wrapper in wrappers:
        file.write(wrapper + "\n")
    written_files.append(module_path)

  logging.info("Created external transform wrapper modules: %s", written_files)

  if format_code:
    formatting_cmd = ['yapf', '--in-place', *written_files]
    subprocess.run(formatting_cmd, capture_output=True, check=True)


def delete_generated_files(root_dir):
  """Scans for and deletes generated wrapper files."""
  logging.info("Deleting external transform wrappers from dir %s", root_dir)
  deleted_files = os.listdir(root_dir)
  for file in deleted_files:
    if file == '__init__.py':
      deleted_files.remove(file)
      continue
    path = os.path.join(root_dir, file)
    if os.path.isfile(path) or os.path.islink(path):
      os.unlink(os.path.join(root_dir, file))
    else:
      shutil.rmtree(path)
  logging.info("Successfully deleted files: %s", deleted_files)


def run_script(
    cleanup,
    generate_config_only,
    input_expansion_services,
    transforms_config_source):
  # Cleanup first if requested. This is needed to remove outdated wrappers.
  if cleanup:
    delete_generated_files(PY_WRAPPER_OUTPUT_DIR)

  # This step requires the expansion service.
  # Only generate a transforms config file if none are provided
  if not transforms_config_source:
    output_transforms_config = os.path.join(
        PROJECT_ROOT, 'sdks', 'standard_external_transforms.yaml')
    generate_transforms_config(
        input_services=input_expansion_services,
        output_file=output_transforms_config)

    transforms_config_source = output_transforms_config
  else:
    if not os.path.exists(transforms_config_source):
      raise RuntimeError(
          "Could not find the provided transforms config "
          f"source: {transforms_config_source}")

  if generate_config_only:
    return

  wrappers_grouped_by_destination = get_wrappers_from_transform_configs(
      transforms_config_source)

  write_wrappers_to_destinations(wrappers_grouped_by_destination)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--cleanup',
      dest='cleanup',
      action='store_true',
      help="Whether to cleanup existing generated wrappers first.")
  parser.add_argument(
      '--generate-config-only',
      dest='generate_config_only',
      action='store_true',
      help="If set, will generate the transform config only without generating"
      "any wrappers.")
  parser.add_argument(
      '--input-expansion-services',
      dest='input_expansion_services',
      default=os.path.join(
          PROJECT_ROOT, 'sdks', 'standard_expansion_services.yaml'),
      help=(
          "Absolute path to the input YAML file that contains "
          "expansion service configs. Ignored if a transforms config"
          "source is provided."))
  parser.add_argument(
      '--transforms-config-source',
      dest='transforms_config_source',
      help=(
          "Absolute path to a source transforms config YAML file to "
          "generate wrapper modules from. If not provided, one will be "
          "created by this script."))
  args = parser.parse_args()

  run_script(
      args.cleanup,
      args.generate_config_only,
      args.input_expansion_services,
      args.transforms_config_source)
