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
import os
import re
from typing import Dict

import yaml

from gen_protos import PROJECT_ROOT
from gen_protos import PYTHON_SDK_ROOT
from gen_xlang_wrappers import pretty_type

SUPPORTED_SDK_DESTINATIONS = ['python']
PYTHON_SUFFIX = "_et.py"
PY_WRAPPER_OUTPUT_DIR = os.path.join(
    PYTHON_SDK_ROOT, 'apache_beam', 'transforms', 'xlang')

MANAGED_HEADER = """---
title: "Managed I/O Connectors"
aliases: [built-in]
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Managed I/O Connectors

Beam’s new Managed API streamlines how you use existing I/Os, offering both
simplicity and powerful enhancements. I/Os are now configured through a
lightweight, consistent interface: a simple configuration map with a unified
API that spans multiple connectors.

With Managed I/O, runners gain deeper insight into each I/O’s structure and
intent. This allows the runner to optimize performance, adjust behavior
dynamically, or even replace the I/O with a more efficient or updated
implementation behind the scenes.

For example, the DataflowRunner can seamlessly upgrade a Managed transform to
its latest SDK version, automatically applying bug fixes and new features (no
manual updates or user intervention required!)

## Supported SDKs

The Managed API is directly accessible through the
[Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/managed/Managed.html)
and
[Python](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.managed.html)
SDKs.

Additionally, some SDKs use the Managed API internally. For example, the Iceberg connector
used in [Beam YAML](https://beam.apache.org/releases/yamldoc/current/#writetoiceberg)
and Beam SQL is invoked via the Managed API under the hood.

"""
_MANAGED_RESOURCES_DIR = os.path.join(
    PROJECT_ROOT, 'sdks', 'java', 'managed', 'src', 'main', 'resources')
_DOCUMENTED_MANAGED_CONFIGS = os.path.join(
    _MANAGED_RESOURCES_DIR, 'available_configs.yaml')
_MANAGED_CONFIG_ALIASES = os.path.join(
    _MANAGED_RESOURCES_DIR, 'config_aliases.yaml')
_DOCUMENTATION_DESTINATION = os.path.join(
    PROJECT_ROOT,
    'website',
    'www',
    'site',
    'content',
    'en',
    'documentation',
    'io',
    'managed-io.md')


def generate_managed_doc(output_location):
  from apache_beam.transforms.external import MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING
  from apache_beam.transforms.external import BeamJarExpansionService
  from apache_beam.transforms.external_transform_provider import ExternalTransform
  from apache_beam.transforms.external_transform_provider import ExternalTransformProvider
  from apache_beam.transforms import managed

  with open(_DOCUMENTED_MANAGED_CONFIGS) as f:
    available_configs: dict = yaml.safe_load(f)
  with open(_MANAGED_CONFIG_ALIASES) as f:
    all_config_aliases: dict = yaml.safe_load(f)

  # Creating a unique list of expansion service jars.
  expansion_service_jar_targets = list(
    dict.fromkeys(MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING.values()))

  read_names_and_identifiers = managed.Read._READ_TRANSFORMS
  write_names_and_identifiers = managed.Write._WRITE_TRANSFORMS

  all_transforms = {}

  for gradle_target in expansion_service_jar_targets:
    provider = ExternalTransformProvider(BeamJarExpansionService(gradle_target))
    discovered: Dict[str, ExternalTransform] = provider.get_all()

    for identifier, transform in discovered.items():
      if identifier in read_names_and_identifiers.values():
        mode = "read"
        name = next(
            k for k, v in read_names_and_identifiers.items() if v == identifier)
      elif identifier in write_names_and_identifiers.values():
        mode = "write"
        name = next(
            k for k,
            v in write_names_and_identifiers.items() if v == identifier)
      else:
        continue

      config_aliases = all_config_aliases.get(identifier, {})
      public_config = available_configs.get(identifier, {})

      fields = []
      for param in transform.configuration_schema.values():
        field_name = resolve_field_name(config_aliases, param.original_name)
        if not should_document(public_config, param.original_name):
          continue

        (tp, nullable) = pretty_type(param.type)
        field_info = {
            'name': field_name,
            'type': str(tp),
            'description': param.description,
            'nullable': nullable
        }
        fields.append(field_info)

      transform = {
          'identifier': identifier,
          'fields': fields,
      }

      all_transforms.setdefault(name, {})[mode] = transform

  doc = MANAGED_HEADER
  doc += generate_configs_summary(all_transforms)
  doc += generate_detailed_configs(all_transforms)

  with open(output_location, 'w') as f:
    f.writelines(doc)


def generate_detailed_configs(all_transforms: dict) -> str:
  details = "## Configuration Details\n\n"

  for name, transform in all_transforms.items():
    for mode, transform_details in transform.items():
      mode = mode[0].upper() + mode[1:].lower()
      details += f"### `{name.upper()}` {mode}\n\n"
      details += get_transform_config_details(transform_details)
  return details


def get_transform_config_details(transform):
  transform_details = """<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>"""

  fields = transform['fields']
  ordered_fields = sorted(fields, key=lambda f: f['nullable'])

  for field in ordered_fields:
    type = field['type']
    if field['name'] == "error_handling":
      continue
    transform_details += f"""
    <tr>
      <td>
        {get_field_name_html(field)}
      </td>
      <td>
        {get_type_format_html(type, False)}
      </td>
      <td>
        {(field['description'] or "n/a")}
      </td>
    </tr>"""

  transform_details += "\n" + spaces(2) + "</table>\n</div>\n\n"

  return transform_details


def generate_configs_summary(all_transforms: dict) -> str:
  summary = """## Available Configurations

<i>Note: required configuration fields are <strong>bolded</strong>.</i>

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Connector Name</th>
      <th>Read Configuration</th>
      <th>Write Configuration</th>
    </tr>\n"""
  for name, transform in all_transforms.items():
    summary += create_html_row(name, transform)
  summary += "  </table>\n</div>\n\n"
  return summary


def create_html_row(transform_name: str, transforms: dict):
  doc = spaces(4) + "<tr>\n" + spaces(
      6) + f"<td><strong>{transform_name.upper()}</strong></td>\n"

  if "read" in transforms:
    doc += get_transform_cell_html(transforms['read'])
  else:
    doc += spaces(6) + "<td>\n" + spaces(8) + "Unavailable\n" + spaces(
        6) + "</td>\n"
  if "write" in transforms:
    doc += get_transform_cell_html(transforms['write'])
  else:
    doc += spaces(6) + "<td>\n" + spaces(8) + "Unavailable\n" + spaces(
        6) + "</td>\n"
  doc += spaces(4) + "</tr>\n"

  return doc


def get_transform_cell_html(transform: dict):
  html = spaces(6) + "<td>\n"
  fields = transform['fields']
  ordered_fields = sorted(fields, key=lambda f: f['nullable'])

  for field in ordered_fields:
    type = field['type']
    if field['name'] == "error_handling":
      continue
    field_line = spaces(8) + get_field_name_html(
        field) + " " + get_type_format_html(type, True) + "<br>\n"
    html += field_line
  html += spaces(6) + "</td>\n"
  return html


def get_field_name_html(field: dict):
  field_name = field['name']
  nullable = field['nullable']

  if not nullable:
    return f"<strong>{field_name}</strong>"
  else:
    return field_name


def resolve_field_name(config_aliases: dict, field_name: str) -> str:
  public_name = next((k for k, v in config_aliases.items() if v == field_name),
                     field_name)
  return public_name


def should_document(public_config: dict, field_name: str) -> bool:
  if not public_config:
    return True

  ignored = public_config.get("ignored", [])
  documented = public_config.get("available", [])

  if not ignored and not documented:
    raise RuntimeError(
        "Documented config should set only one of 'ignored' or 'documented'. "
        "Check " + _DOCUMENTED_MANAGED_CONFIGS)
  if documented:
    return field_name in documented
  else:  # ignored
    return field_name not in ignored


def spaces(n: int):
  return " " * n


def get_type_color(primitive_type: str):
  if primitive_type == "str":
    return "green"
  elif primitive_type.startswith("int"):
    return "#f54251"
  elif primitive_type == "boolean":
    return "orange"

  raise ValueError("Unsupported type: " + primitive_type)


def get_type_format_html(type: str, with_paranthesis: bool):
  if type.startswith("map"):
    regex = r"map\[(.*?),\s*(.*?)\]"
    match = re.search(regex, type)
    key_type = match.group(1)
    value_type = match.group(2)
    key_color = get_type_color(key_type)
    value_color = get_type_color(value_type)
    html_type = (
        '<code>map['
        f'<span style="color: {key_color};">{key_type}</span>, '
        f'<span style="color: {value_color};">{value_type}</span>]'
        '</code>')
  elif type.startswith("list"):
    regex = r"list\[(.*?)\]"
    match = re.search(regex, type)
    inner_type = match.group(1)
    inner_color = get_type_color(inner_type)
    html_type = (
        '<code>list['
        f'<span style="color: {inner_color};">{inner_type}</span>]'
        '</code>')
  else:
    color = get_type_color(type)
    html_type = f'<code style="color: {color}">{type}</code>'
  if with_paranthesis:
    html_type = "(" + html_type + ")"
  return html_type


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_location',
      dest='output_location',
      default=_DOCUMENTATION_DESTINATION,
      help="Destination that the generated doc will be written to.")
  args = parser.parse_args()

  generate_managed_doc(args.output_location)
