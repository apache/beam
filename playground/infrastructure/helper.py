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

"""
Common helper module for CI/CD Steps
"""

import asyncio
import logging
import os
from collections import namedtuple
from dataclasses import dataclass, fields
from typing import List, Optional, Dict, Union

import yaml
from yaml import YAMLError

from api.v1.api_pb2 import SDK_UNSPECIFIED, STATUS_UNSPECIFIED, Sdk, \
  STATUS_VALIDATING, STATUS_PREPARING, \
  STATUS_COMPILING, STATUS_EXECUTING, PRECOMPILED_OBJECT_TYPE_UNIT_TEST, \
  PRECOMPILED_OBJECT_TYPE_KATA, PRECOMPILED_OBJECT_TYPE_UNSPECIFIED, \
  PRECOMPILED_OBJECT_TYPE_EXAMPLE, PrecompiledObjectType
from config import Config, TagFields, PrecompiledExampleType
from grpc_client import GRPCClient

Tag = namedtuple(
    "Tag",
    [
        TagFields.name,
        TagFields.description,
        TagFields.multifile,
        TagFields.categories,
        TagFields.pipeline_options
    ])


@dataclass
class Example:
  """
  Class which contains all information about beam example
  """
  name: str
  sdk: SDK_UNSPECIFIED
  filepath: str
  code: str
  status: STATUS_UNSPECIFIED
  tag: Tag
  logs: str = ""
  type: PrecompiledObjectType = PRECOMPILED_OBJECT_TYPE_UNSPECIFIED
  pipeline_id: str = ""
  output: str = ""


@dataclass
class ExampleTag:
  """
  Class which contains all information about beam playground tag
  """
  tag_as_dict: Dict[str, str]
  tag_as_string: str


def find_examples(work_dir: str, supported_categories: List[str],
                  sdk: Sdk) -> List[Example]:
  """
  Find and return beam examples.

  Search throws all child files of work_dir directory files with beam tag:
  Beam-playground:
      name: NameOfExample
      description: Description of NameOfExample.
      multifile: false
      categories:
          - category-1
          - category-2
      pipeline_options: --inputFile=your_file --outputFile=your_output_file
  If some example contain beam tag with incorrect format raise an error.

  Args:
      work_dir: directory where to search examples.
      supported_categories: list of supported categories.
      sdk: sdk that using to find examples for the specific sdk.

  Returns:
      List of Examples.
  """
  has_error = False
  examples = []
  for root, _, files in os.walk(work_dir):
    for filename in files:
      filepath = os.path.join(root, filename)
      error_during_check_file = _check_file(
          examples=examples,
          filename=filename,
          filepath=filepath,
          supported_categories=supported_categories,
          sdk=sdk)
      has_error = has_error or error_during_check_file
  if has_error:
    raise ValueError(
        "Some of the beam examples contain beam playground tag with "
        "an incorrect format")
  return examples


async def get_statuses(examples: List[Example]):
  """
  Receive status and update example.status and example.pipeline_id for
  each example

  Args:
      examples: beam examples for processing and updating statuses and
      pipeline_id values.
  """
  tasks = []
  client = GRPCClient()
  for example in examples:
    tasks.append(_update_example_status(example, client))
  await asyncio.gather(*tasks)


def get_tag(filepath) -> Optional[ExampleTag]:
  """
  Parse file by filepath and find beam tag

  Args:
      filepath: path of the file

  Returns:
      If file contains tag, returns tag as a map.
      If file doesn't contain tag, returns None
  """
  add_to_yaml = False
  yaml_string = ""
  tag_string = ""

  with open(filepath, encoding="utf-8") as parsed_file:
    lines = parsed_file.readlines()

  for line in lines:
    formatted_line = line.replace("//", "").replace("#", "")
    if add_to_yaml is False:
      if formatted_line.lstrip() == Config.BEAM_PLAYGROUND_TITLE:
        add_to_yaml = True
        yaml_string += formatted_line.lstrip()
        tag_string += line
    else:
      yaml_with_new_string = yaml_string + formatted_line
      try:
        yaml.load(yaml_with_new_string, Loader=yaml.SafeLoader)
        yaml_string += formatted_line
        tag_string += line
      except YAMLError:
        break

  if add_to_yaml:
    tag_object = yaml.load(yaml_string, Loader=yaml.SafeLoader)
    return ExampleTag(tag_object[Config.BEAM_PLAYGROUND], tag_string)

  return None


def _check_file(examples, filename, filepath, supported_categories, sdk: Sdk):
  """
  Check file by filepath for matching to beam example. If file is beam example,
  then add it to list of examples

  Args:
      examples: list of examples.
      filename: name of the file.
      filepath: path to the file.
      supported_categories: list of supported categories.
      sdk: sdk that using to find examples for the specific sdk.

  Returns:
      True if file has beam playground tag with incorrect format.
      False if file has correct beam playground tag.
      False if file doesn't contains beam playground tag.
  """
  if filepath.endswith("infrastructure/helper.py"):
    return False

  has_error = False
  extension = filepath.split(os.extsep)[-1]
  if extension == Config.SDK_TO_EXTENSION[sdk]:
    tag = get_tag(filepath)
    if tag is not None:
      if _validate(tag.tag_as_dict, supported_categories) is False:
        logging.error(
            "%s contains beam playground tag with incorrect format", filepath)
        has_error = True
      else:
        examples.append(_get_example(filepath, filename, tag))
  return has_error


def get_supported_categories(categories_path: str) -> List[str]:
  """
  Return list of supported categories from categories_path file

  Args:
      categories_path: path to the file with categories.

  Returns:
      All supported categories as a list.
  """
  with open(categories_path, encoding="utf-8") as supported_categories:
    yaml_object = yaml.load(supported_categories.read(), Loader=yaml.SafeLoader)
    return yaml_object[TagFields.categories]


def _get_example(
    filepath: str, filename: str, tag: ExampleTag) -> Example:
  """
  Return an Example by filepath and filename.

  Args:
       tag: tag of the example.
       filepath: path of the example's file.
       filename: name of the example's file.

  Returns:
      Parsed Example object.
  """
  name = _get_name(filename)
  sdk = Config.EXTENSION_TO_SDK[filename.split(os.extsep)[-1]]
  object_type = _get_object_type(filename, filepath)
  with open(filepath, encoding="utf-8") as parsed_file:
    content = parsed_file.read()
  content = content.replace(tag.tag_as_string, "")

  return Example(
      name=name,
      sdk=sdk,
      filepath=filepath,
      code=content,
      status=STATUS_UNSPECIFIED,
      tag=Tag(**tag.tag_as_dict),
      type=object_type)


def _validate(tag: dict, supported_categories: List[str]) -> bool:
  """
  Validate all tag's fields

  Validate that tag contains all required fields and all fields have required
  format.

  Args:
      tag: beam tag to validate.
      supported_categories: list of supported categories.

  Returns:
      In case tag is valid, True
      In case tag is not valid, False
  """
  valid = True
  for field in fields(TagFields):
    if field.default not in tag:
      logging.error(
          "tag doesn't contain %s field: %s \n"
          "Please, check that this field exists in the beam playground tag."
          "If you are sure that this field exists in the tag"
          " check the format of indenting.",
          field.default,
          tag.__str__())
      valid = False

    name = tag.get(TagFields.name)
    if name == "":
      logging.error(
          "tag's field name is incorrect: %s \nname can not be empty.",
          tag.__str__())
      valid = False

  multifile = tag.get(TagFields.multifile)
  if (multifile is not None) and (str(multifile).lower() not in ["true",
                                                                 "false"]):
    logging.error(
        "tag's field multifile is incorrect: %s \n"
        "multifile variable should be boolean format, but tag contains: %s",
        tag.__str__(),
        str(multifile))
    valid = False

  categories = tag.get(TagFields.categories)
  if categories is not None:
    if not isinstance(categories, list):
      logging.error(
          "tag's field categories is incorrect: %s \n"
          "categories variable should be list format, but tag contains: %s",
          tag.__str__(),
          str(type(categories)))
      valid = False
    else:
      for category in categories:
        if category not in supported_categories:
          logging.error(
              "tag contains unsupported category: %s \n"
              "If you are sure that %s category should be placed in "
              "Beam Playground, you can add it to the "
              "`playground/categories.yaml` file",
              category,
              category)
          valid = False
  return valid


def _get_name(filename: str) -> str:
  """
  Return name of the example by his filepath.

  Get name of the example by his filename.

  Args:
      filename: filename of the beam example file.

  Returns:
      example's name.
  """
  return filename.split(os.extsep)[0]


async def _update_example_status(example: Example, client: GRPCClient):
  """
  Receive status for examples and update example.status and pipeline_id

  Use client to send requests to the backend:
  1. Start code processing.
  2. Ping the backend while status is STATUS_VALIDATING/
    STATUS_PREPARING/STATUS_COMPILING/STATUS_EXECUTING
  Update example.status with resulting status.

  Args:
      example: beam example for processing and updating status and pipeline_id.
      client: client to send requests to the server.
  """
  pipeline_id = await client.run_code(
      example.code, example.sdk, example.tag[TagFields.pipeline_options])
  example.pipeline_id = pipeline_id
  status = await client.check_status(pipeline_id)
  while status in [STATUS_VALIDATING,
                   STATUS_PREPARING,
                   STATUS_COMPILING,
                   STATUS_EXECUTING]:
    await asyncio.sleep(Config.PAUSE_DELAY)
    status = await client.check_status(pipeline_id)
  example.status = status


def _get_object_type(filename, filepath):
  """
  Get type of an object based on it filename/filepath

  Args:
      filename: object's filename
      filepath: object's filepath

  Returns: type of the object (example, kata, unit-test)
  """
  filename_no_ext = (os.path.splitext(filename)[0]).lower()
  if filename_no_ext.endswith(PrecompiledExampleType.test_ends):
    object_type = PRECOMPILED_OBJECT_TYPE_UNIT_TEST
  elif PrecompiledExampleType.katas in filepath.split(os.sep):
    object_type = PRECOMPILED_OBJECT_TYPE_KATA
  elif PrecompiledExampleType.examples in filepath.split(os.sep):
    object_type = PRECOMPILED_OBJECT_TYPE_EXAMPLE
  else:
    object_type = PRECOMPILED_OBJECT_TYPE_UNSPECIFIED
  return object_type
