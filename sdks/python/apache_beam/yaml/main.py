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

import argparse
import contextlib
import json

import yaml

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import MillisInstant
from apache_beam.yaml import yaml_transform


def _preparse_jinja_flags(argv):
  """Promotes any flags to --jinja_variables based on --jinja_variable_flags.

  This is to facilitate tools (such as dataflow templates) that must pass
  options as un-nested flags.
  """
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--jinja_variable_flags',
      default=[],
      type=lambda s: s.split(','),
      help='A list of flag names that should be used as jinja variables.')
  parser.add_argument(
      '--jinja_variables',
      default={},
      type=json.loads,
      help='A json dict of variables used when invoking the jinja preprocessor '
      'on the provided yaml pipeline.')
  jinja_args, other_args = parser.parse_known_args(argv)
  if not jinja_args.jinja_variable_flags:
    return argv

  jinja_variable_parser = argparse.ArgumentParser()
  for flag_name in jinja_args.jinja_variable_flags:
    jinja_variable_parser.add_argument('--' + flag_name)
  jinja_flag_variables, pipeline_args = jinja_variable_parser.parse_known_args(
      other_args)
  jinja_args.jinja_variables.update(
      **
      {k: v
       for (k, v) in vars(jinja_flag_variables).items() if v is not None})
  if jinja_args.jinja_variables:
    pipeline_args = pipeline_args + [
        '--jinja_variables=' + json.dumps(jinja_args.jinja_variables)
    ]

  return pipeline_args


def _parse_arguments(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--yaml_pipeline',
      '--pipeline_spec',
      help='A yaml description of the pipeline to run.')
  parser.add_argument(
      '--yaml_pipeline_file',
      '--pipeline_spec_file',
      help='A file containing a yaml description of the pipeline to run.')
  parser.add_argument(
      '--json_schema_validation',
      default='generic',
      help='none: do no pipeline validation against the schema; '
      'generic: validate the pipeline shape, but not individual transforms; '
      'per_transform: also validate the config of known transforms')
  parser.add_argument(
      '--jinja_variables',
      default=None,
      type=json.loads,
      help='A json dict of variables used when invoking the jinja preprocessor '
      'on the provided yaml pipeline.')
  return parser.parse_known_args(argv)


def _pipeline_spec_from_args(known_args):
  if known_args.yaml_pipeline_file and known_args.yaml_pipeline:
    raise ValueError(
        "Exactly one of yaml_pipeline or yaml_pipeline_file must be set.")
  elif known_args.yaml_pipeline_file:
    with FileSystems.open(known_args.yaml_pipeline_file) as fin:
      pipeline_yaml = fin.read().decode()
  elif known_args.yaml_pipeline:
    pipeline_yaml = known_args.yaml_pipeline
  else:
    raise ValueError(
        "Exactly one of yaml_pipeline or yaml_pipeline_file must be set.")

  return pipeline_yaml


@contextlib.contextmanager
def _fix_xlang_instant_coding():
  # Scoped workaround for https://github.com/apache/beam/issues/28151.
  old_registry = LogicalType._known_logical_types
  LogicalType._known_logical_types = old_registry.copy()
  try:
    LogicalType.register_logical_type(MillisInstant)
    yield
  finally:
    LogicalType._known_logical_types = old_registry


def run(argv=None):
  argv = _preparse_jinja_flags(argv)
  known_args, pipeline_args = _parse_arguments(argv)
  pipeline_template = _pipeline_spec_from_args(known_args)
  pipeline_yaml = yaml_transform.expand_jinja(
      pipeline_template, known_args.jinja_variables or {})
  pipeline_spec = yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)

  with _fix_xlang_instant_coding():
    with beam.Pipeline(  # linebreak for better yapf formatting
        options=beam.options.pipeline_options.PipelineOptions(
            pipeline_args,
            pickle_library='cloudpickle',
            **yaml_transform.SafeLineLoader.strip_metadata(pipeline_spec.get(
                'options', {}))),
        display_data={'yaml': pipeline_yaml,
                      'yaml_jinja_template': pipeline_template,
                      'yaml_jinja_variables': json.dumps(
                          known_args.jinja_variables)}) as p:
      print("Building pipeline...")
      yaml_transform.expand_pipeline(
          p, pipeline_spec, validate_schema=known_args.json_schema_validation)
      print("Running pipeline...")


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()
