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
import json

import jinja2
import yaml

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import MillisInstant
from apache_beam.yaml import yaml_transform

# Workaround for https://github.com/apache/beam/issues/28151.
LogicalType.register_logical_type(MillisInstant)


def _configure_parser(argv):
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
      help='A json dict of variables used to invoke the jinja preprocessor '
      'on the provided yaml pipeline.')
  parser.add_argument(
      '--flags_as_jinja_variables',
      default=False,
      action='store_true',
      help='Whether to treat all unknown `--flag=value` arguments as jinja '
      'variables.')
  return parser.parse_known_args(argv)


def _extract_jinja_variables(known_args, other_args):
  jinja_variables = known_args.jinja_variables or {}
  # This is best-effort and may catch pipeline options not intended for
  # templating, but jinja2 doesn't care if there are extra variables defined.
  if known_args.flags_as_jinja_variables:
    skip = False
    for ix, arg in enumerate(other_args):
      if skip:
        skip = False
        continue
      if arg.startswith('--'):
        if '=' in arg:
          key, value = arg[2:].split('=', 1)
        elif len(other_args) > ix + 1:
          key = arg[2:]
          value = other_args[ix + 1]
          skip = True
        else:
          continue
        jinja_variables[key] = value

  return jinja_variables


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


def run(argv=None):
  known_args, pipeline_args = _configure_parser(argv)
  pipeline_yaml = _pipeline_spec_from_args(known_args)
  jinja_variables = _extract_jinja_variables(known_args, pipeline_args)
  if (known_args.jinja_variables is not None or
      known_args.flags_as_jinja_variables):
    class FileIOLoader(jinja2.BaseLoader):
      def get_source(self, environment, path):
        source = FileSystems.open(path).read().decode()
        return source, path, lambda: True
          
    pipeline_yaml = jinja2.Environment(
        undefined=jinja2.StrictUndefined, 
        loader=FileIOLoader()).from_string(pipeline_yaml).render(
            **jinja_variables)
  pipeline_spec = yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)

  with beam.Pipeline(  # linebreak for better yapf formatting
      options=beam.options.pipeline_options.PipelineOptions(
          pipeline_args,
          pickle_library='cloudpickle',
          **yaml_transform.SafeLineLoader.strip_metadata(pipeline_spec.get(
              'options', {}))),
      display_data={'yaml': pipeline_yaml}) as p:
    print("Building pipeline...")
    yaml_transform.expand_pipeline(
        p, pipeline_spec, validate_schema=known_args.json_schema_validation)
    print("Running pipeline...")


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  run()
