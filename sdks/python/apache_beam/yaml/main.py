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
import os
import sys
import unittest

import yaml

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
# The following imports force the registration of JDBC logical types.
# When running a Beam YAML pipeline, the expansion service handles JDBCIO using
# Java transforms, bypassing the Python module (`apache_beam.io.jdbc`) that
# registers these types. These imports load the module, preventing a
# "logical type not found" error.
from apache_beam.io.jdbc import JdbcDateType  # pylint: disable=unused-import
from apache_beam.io.jdbc import JdbcTimeType  # pylint: disable=unused-import
from apache_beam.transforms import resources
from apache_beam.yaml import yaml_testing
from apache_beam.yaml import yaml_transform
from apache_beam.yaml import yaml_utils


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
  parser.add_argument(
      '--tests',
      '--test',
      action=argparse.BooleanOptionalAction,
      help='Run the tests associated with the given pipeline, rather than the '
      'pipeline itself.')
  parser.add_argument(
      '--fix_tests',
      action=argparse.BooleanOptionalAction,
      help='Update failing test expectations to match the actual ouput. '
      'Requires --test_suite if the pipeline uses jinja formatting.')
  parser.add_argument(
      '--create_test',
      action=argparse.BooleanOptionalAction,
      help='Automatically creates a regression test for the given pipeline, '
      'adding it to the pipeline spec or test suite dependon on whether '
      '--test_suite is given. '
      'Requires --test_suite if the pipeline uses jinja formatting.')
  parser.add_argument(
      '--test_suite',
      help='Run the given tests against the given pipeline, rather than the '
      'pipeline itself. '
      'Should be a file containing a list of yaml test specifications.')
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


def run(argv=None):
  options, constructor, display_data = build_pipeline_components_from_argv(argv)
  with beam.Pipeline(options=options, display_data=display_data) as p:
    print('Building pipeline...')
    constructor(p)
    print('Running pipeline...')


def run_tests(argv=None, exit=True):
  known_args, pipeline_args, _, pipeline_yaml = _build_pipeline_yaml_from_argv(
      argv)
  pipeline_spec = yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)
  options = _build_pipeline_options(pipeline_spec, pipeline_args)

  if known_args.create_test and known_args.fix_tests:
    raise ValueError(
        'At most one of --create_test and --fix_tests may be specified.')
  elif known_args.create_test:
    result = unittest.TestResult()
    tests = []
  else:
    if known_args.test_suite:
      with open(known_args.test_suite) as fin:
        test_suite_holder = yaml.load(
            fin, Loader=yaml_transform.SafeLineLoader) or {}
    else:
      test_suite_holder = pipeline_spec
    test_specs = test_suite_holder.get('tests', [])
    if not isinstance(test_specs, list):
      raise TypeError('tests attribute must be a list of test specifications.')
    elif not test_specs:
      raise RuntimeError(
          'No tests found. '
          "If you haven't added a set of tests yet, you can get started by "
          'running your pipeline with the --create_test flag enabled.')

    tests = [
        yaml_testing.YamlTestCase(
            pipeline_spec, test_spec, options, known_args.fix_tests)
        for test_spec in test_specs
    ]
    suite = unittest.TestSuite(tests)
    result = unittest.TextTestRunner().run(suite)

  if known_args.fix_tests or known_args.create_test:
    update_tests(known_args, pipeline_yaml, pipeline_spec, options, tests)

  if exit:
    # emulates unittest.main()
    sys.exit(0 if result.wasSuccessful() else 1)
  else:
    if not result.wasSuccessful():
      raise RuntimeError(result)


def update_tests(known_args, pipeline_yaml, pipeline_spec, options, tests):
  if known_args.test_suite:
    path = known_args.test_suite
    if not os.path.exists(path) and known_args.create_test:
      with open(path, 'w') as fout:
        fout.write('tests: []')
  elif known_args.yaml_pipeline_file:
    path = known_args.yaml_pipeline_file
  else:
    raise RuntimeError(
        'Test fixing only supported for file-backed tests. '
        'Please use the --test_suite flag.')
  with open(path) as fin:
    original_yaml = fin.read()
  if path == known_args.yaml_pipeline_file and pipeline_yaml.strip(
  ) != original_yaml.strip():
    raise RuntimeError(
        'In-file test fixing not yet supported for templated pipelines. '
        'Please use the --test_suite flag.')
  updated_spec = yaml.load(original_yaml, Loader=yaml.SafeLoader) or {}

  if known_args.fix_tests:
    updated_spec['tests'] = [test.fixed_test() for test in tests]

  if known_args.create_test:
    if 'tests' not in updated_spec:
      updated_spec['tests'] = []
    updated_spec['tests'].append(
        yaml_testing.create_test(pipeline_spec, options))

  updated_yaml = yaml_utils.patch_yaml(original_yaml, updated_spec)
  with open(path, 'w') as fout:
    fout.write(updated_yaml)


def _build_pipeline_yaml_from_argv(argv):
  argv = _preparse_jinja_flags(argv)
  known_args, pipeline_args = _parse_arguments(argv)
  pipeline_template = _pipeline_spec_from_args(known_args)
  pipeline_yaml = yaml_transform.expand_jinja(
      pipeline_template, known_args.jinja_variables or {})
  return known_args, pipeline_args, pipeline_template, pipeline_yaml


def _build_pipeline_options(pipeline_spec, pipeline_args):
  return beam.options.pipeline_options.PipelineOptions(
      pipeline_args,
      pickle_library='cloudpickle',
      **yaml_transform.SafeLineLoader.strip_metadata(
          pipeline_spec.get('options', {})))


def build_pipeline_components_from_argv(argv):
  (known_args, pipeline_args, pipeline_template,
   pipeline_yaml) = _build_pipeline_yaml_from_argv(argv)
  display_data = {
      'yaml': pipeline_yaml,
      'yaml_jinja_template': pipeline_template,
      'yaml_jinja_variables': json.dumps(known_args.jinja_variables),
  }
  options, constructor = build_pipeline_components_from_yaml(
      pipeline_yaml,
      pipeline_args,
      known_args.json_schema_validation,
      known_args.yaml_pipeline_file,
  )
  return options, constructor, display_data


def build_pipeline_components_from_yaml(
    pipeline_yaml, pipeline_args, validate_schema='generic', pipeline_path=''):
  pipeline_spec = yaml.load(pipeline_yaml, Loader=yaml_transform.SafeLineLoader)

  options = _build_pipeline_options(pipeline_spec, pipeline_args)

  def constructor(root):
    if 'resource_hints' in pipeline_spec.get('pipeline', {}):
      # Add the declared resource hints to the "root" spec.
      root._current_transform().resource_hints.update(
          resources.parse_resource_hints(
              yaml_transform.SafeLineLoader.strip_metadata(
                  pipeline_spec['pipeline']['resource_hints'])))
    yaml_transform.expand_pipeline(
        root,
        pipeline_spec,
        validate_schema=validate_schema,
        pipeline_path=pipeline_path,
    )

  return options, constructor


if __name__ == '__main__':
  import logging
  logging.getLogger().setLevel(logging.INFO)
  if '--tests' in sys.argv or '--test' in sys.argv:
    run_tests()
  else:
    run()
