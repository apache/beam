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
import collections
import yaml

import apache_beam as beam
from apache_beam.yaml import yaml_transform


def run(argv=None):
  # Do imports here to avoid main session issues.
  parser = argparse.ArgumentParser()
  parser.add_argument('--pipeline_spec')
  parser.add_argument('--pipeline_spec_file')
  known_args, pipeline_args = parser.parse_known_args(argv)

  if known_args.pipeline_spec_file:
    with open(known_args.pipeline_spec_file) as fin:
      known_args.pipeline_spec = fin.read()

  if known_args.pipeline_spec:
    pipeline_spec = yaml.load(
        known_args.pipeline_spec, Loader=yaml_transform.SafeLineLoader)
  else:
    raise ValueError(
        "Exactly one of pipeline_spec or pipeline_spec_file must be set.")

  yaml_transform._LOGGER.setLevel('INFO')

  with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
      pipeline_args,
      pickle_library='cloudpickle',
      **pipeline_spec.get('options', {}))) as p:
    print("Building pipeline...")
    yaml_transform.expand_pipeline(p, known_args.pipeline_spec)
    print("Running pipeline...")


if __name__ == '__main__':
  run()
