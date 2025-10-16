#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# To run the pipelines locally:
# python -m apache_beam.examples.sinks.test_periodicimpulse

# This file contains examples of writing unbounded PCollection using
# PeriodicImpulse to files

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms.window import FixedWindows


def run(argv=None, save_main_session=True) -> PipelineResult:
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  p = beam.Pipeline(options=pipeline_options)

  _ = (
      p
      | "Create elements" >> beam.transforms.periodicsequence.PeriodicImpulse(
          start_timestamp=1,
          stop_timestamp=100,
          fire_interval=10,
          apply_windowing=False)
      | 'ApplyWindowing' >> beam.WindowInto(FixedWindows(20))
      | beam.io.WriteToText(
          file_path_prefix="__output__/ouput_WriteToText",
          file_name_suffix=".txt"))

  # Execute the pipeline and return the result.
  result = p.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
