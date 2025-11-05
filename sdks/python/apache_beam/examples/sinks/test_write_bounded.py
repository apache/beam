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
# python -m apache_beam.examples.sinks.test_write_bounded

# This file contains multiple examples of writing bounded PCollection to files

import argparse
import json
import logging

import pyarrow

import apache_beam as beam
from apache_beam.io.fileio import WriteToFiles
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms.util import LogElements


def run(argv=None, save_main_session=True) -> PipelineResult:
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  p = beam.Pipeline(options=pipeline_options)

  output = (
      p | beam.Create([{
          'age': 10
      }, {
          'age': 20
      }, {
          'age': 30
      }])
      | beam.LogElements(
          prefix='before write ', with_window=False, level=logging.INFO))
  #TextIO
  output2 = output | 'Write to text' >> WriteToText(
      file_path_prefix="__output_batch__/ouput_WriteToText",
      file_name_suffix=".txt",
      shard_name_template='-U-SSSSS-of-NNNNN')
  _ = output2 | 'LogElements after WriteToText' >> LogElements(
      prefix='after WriteToText ', with_window=False, level=logging.INFO)

  #FileIO
  output3 = (
      output | 'Serialize' >> beam.Map(json.dumps)
      | 'Write to files' >>
      WriteToFiles(path="__output_batch__/output_WriteToFiles"))
  _ = output3 | 'LogElements after WriteToFiles' >> LogElements(
      prefix='after WriteToFiles ', with_window=False, level=logging.INFO)

  #ParquetIO
  output4 = output | 'Write' >> beam.io.WriteToParquet(
      file_path_prefix="__output_batch__/output_parquet",
      schema=pyarrow.schema([('age', pyarrow.int64())]))
  _ = output4 | 'LogElements after WriteToParquet' >> LogElements(
      prefix='after WriteToParquet ', with_window=False, level=logging.INFO)
  _ = output | 'Write parquet' >> beam.io.WriteToParquet(
      file_path_prefix="__output_batch__/output_WriteToParquet",
      schema=pyarrow.schema([('age', pyarrow.int64())]),
      record_batch_size=10,
      num_shards=0)

  # Execute the pipeline and return the result.
  result = p.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
