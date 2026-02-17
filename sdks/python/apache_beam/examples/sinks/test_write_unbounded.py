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
# python -m apache_beam.examples.sinks.test_write_unbounded

# This file contains multiple examples of writing unbounded PCollection to files

import argparse
import json
import logging

import pyarrow

import apache_beam as beam
from apache_beam.examples.sinks.generate_event import GenerateEvent
from apache_beam.io.fileio import WriteToFiles
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.util import LogElements
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Duration


def run(argv=None, save_main_session=True) -> PipelineResult:
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  p = beam.Pipeline(options=pipeline_options)

  output = p | GenerateEvent.sample_data()

  #TextIO
  output2 = output | 'TextIO WriteToText' >> beam.io.WriteToText(
      file_path_prefix="__output__/ouput_WriteToText",
      file_name_suffix=".txt",
      #shard_name_template='-V-SSSSS-of-NNNNN',
      num_shards=2,
      triggering_frequency=5,
  )
  _ = output2 | 'LogElements after WriteToText' >> LogElements(
      prefix='after WriteToText ', with_window=True, level=logging.INFO)

  #FileIO
  _ = (
      output
      | 'FileIO window' >> beam.WindowInto(
          FixedWindows(5),
          trigger=AfterWatermark(),
          accumulation_mode=AccumulationMode.DISCARDING,
          allowed_lateness=Duration(seconds=0))
      | 'Serialize' >> beam.Map(json.dumps)
      | 'FileIO WriteToFiles' >>
      WriteToFiles(path="__output__/output_WriteToFiles"))

  #ParquetIO
  pyschema = pyarrow.schema([('age', pyarrow.int64())])

  output4a = output | 'WriteToParquet' >> beam.io.WriteToParquet(
      file_path_prefix="__output__/output_parquet",
      #shard_name_template='-V-SSSSS-of-NNNNN',
      file_name_suffix=".parquet",
      num_shards=2,
      triggering_frequency=5,
      schema=pyschema)
  _ = output4a | 'LogElements after WriteToParquet' >> LogElements(
      prefix='after WriteToParquet 4a ', with_window=True, level=logging.INFO)

  output4aw = (
      output
      | 'ParquetIO window' >> beam.WindowInto(
          FixedWindows(20),
          trigger=AfterWatermark(),
          accumulation_mode=AccumulationMode.DISCARDING,
          allowed_lateness=Duration(seconds=0))
      | 'WriteToParquet windowed' >> beam.io.WriteToParquet(
          file_path_prefix="__output__/output_parquet",
          shard_name_template='-W-SSSSS-of-NNNNN',
          file_name_suffix=".parquet",
          num_shards=2,
          schema=pyschema))
  _ = output4aw | 'LogElements after WriteToParquet windowed' >> LogElements(
      prefix='after WriteToParquet 4aw ', with_window=True, level=logging.INFO)

  output4b = (
      output
      | 'To PyArrow Table' >>
      beam.Map(lambda x: pyarrow.Table.from_pylist([x], schema=pyschema))
      | 'WriteToParquetBatched to parquet' >> beam.io.WriteToParquetBatched(
          file_path_prefix="__output__/output_parquet_batched",
          shard_name_template='-V-SSSSS-of-NNNNN',
          file_name_suffix=".parquet",
          num_shards=2,
          triggering_frequency=5,
          schema=pyschema))
  _ = output4b | 'LogElements after WriteToParquetBatched' >> LogElements(
      prefix='after WriteToParquetBatched 4b ',
      with_window=True,
      level=logging.INFO)

  #AvroIO
  avroschema = {
      'name': 'dummy', # your supposed to be file name with .avro extension 
      'type': 'record', # type of avro serilazation, there are more (see above 
                        # docs) but as per me this will do most of the time
      'fields': [ # this defines actual keys & their types
          {'name': 'age', 'type': 'int'},
      ],
    }
  output5 = output | 'WriteToAvro' >> beam.io.WriteToAvro(
      file_path_prefix="__output__/output_avro",
      #shard_name_template='-V-SSSSS-of-NNNNN',
      file_name_suffix=".avro",
      num_shards=2,
      triggering_frequency=5,
      schema=avroschema)
  _ = output5 | 'LogElements after WriteToAvro' >> LogElements(
      prefix='after WriteToAvro 5 ', with_window=True, level=logging.INFO)

  #TFrecordIO
  output6 = (
      output
      | "encode" >> beam.Map(lambda s: json.dumps(s).encode('utf-8'))
      | 'WriteToTFRecord' >> beam.io.WriteToTFRecord(
          file_path_prefix="__output__/output_tfrecord",
          #shard_name_template='-V-SSSSS-of-NNNNN',
          file_name_suffix=".tfrecord",
          num_shards=2,
          triggering_frequency=5))
  _ = output6 | 'LogElements after WriteToTFRecord' >> LogElements(
      prefix='after WriteToTFRecord 6 ', with_window=True, level=logging.INFO)

  # Execute the pipeline and return the result.
  result = p.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
