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

import logging
import argparse
import numpy as np

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
from apache_beam.ml.transforms.tft import Bucketize
from apache_beam.options.pipeline_options import PipelineOptions
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import schema_utils
import tensorflow as tf

NUM_NUMERIC_FEATURES = 13
max_vocab_size = 10_000_000
# Number of buckets for integer columns.
_NUM_BUCKETS = 10

NUMERIC_FEATURE_KEYS = ["int_feature_%d" % x for x in range(1, 14)]
CATEGORICAL_FEATURE_KEYS = ["categorical_feature_%d" % x for x in range(14, 40)]
LABEL_KEY = "clicked"

FEATURE_SPEC = dict([
    (name, tf.io.FixedLenFeature([], dtype=tf.int64))  # hex-to-int already done
    for name in CATEGORICAL_FEATURE_KEYS
] + [(name, tf.io.FixedLenFeature([], dtype=tf.float32))
     for name in NUMERIC_FEATURE_KEYS] +
                    [(LABEL_KEY, tf.io.FixedLenFeature([], tf.float32))])
INPUT_METADATA = dataset_metadata.DatasetMetadata(
    schema_utils.schema_from_feature_spec(FEATURE_SPEC))


class FillMissing(beam.DoFn):
  """Fills missing elements with zero string value."""
  def process(self, element):
    elem_list = element.split('\t')
    out_list = []
    for val in elem_list:
      new_val = "0" if not val else val
      out_list.append(new_val)
    yield ('\t').join(out_list)


class NegsToZeroLog(beam.DoFn):
  """For int features, sets negative values to zero and takes log(x+1)."""
  def process(self, element):
    elem_list = element.split('\t')

    def convert(val, index):
      if 0 < index <= NUM_NUMERIC_FEATURES:
        num = int(val)
        num = num if num >= 0 else 0
        return str(np.log1p(num))

    out_list = [convert(val, index) for index, val in enumerate(elem_list)]
    yield '\t'.join(out_list)


def convert_str_to_int(element):
  for key, value in element.items():
    if key in NUMERIC_FEATURE_KEYS:
      element[key] = float(value)
  return element


def parse_known_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='/usr/local/google/home/anandinguva/Downloads/train.txt')
  parser.add_argument(
      "--csv_delimeter",
      default="\t",
      help="Delimeter string for input and output.")
  parser.add_argument("--artifact_location", required=True)
  parser.add_argument(
      '--shuffle',
      action='store_true',
      dest='shuffle',
      default=True,
      help='Skips shuffling the data.')
  return parser.parse_known_args(argv)


def run(
    argv=None,
    test_pipeline=None  # for testing purposes.
):
  known_args, pipeline_args = parse_known_args(argv)
  options = PipelineOptions(flags=pipeline_args)
  data_path = known_args.input
  ordered_columns = [
      LABEL_KEY
  ] + NUMERIC_FEATURE_KEYS + CATEGORICAL_FEATURE_KEYS
  pipeline = test_pipeline
  pipeline = test_pipeline or beam.Pipeline(options=options)
  processed_lines = (
      pipeline
      # Read in TSV data.
      | beam.io.ReadFromText(data_path, coder=beam.coders.StrUtf8Coder())
      # Fill in missing elements with the defaults (zeros).
      | "FillMissing" >> beam.ParDo(FillMissing())
      # For numerical features, set negatives to zero. Then take log(x+1).
      | "NegsToZeroLog" >> beam.ParDo(NegsToZeroLog())
      | beam.Map(lambda x: str(x).split('\t'))
      | beam.Map(lambda x: {ordered_columns[i]: x[i]
                            for i in range(len(x))})
      | beam.Map(convert_str_to_int))

  if known_args.shuffle:
    processed_lines = processed_lines | beam.Reshuffle()

  artifact_location = known_args.artifact_location
  ml_transform = MLTransform(artifact_location=artifact_location)
  ml_transform.with_transform(
      ComputeAndApplyVocabulary(columns=CATEGORICAL_FEATURE_KEYS))
  ml_transform.with_transform(
      Bucketize(columns=NUMERIC_FEATURE_KEYS, num_buckets=_NUM_BUCKETS))

  transformed_lines = (processed_lines | 'MLTransform' >> ml_transform)

  _ = transformed_lines | beam.Map(logging.info)

  return transformed_lines


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
