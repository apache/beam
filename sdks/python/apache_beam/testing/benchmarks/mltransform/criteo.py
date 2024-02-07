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
# pylint: skip-file

"""
This example demonstrates the use of MLTransform to preprocess text data using
ComputeAndApplyVocabulary.

This examples follows https://github.com/tensorflow/models/blob/master/official/recommendation/ranking/preprocessing/criteo_preprocess.py
but the instead of tensorflow-transform, it uses Apache Beam's MLTransform.
MLTransform abstracts the user away from providing tensorflow-transform's
schema and making it easier for users to use it with Apache Beam.
"""

import logging
import argparse
import numpy as np

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary
from apache_beam.ml.transforms.tft import Bucketize
from apache_beam.options.pipeline_options import PipelineOptions

NUM_NUMERIC_FEATURES = 13
# Number of buckets for integer columns.
_NUM_BUCKETS = 10
csv_delimiter = '\t'

NUMERIC_FEATURE_KEYS = ["int_feature_%d" % x for x in range(1, 14)]
CATEGORICAL_FEATURE_KEYS = ["categorical_feature_%d" % x for x in range(14, 40)]
LABEL_KEY = "clicked"
MAX_VOCAB_SIZE = 5000000


class FillMissing(beam.DoFn):
  """Fills missing elements with zero string value."""
  def process(self, element):
    elem_list = element.split(csv_delimiter)
    out_list = []
    for val in elem_list:
      new_val = "0" if not val else val
      out_list.append(new_val)
    yield (csv_delimiter).join(out_list)


class NegsToZeroLog(beam.DoFn):
  """For int features, sets negative values to zero and takes log(x+1)."""
  def process(self, element):
    elem_list = element.split(csv_delimiter)
    out_list = []
    for i, val in enumerate(elem_list):
      if 0 < i <= NUM_NUMERIC_FEATURES:
        val = "0" if int(val) < 0 else val
        val = str(np.log(int(val) + 1))
      out_list.append(val)
    yield (csv_delimiter).join(out_list)


class HexToIntModRange(beam.DoFn):
  """For categorical features, takes decimal value and mods with max value."""
  def process(self, element):
    elem_list = element.split(csv_delimiter)
    out_list = []
    for i, val in enumerate(elem_list):
      if i > NUM_NUMERIC_FEATURES:
        new_val = int(val, 16) % MAX_VOCAB_SIZE
      else:
        new_val = val
      out_list.append(str(new_val))
    yield str.encode((csv_delimiter).join(out_list))


def parse_known_args(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='/usr/local/google/home/anandinguva/Downloads/train.txt')
  parser.add_argument(
      "--artifact_location", help="Artifact location to store artifacts.")
  return parser.parse_known_args(argv)


def run(argv=None):
  known_args, pipeline_args = parse_known_args(argv)
  options = PipelineOptions(flags=pipeline_args)
  data_path = known_args.input
  ordered_columns = [
      LABEL_KEY
  ] + NUMERIC_FEATURE_KEYS + CATEGORICAL_FEATURE_KEYS
  with beam.Pipeline(options=options) as pipeline:
    processed_lines = (
        pipeline
        # Read in TSV data.
        | beam.io.ReadFromText(data_path, coder=beam.coders.StrUtf8Coder())
        # Fill in missing elements with the defaults (zeros).
        | "FillMissing" >> beam.ParDo(FillMissing())
        # For numerical features, set negatives to zero. Then take log(x+1).
        | "NegsToZeroLog" >> beam.ParDo(NegsToZeroLog())
        # For categorical features, mod the values with vocab size.
        | "HexToIntModRange" >> beam.ParDo(HexToIntModRange()))

    transformed_lines = (
        processed_lines
        | "MLTransform" >>
        MLTransform(write_artifact_location=known_args.artifact_location).
        with_transform(
            ComputeAndApplyVocabulary(
                columns=CATEGORICAL_FEATURE_KEYS, frequency_threshold=5)
        ).with_transform(
            Bucketize(columns=NUMERIC_FEATURE_KEYS, num_buckets=_NUM_BUCKETS)))

    # TODO: Write to CSV.
    transformed_lines | beam.Map(logging.info)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
