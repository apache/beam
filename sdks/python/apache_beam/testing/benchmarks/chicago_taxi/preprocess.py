# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Preprocessor applying tf.transform to the chicago_taxi data."""
# pytype: skip-file

from __future__ import absolute_import, division, print_function

import argparse
import os

import tensorflow as tf
import tensorflow_transform as transform
import tensorflow_transform.beam as tft_beam
from tensorflow_transform.coders import example_proto_coder
from tensorflow_transform.tf_metadata import dataset_metadata, dataset_schema

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.load_tests.load_test_metrics_utils import (
    MeasureTime, MetricsReader)
from trainer import taxi


def _fill_in_missing(x):
  """Replace missing values in a SparseTensor.

  Fills in missing values of `x` with '' or 0, and converts to a dense tensor.

  Args:
    x: A `SparseTensor` of rank 2.  Its dense shape should have size at most 1
      in the second dimension.

  Returns:
    A rank 1 tensor where missing values of `x` have been filled in.
  """
  default_value = '' if x.dtype == tf.string else 0
  return tf.squeeze(
      tf.sparse.to_dense(
          tf.SparseTensor(x.indices, x.values, [x.dense_shape[0], 1]),
          default_value),
      axis=1)


def transform_data(
    input_handle,
    outfile_prefix,
    working_dir,
    schema_file,
    transform_dir=None,
    max_rows=None,
    pipeline_args=None,
    publish_to_bq=False,
    project=None,
    metrics_table=None,
    metrics_dataset=None):
  """The main tf.transform method which analyzes and transforms data.

  Args:
    input_handle: BigQuery table name to process specified as DATASET.TABLE or
      path to csv file with input data.
    outfile_prefix: Filename prefix for emitted transformed examples
    working_dir: Directory in which transformed examples and transform function
      will be emitted.
    schema_file: An file path that contains a text-serialized TensorFlow
      metadata schema of the input data.
    transform_dir: Directory in which the transform output is located. If
      provided, this will load the transform_fn from disk instead of computing
      it over the data. Hint: this is useful for transforming eval data.
    max_rows: Number of rows to query from BigQuery
    pipeline_args: additional DataflowRunner or DirectRunner args passed to the
      beam pipeline.
  """
  def preprocessing_fn(inputs):
    """tf.transform's callback function for preprocessing inputs.

    Args:
      inputs: map from feature keys to raw not-yet-transformed features.

    Returns:
      Map from string feature key to transformed feature operations.
    """
    outputs = {}
    for key in taxi.DENSE_FLOAT_FEATURE_KEYS:
      # Preserve this feature as a dense float, setting nan's to the mean.
      outputs[taxi.transformed_name(key)] = transform.scale_to_z_score(
          _fill_in_missing(inputs[key]))

    for key in taxi.VOCAB_FEATURE_KEYS:
      # Build a vocabulary for this feature.
      outputs[taxi.transformed_name(
          key)] = transform.compute_and_apply_vocabulary(
              _fill_in_missing(inputs[key]),
              top_k=taxi.VOCAB_SIZE,
              num_oov_buckets=taxi.OOV_SIZE)

    for key in taxi.BUCKET_FEATURE_KEYS:
      outputs[taxi.transformed_name(key)] = transform.bucketize(
          _fill_in_missing(inputs[key]), taxi.FEATURE_BUCKET_COUNT)

    for key in taxi.CATEGORICAL_FEATURE_KEYS:
      outputs[taxi.transformed_name(key)] = _fill_in_missing(inputs[key])

    # Was this passenger a big tipper?
    taxi_fare = _fill_in_missing(inputs[taxi.FARE_KEY])
    tips = _fill_in_missing(inputs[taxi.LABEL_KEY])
    outputs[taxi.transformed_name(taxi.LABEL_KEY)] = tf.where(
        tf.is_nan(taxi_fare),
        tf.cast(tf.zeros_like(taxi_fare), tf.int64),
        # Test if the tip was > 20% of the fare.
        tf.cast(
            tf.greater(tips, tf.multiply(taxi_fare, tf.constant(0.2))),
            tf.int64))

    return outputs

  namespace = metrics_table
  metrics_monitor = None
  if publish_to_bq:
    metrics_monitor = MetricsReader(
        publish_to_bq=publish_to_bq,
        project_name=project,
        bq_table=metrics_table,
        bq_dataset=metrics_dataset,
        namespace=namespace,
        filters=MetricsFilter().with_namespace(namespace))
  schema = taxi.read_schema(schema_file)
  raw_feature_spec = taxi.get_raw_feature_spec(schema)
  raw_schema = dataset_schema.from_feature_spec(raw_feature_spec)
  raw_data_metadata = dataset_metadata.DatasetMetadata(raw_schema)

  pipeline = beam.Pipeline(argv=pipeline_args)
  with tft_beam.Context(temp_dir=working_dir):
    query = taxi.make_sql(input_handle, max_rows, for_eval=False)
    raw_data = (
        pipeline
        | 'ReadBigQuery' >> ReadFromBigQuery(
            query=query, project=project, use_standard_sql=True)
        | 'Measure time: start' >> beam.ParDo(MeasureTime(namespace)))
    decode_transform = beam.Map(
        taxi.clean_raw_data_dict, raw_feature_spec=raw_feature_spec)

    if transform_dir is None:
      decoded_data = raw_data | 'DecodeForAnalyze' >> decode_transform
      transform_fn = ((decoded_data, raw_data_metadata) |
                      ('Analyze' >> tft_beam.AnalyzeDataset(preprocessing_fn)))

      _ = (
          transform_fn |
          ('WriteTransformFn' >> tft_beam.WriteTransformFn(working_dir)))
    else:
      transform_fn = pipeline | tft_beam.ReadTransformFn(transform_dir)

    # Shuffling the data before materialization will improve Training
    # effectiveness downstream. Here we shuffle the raw_data (as opposed to
    # decoded data) since it has a compact representation.
    shuffled_data = raw_data | 'RandomizeData' >> beam.transforms.Reshuffle()

    decoded_data = shuffled_data | 'DecodeForTransform' >> decode_transform
    (transformed_data,
     transformed_metadata) = (((decoded_data, raw_data_metadata), transform_fn)
                              | 'Transform' >> tft_beam.TransformDataset())

    coder = example_proto_coder.ExampleProtoCoder(transformed_metadata.schema)
    _ = (
        transformed_data
        | 'SerializeExamples' >> beam.Map(coder.encode)
        | 'Measure time: end' >> beam.ParDo(MeasureTime(namespace))
        | 'WriteExamples' >> beam.io.WriteToTFRecord(
            os.path.join(working_dir, outfile_prefix), file_name_suffix='.gz'))
  result = pipeline.run()
  result.wait_until_finish()
  if metrics_monitor:
    metrics_monitor.publish_metrics(result)


def main():
  tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.INFO)
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      help=('Input BigQuery table to process specified as: '
            'DATASET.TABLE'))

  parser.add_argument(
      '--schema_file', help='File holding the schema for the input data')

  parser.add_argument(
      '--output_dir',
      help=(
          'Directory in which transformed examples and function '
          'will be emitted.'))

  parser.add_argument(
      '--outfile_prefix',
      help='Filename prefix for emitted transformed examples')

  parser.add_argument(
      '--transform_dir',
      required=False,
      default=None,
      help='Directory in which the transform output is located')

  parser.add_argument(
      '--max_rows',
      help='Number of rows to query from BigQuery',
      default=None,
      type=int)
  parser.add_argument(
      '--publish_to_big_query',
      help='Whether to publish to BQ',
      default=None,
      type=bool)

  parser.add_argument(
      '--metrics_dataset', help='BQ dataset', default=None, type=str)

  parser.add_argument(
      '--metrics_table', help='BQ table', default=None, type=str)

  parser.add_argument(
      '--metric_reporting_project',
      help='BQ table project',
      default=None,
      type=str)

  known_args, pipeline_args = parser.parse_known_args()
  transform_data(
      input_handle=known_args.input,
      outfile_prefix=known_args.outfile_prefix,
      working_dir=known_args.output_dir,
      schema_file=known_args.schema_file,
      transform_dir=known_args.transform_dir,
      max_rows=known_args.max_rows,
      pipeline_args=pipeline_args,
      publish_to_bq=known_args.publish_to_big_query,
      metrics_dataset=known_args.metrics_dataset,
      metrics_table=known_args.metrics_table,
      project=known_args.metric_reporting_project)


if __name__ == '__main__':
  main()
