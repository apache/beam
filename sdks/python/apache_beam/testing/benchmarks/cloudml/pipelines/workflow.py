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
import logging
import os

import apache_beam as beam
from apache_beam.testing.benchmarks.cloudml.criteo_tft import criteo
from apache_beam.testing.benchmarks.cloudml.pipelines import tfma_pipeline
import tensorflow_data_validation as tfdv
import tensorflow_transform as tft
from tensorflow_transform import coders
import tensorflow_transform.beam as tft_beam
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import schema_utils
from tfx_bsl.public import tfxio

# Name of the column for the synthetic version of the benchmark.
_SYNTHETIC_COLUMN = 'x'


class _RecordBatchToPyDict(beam.PTransform):
  """Converts PCollections of pa.RecordBatch to python dicts."""
  def __init__(self, input_feature_spec):
    self._input_feature_spec = input_feature_spec

  def expand(self, pcoll):
    def format_values(instance):
      return {
          k: v.squeeze(0).tolist()
          if v is not None else self._input_feature_spec[k].default_value
          for k,
          v in instance.items()
      }

    return (
        pcoll
        | 'RecordBatchToDicts' >>
        beam.FlatMap(lambda x: x.to_pandas().to_dict(orient='records'))
        | 'FormatPyDictValues' >> beam.Map(format_values))


def _synthetic_preprocessing_fn(inputs):
  return {
      _SYNTHETIC_COLUMN: tft.compute_and_apply_vocabulary(
          inputs[_SYNTHETIC_COLUMN],

          # Execute more codepaths but do no frequency filtration.
          frequency_threshold=1,

          # Execute more codepaths but do no top filtration.
          top_k=2**31 - 1,

          # Execute more codepaths
          num_oov_buckets=10)
  }


class _PredictionHistogramFn(beam.DoFn):
  def __init__(self):
    # Beam Metrics API for Distributions only works with integers but
    # predictions are floating point numbers. We thus store a "quantized"
    # distribution of the prediction with sufficient granularity and for ease
    # of human interpretation (eg as a percentage for logistic regression).
    self._prediction_distribution = beam.metrics.Metrics.distribution(
        self.__class__, 'int(scores[0]*100)')

  def process(self, element):
    self._prediction_distribution.update(int(element['scores'][0] * 100))


def setup_pipeline(p, args):

  preprocessing_fn = None
  if args.classifier == 'criteo':
    if args.benchmark_type == 'tfma':
      # These benchmarks use golden models that expect input conforming to the
      # legacy input schema.
      # legacy input schema.
      input_feature_spec = criteo.make_legacy_input_feature_spec()
      # input_feature_spec = criteo.make_input_feature_spec()
    else:
      assert args.benchmark_type in ('tfdv', 'tft')
      input_feature_spec = criteo.make_input_feature_spec()
    input_schema = schema_utils.schema_from_feature_spec(input_feature_spec)
    input_tfxio = tfxio.BeamRecordCsvTFXIO(
        physical_format='text',
        column_names=criteo.make_ordered_column_names(),
        schema=input_schema,
        delimiter=criteo.DEFAULT_DELIMITER,
        telemetry_descriptors=['CriteoCloudMLBenchmark'])
    preprocessing_fn = criteo.make_preprocessing_fn(args.frequency_threshold)
  else:
    assert False, 'Unknown args classifier <{}>'.format(args.classifier)

  input_data = p | 'ReadFromText' >> beam.io.textio.ReadFromText(
      args.input, coder=beam.coders.BytesCoder())

  if args.benchmark_type == 'tfdv':
    logging.info('TFDV benchmark')
    assert args.classifier == 'criteo'

    # Convert the dataset schema into a schema proto, which TFDV uses to infer
    # types when decoding CSV.
    _ = (
        input_data
        | 'Decode' >> input_tfxio.BeamSource()
        | 'GenerateStatistics' >> tfdv.GenerateStatistics()
        | 'Write' >> beam.io.WriteToText(
            os.path.join(args.output, 'feature_stats')))

  elif args.benchmark_type == 'tfma':
    logging.info('TFMA benchmark')
    assert args.classifier == 'criteo'
    assert args.model_dir
    # Convert to tf.Example for TFMA.
    example_coder = coders.ExampleProtoCoder(input_schema)

    def remove_empty_features(serialized_example):
      """Remove columns with empty feature list."""
      # From beam: "imports, functions and other variables defined in the global
      # context of your __main__ file of your Dataflow pipeline are, by default,
      # not available in the worker execution environment, and such references
      # will cause a NameError, unless the --save_main_session pipeline option
      # is set to True. Please see
      # https://cloud.google.com/dataflow/faq#how-do-i-handle-nameerrors ."
      import tensorflow.compat.v1 as tf  # pylint:disable=redefined-outer-name,reimported
      result = tf.train.Example()
      result.ParseFromString(serialized_example)
      feature_map = result.features.feature
      for k in feature_map.keys():
        feature = feature_map[k]
        kind = feature.WhichOneof('kind')
        if kind == 'int64_list':
          value = feature.int64_list.value
        elif kind == 'bytes_list':
          value = feature.bytes_list.value
        else:
          value = feature.float_list.value
        if not value:
          feature_map.pop(k)
      return result.SerializeToString()

    example_data = (
        input_data
        | 'Decode' >> input_tfxio.BeamSource()
        | 'RecordBatchToDicts' >> _RecordBatchToPyDict(input_feature_spec)
        | 'Encode' >> beam.Map(example_coder.encode)
        | 'RemoveEmptyFeatureList' >> beam.Map(remove_empty_features))
    tfma_pipeline.setup_tfma_pipeline(args, example_data)
  elif args.benchmark_type == 'tft':
    logging.info('TFT benchmark')

    # Setting TFXIO output format only for Criteo benchmarks to make sure that
    # both codepaths are covered.
    output_record_batches = args.classifier == 'criteo'

    # pylint: disable=expression-not-assigned
    input_metadata = dataset_metadata.DatasetMetadata(schema=input_schema)
    (
        input_metadata
        | 'WriteInputMetadata' >> tft_beam.WriteMetadata(
            os.path.join(args.output, 'raw_metadata'), pipeline=p))

    with tft_beam.Context(temp_dir=os.path.join(args.output, 'tmp'),
                          use_deep_copy_optimization=True):
      decoded_input_data = (
          input_data | 'DecodeForAnalyze' >> input_tfxio.BeamSource())
      transform_fn = ((decoded_input_data, input_tfxio.TensorAdapterConfig())
                      | 'Analyze' >> tft_beam.AnalyzeDataset(preprocessing_fn))

    if args.shuffle:
      # Shuffle the data before any decoding (more compact representation).
      input_data |= 'Shuffle' >> beam.transforms.Reshuffle()  # pylint: disable=no-value-for-parameter

    decoded_input_data = (
        input_data | 'DecodeForTransform' >> input_tfxio.BeamSource())
    (dataset,
     metadata) = ((decoded_input_data, input_tfxio.TensorAdapterConfig()),
                  transform_fn) | 'Transform' >> tft_beam.TransformDataset(
                      output_record_batches=output_record_batches)

    if output_record_batches:

      def record_batch_to_examples(batch, unary_passthrough_features):
        """Encodes transformed data as tf.Examples."""
        # Ignore unary pass-through features.
        del unary_passthrough_features
        # From beam: "imports, functions and other variables defined in the
        # global context of your __main__ file of your Dataflow pipeline are, by
        # default, not available in the worker execution environment, and such
        # references will cause a NameError, unless the --save_main_session
        # pipeline option is set to True. Please see
        # https://cloud.google.com/dataflow/faq#how-do-i-handle-nameerrors ."
        from tfx_bsl.coders.example_coder import RecordBatchToExamples
        return RecordBatchToExamples(batch)

      encode_ptransform = beam.FlatMapTuple(record_batch_to_examples)
    else:
      example_coder = coders.ExampleProtoCoder(metadata.schema)
      encode_ptransform = beam.Map(example_coder.encode)

    # TODO: Use WriteDataset instead when it becomes available.
    (
        dataset
        | 'Encode' >> encode_ptransform
        | 'Write' >> beam.io.WriteToTFRecord(
            os.path.join(args.output, 'features_train'),
            file_name_suffix='.tfrecord.gz'))

    transform_fn | 'WriteTransformFn' >> tft_beam.WriteTransformFn(args.output)

    # TODO: Remember to eventually also save the statistics.
  else:
    logging.fatal('Unknown benchmark type: %s', args.benchmark_type)


def parse_known_args(argv):
  """Parses args for this workflow."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input path for input files.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output path for output files.')
  parser.add_argument(
      '--model_dir',
      dest='model_dir',
      required=False,
      help=(
          'The model dir to store the model. If specified, '
          'prediction would be performed. So far we use the '
          'same training data for prediction.'))
  parser.add_argument(
      '--classifier',
      dest='classifier',
      required=True,
      help='Name of classifier to use.')
  parser.add_argument(
      '--frequency_threshold',
      dest='frequency_threshold',
      default=5,  # TODO: Align default with TFT (ie 0).
      help='Threshold for minimum number of unique values for a category.')
  parser.add_argument(
      '--no_shuffle',
      dest='shuffle',
      default=True,
      action='store_false',
      help='Skips shuffling the data.')
  parser.add_argument(
      '--benchmark_type',
      dest='benchmark_type',
      required=True,
      help='Type of benchmark to run.')

  return parser.parse_known_args(argv)


def run(argv=None):
  """Main entry point; defines and runs the pipeline."""
  known_args, pipeline_args = parse_known_args(argv)
  with beam.Pipeline(argv=pipeline_args) as p:
    setup_pipeline(p, known_args)


if __name__ == '__main__':
  run()
