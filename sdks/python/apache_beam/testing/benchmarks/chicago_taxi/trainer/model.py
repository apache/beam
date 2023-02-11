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

"""Defines the model used to predict who will tip in the Chicago Taxi demo."""
# pytype: skip-file

import tensorflow as tf
import tensorflow_model_analysis as tfma
from tensorflow import estimator as tf_estimator
from trainer import taxi


def build_estimator(tf_transform_output, config, hidden_units=None):
  """Build an estimator for predicting the tipping behavior of taxi riders.

  Args:
    tf_transform_output: A TFTransformOutput.
    config: tf.contrib.learn.RunConfig defining the runtime environment for the
      estimator (including model_dir).
    hidden_units: [int], the layer sizes of the DNN (input layer first)

  Returns:
    Resulting DNNLinearCombinedClassifier.
  """
  transformed_feature_spec = (
      tf_transform_output.transformed_feature_spec().copy())

  transformed_feature_spec.pop(taxi.transformed_name(taxi.LABEL_KEY))

  real_valued_columns = [
      tf.feature_column.numeric_column(key, shape=())
      for key in taxi.transformed_names(taxi.DENSE_FLOAT_FEATURE_KEYS)
  ]
  categorical_columns = [
      tf.feature_column.categorical_column_with_identity(
          key, num_buckets=taxi.VOCAB_SIZE + taxi.OOV_SIZE, default_value=0)
      for key in taxi.transformed_names(taxi.VOCAB_FEATURE_KEYS)
  ]
  categorical_columns += [
      tf.feature_column.categorical_column_with_identity(
          key, num_buckets=taxi.FEATURE_BUCKET_COUNT, default_value=0)
      for key in taxi.transformed_names(taxi.BUCKET_FEATURE_KEYS)
  ]
  categorical_columns += [
      tf.feature_column.categorical_column_with_identity(
          key, num_buckets=num_buckets, default_value=0)
      for key, num_buckets in zip(
          taxi.transformed_names(taxi.CATEGORICAL_FEATURE_KEYS),  #
          taxi.MAX_CATEGORICAL_FEATURE_VALUES)
  ]
  return tf_estimator.DNNLinearCombinedClassifier(
      config=config,
      linear_feature_columns=categorical_columns,
      dnn_feature_columns=real_valued_columns,
      dnn_hidden_units=hidden_units or [100, 70, 50, 25])


def example_serving_receiver_fn(tf_transform_output, schema):
  """Build the serving in inputs.

  Args:
    tf_transform_output: A TFTransformOutput.
    schema: the schema of the input data.

  Returns:
    Tensorflow graph which parses examples, applying tf-transform to them.
  """
  raw_feature_spec = taxi.get_raw_feature_spec(schema)
  raw_feature_spec.pop(taxi.LABEL_KEY)

  raw_input_fn = tf_estimator.export.build_parsing_serving_input_receiver_fn(
      raw_feature_spec, default_batch_size=None)
  serving_input_receiver = raw_input_fn()

  transformed_features = tf_transform_output.transform_raw_features(
      serving_input_receiver.features)

  return tf_estimator.export.ServingInputReceiver(
      transformed_features, serving_input_receiver.receiver_tensors)


def eval_input_receiver_fn(tf_transform_output, schema):
  """Build everything needed for the tf-model-analysis to run the model.

  Args:
    tf_transform_output: A TFTransformOutput.
    schema: the schema of the input data.

  Returns:
    EvalInputReceiver function, which contains:
      - Tensorflow graph which parses raw untransformed features, applies the
        tf-transform preprocessing operators.
      - Set of raw, untransformed features.
      - Label against which predictions will be compared.
  """
  # Notice that the inputs are raw features, not transformed features here.
  raw_feature_spec = taxi.get_raw_feature_spec(schema)

  serialized_tf_example = tf.placeholder(
      dtype=tf.string, shape=[None], name='input_example_tensor')

  # Add a parse_example operator to the tensorflow graph, which will parse
  # raw, untransformed, tf examples.
  features = tf.parse_example(serialized_tf_example, raw_feature_spec)

  # Now that we have our raw examples, process them through the tf-transform
  # function computed during the preprocessing step.
  transformed_features = tf_transform_output.transform_raw_features(features)

  # The key name MUST be 'examples'.
  receiver_tensors = {'examples': serialized_tf_example}

  # NOTE: Model is driven by transformed features (since training works on the
  # materialized output of TFT, but slicing will happen on raw features.
  features.update(transformed_features)

  return tfma.export.EvalInputReceiver(
      features=features,
      receiver_tensors=receiver_tensors,
      labels=transformed_features[taxi.transformed_name(taxi.LABEL_KEY)])


def _gzip_reader_fn():
  """Small utility returning a record reader that can read gzip'ed files."""
  return tf.TFRecordReader(
      options=tf.python_io.TFRecordOptions(
          compression_type=tf.python_io.TFRecordCompressionType.GZIP))


def input_fn(filenames, tf_transform_output, batch_size=200):
  """Generates features and labels for training or evaluation.

  Args:
    filenames: [str] list of CSV files to read data from.
    tf_transform_output: A TFTransformOutput.
    batch_size: int First dimension size of the Tensors returned by input_fn

  Returns:
    A (features, indices) tuple where features is a dictionary of
      Tensors, and indices is a single Tensor of label indices.
  """
  transformed_feature_spec = (
      tf_transform_output.transformed_feature_spec().copy())

  transformed_features = tf.contrib.learn.io.read_batch_features(
      filenames, batch_size, transformed_feature_spec, reader=_gzip_reader_fn)

  # We pop the label because we do not want to use it as a feature while we're
  # training.
  return transformed_features, transformed_features.pop(
      taxi.transformed_name(taxi.LABEL_KEY))
