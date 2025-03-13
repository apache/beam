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

"""Schema and transform definition for the Criteo dataset."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow_transform as tft


def _get_raw_categorical_column_name(column_idx):
  return 'categorical-feature-{}'.format(column_idx)


def get_transformed_categorical_column_name(column_name_or_id):
  if isinstance(column_name_or_id, bytes):
    # assume the input is column name
    column_name = column_name_or_id
  else:
    # assume the input is column id
    column_name = _get_raw_categorical_column_name(column_name_or_id)
  return column_name + '_id'


_INTEGER_COLUMN_NAMES = [
    'int-feature-{}'.format(column_idx) for column_idx in range(1, 14)
]
_CATEGORICAL_COLUMN_NAMES = [
    _get_raw_categorical_column_name(column_idx)
    for column_idx in range(14, 40)
]
DEFAULT_DELIMITER = '\t'
# Number of buckets for integer columns.
_NUM_BUCKETS = 10

# Schema annotations aren't supported in this build.
tft.common.IS_ANNOTATIONS_PB_AVAILABLE = False


def make_ordered_column_names(include_label=True):
  """Returns the column names in the dataset in the order as they appear.

  Args:
    include_label: Indicates whether the label feature should be included.
  Returns:
    A list of column names in the dataset.
  """
  result = ['clicked'] if include_label else []
  for name in _INTEGER_COLUMN_NAMES:
    result.append(name)
  for name in _CATEGORICAL_COLUMN_NAMES:
    result.append(name)
  return result


def make_legacy_input_feature_spec(include_label=True):
  """Input schema definition.

  Args:
    include_label: Indicates whether the label feature should be included.
  Returns:
    A `Schema` object.
  """
  result = {}
  if include_label:
    result['clicked'] = tf.io.FixedLenFeature(shape=[], dtype=tf.int64)
  for name in _INTEGER_COLUMN_NAMES:
    result[name] = tf.io.FixedLenFeature(
        shape=[], dtype=tf.int64, default_value=-1)
  for name in _CATEGORICAL_COLUMN_NAMES:
    result[name] = tf.io.FixedLenFeature(
        shape=[], dtype=tf.string, default_value='')
  return result


def make_input_feature_spec(include_label=True):
  """Input schema definition.

  Args:
    include_label: Indicates whether the label feature should be included.

  Returns:
    A `Schema` object.
  """
  result = {}
  if include_label:
    result['clicked'] = tf.io.FixedLenFeature(shape=[], dtype=tf.int64)
  for name in _INTEGER_COLUMN_NAMES:
    result[name] = tf.io.VarLenFeature(dtype=tf.int64)

  for name in _CATEGORICAL_COLUMN_NAMES:
    result[name] = tf.io.VarLenFeature(dtype=tf.string)

  return result


def make_preprocessing_fn(frequency_threshold):
  """Creates a preprocessing function for criteo.

  Args:
    frequency_threshold: The frequency_threshold used when generating
      vocabularies for the categorical features.

  Returns:
    A preprocessing function.
  """
  def preprocessing_fn(inputs):
    """User defined preprocessing function for criteo columns.

    Args:
      inputs: dictionary of input `tensorflow_transform.Column`.
    Returns:
      A dictionary of `tensorflow_transform.Column` representing the transformed
          columns.
    """
    result = {'clicked': inputs['clicked']}
    for name in _INTEGER_COLUMN_NAMES:
      feature = inputs[name]
      # TODO(https://github.com/apache/beam/issues/24902):
      #  Replace this boilerplate with a helper function.
      # This is a SparseTensor because it is optional. Here we fill in a
      # default value when it is missing.
      feature = tft.sparse_tensor_to_dense_with_shape(
          feature, [None, 1], default_value=-1)
      # Reshaping from a batch of vectors of size 1 to a batch of scalars and
      # adding a bucketized version.
      feature = tf.squeeze(feature, axis=1)
      result[name] = feature
      result[name + '_bucketized'] = tft.bucketize(feature, _NUM_BUCKETS)
    for name in _CATEGORICAL_COLUMN_NAMES:
      feature = inputs[name]
      # Similar to for integer columns, but use '' as default.
      feature = tft.sparse_tensor_to_dense_with_shape(
          feature, [None, 1], default_value='')
      feature = tf.squeeze(feature, axis=1)
      result[get_transformed_categorical_column_name(
          name)] = tft.compute_and_apply_vocabulary(
              feature, frequency_threshold=frequency_threshold)

    return result

  return preprocessing_fn
