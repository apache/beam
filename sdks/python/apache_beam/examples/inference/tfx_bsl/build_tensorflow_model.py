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

# Intended only for internal testing.

from typing import Dict
from typing import Optional

import tensorflow as tf


class TFModelWrapperWithSignature(tf.keras.Model):
  def __init__(
      self,
      model,
      preprocess_input=None,
      input_dtype=None,
      feature_description=None,
      **kwargs):
    super().__init__()
    self.model = model
    self.preprocess_input = preprocess_input
    self.input_dtype = input_dtype
    self.feature_description = feature_description
    if not feature_description:
      self.feature_description = {'image': tf.io.FixedLenFeature((), tf.string)}
    self._kwargs = kwargs

  @tf.function(input_signature=[tf.TensorSpec(shape=[None], dtype=tf.string)])
  def call(self, serialized_examples):
    features = tf.io.parse_example(
        serialized_examples, features=self.feature_description)

    # Initialize a TensorArray to store the deserialized values.
    # For more details, please look at
    # https://github.com/tensorflow/tensorflow/issues/39323#issuecomment-627586602
    batch = len(features['image'])
    deserialized_vectors = tf.TensorArray(
        self.input_dtype, size=batch, dynamic_size=True)
    # Vectorized version of tf.io.parse_tensor is not available.
    # Use for loop to vectorize the tensor. For more details, refer
    # https://github.com/tensorflow/tensorflow/issues/43706
    for i in range(batch):
      deserialized_value = tf.io.parse_tensor(
          features['image'][i], out_type=self.input_dtype)
      # http://github.com/tensorflow/tensorflow/issues/30409#issuecomment-508962873
      # In Graph mode, return value must get assigned in order to
      # update the array
      deserialized_vectors = deserialized_vectors.write(i, deserialized_value)

    deserialized_tensor = deserialized_vectors.stack()
    if self.preprocess_input:
      deserialized_tensor = self.preprocess_input(deserialized_tensor)
    return self.model(deserialized_tensor, **self._kwargs)


def save_tf_model_with_signature(
    path_to_save_model,
    model=None,
    preprocess_input=None,
    input_dtype=tf.float32,
    feature_description: Optional[Dict] = None,
    **kwargs,
):
  """
  Helper function used to save the Tensorflow Model with a serving signature.
  This is intended only for internal testing.
  Args:
   path_to_save_model: Path to save the model with modified signature.
   model: Base tensorflow model used for TFX-BSL RunInference transform.
   preprocess_input: Preprocess method to be included as part of the
   Model's serving signature.
   input_dtype: dtype of the inputs to the model.
   feature_description: Feature spec to parse inputs from tf.train.Example.
  """
  if not model:
    model = tf.keras.applications.MobileNetV2(weights='imagenet')
    preprocess_input = tf.keras.applications.mobilenet_v2.preprocess_input
  signature_model = TFModelWrapperWithSignature(
      model=model,
      preprocess_input=preprocess_input,
      input_dtype=input_dtype,
      feature_description=feature_description,
      **kwargs)
  tf.saved_model.save(signature_model, path_to_save_model)
