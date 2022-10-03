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

import tensorflow as tf

DATA_SPEC = {
    'image': tf.io.FixedLenFeature((), tf.string),
}
_D_TYPE = tf.int32

base_model = tf.keras.applications.MobileNetV2(weights='imagenet')


@tf.function(input_signature=[tf.TensorSpec(shape=[None], dtype=tf.string)])
def serve_tf_example(serialized_tf_examples):
  features = tf.io.parse_example(serialized_tf_examples, features=DATA_SPEC)

  # TODO: Pass the output type as serialized value through tf.train.Example
  # using TensorArray as suggested at
  # https://github.com/tensorflow/tensorflow/issues/39323#issuecomment-627586602
  batch = len(features['image'])
  deserialized_vectors = tf.TensorArray(_D_TYPE, size=batch, dynamic_size=True)
  # issue arise with the indexing at features['image']
  # Vectorized version of tf.io.parse_tensor is not available
  # https://github.com/tensorflow/tensorflow/issues/43706
  for i in range(batch):
    deserialized_value = tf.io.parse_tensor(
        features['image'][i], out_type=_D_TYPE)

    # http://github.com/tensorflow/tensorflow/issues/30409#issuecomment-508962873
    # In Graph mode, return value must get assigned in order to update the array
    deserialized_vectors = deserialized_vectors.write(i, deserialized_value)

  # deserialized_value = tf.expand_dims(deserialized_vectors, axis=0)
  deserialized_tensor = deserialized_vectors.stack()
  preprocessed_tensor = tf.keras.applications.mobilenet_v2.preprocess_input(
      deserialized_tensor)
  return base_model(preprocessed_tensor, training=False)


signature = {'serving_default': serve_tf_example}
tf.keras.models.save_model(base_model, '/tmp/tf', signatures=signature)
