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

import apache_beam as beam
import tensorflow as tf
from apache_beam.examples.inference.tensorflow_mnist_classification import PostProcessor
from apache_beam.examples.inference.tensorflow_mnist_classification import parse_known_args
from apache_beam.examples.inference.tensorflow_mnist_classification import process_input
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorflow_inference import ModelType
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerNumpy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult


def get_model():
  inputs = tf.keras.layers.Input(shape=(28, 28, 1))
  x = tf.keras.layers.Conv2D(32, 3, activation="relu")(inputs)
  x = tf.keras.layers.Conv2D(32, 3, activation="relu")(x)
  x = tf.keras.layers.MaxPooling2D(2)(x)
  x = tf.keras.layers.Conv2D(64, 3, activation="relu")(x)
  x = tf.keras.layers.Conv2D(64, 3, activation="relu")(x)
  x = tf.keras.layers.MaxPooling2D(2)(x)
  x = tf.keras.layers.Flatten()(x)
  x = tf.keras.layers.Dropout(0.2)(x)
  outputs = tf.keras.layers.Dense(10, activation='softmax')(x)
  model = tf.keras.Model(inputs, outputs)
  return model


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  """
  Args:
    argv: Command line arguments defined for this example.
    save_main_session: Used for internal testing.
    test_pipeline: Used for internal testing.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # In this example we pass keyed inputs to RunInference transform.
  # Therefore, we use KeyedModelHandler wrapper over TFModelHandlerNumpy.
  model_loader = KeyedModelHandler(
      TFModelHandlerNumpy(
          model_uri=known_args.model_path,
          model_type=ModelType.SAVED_WEIGHTS,
          create_model_fn=get_model))

  pipeline = test_pipeline
  if not test_pipeline:
    pipeline = beam.Pipeline(options=pipeline_options)

  label_pixel_tuple = (
      pipeline
      | "ReadFromInput" >> beam.io.ReadFromText(known_args.input)
      | "PreProcessInputs" >> beam.Map(process_input))

  predictions = (
      label_pixel_tuple
      | "RunInference" >> RunInference(model_loader)
      | "PostProcessOutputs" >> beam.ParDo(PostProcessor()))

  _ = predictions | "WriteOutput" >> beam.io.WriteToText(
      known_args.output, shard_name_template='', append_trailing_newlines=True)

  result = pipeline.run()
  result.wait_until_finish()
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
