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

"""Trainer for the chicago_taxi demo."""
# pytype: skip-file

import argparse
import os

import tensorflow as tf
import tensorflow_model_analysis as tfma
import tensorflow_transform as tft
from tensorflow import estimator as tf_estimator
from trainer import model
from trainer import taxi

SERVING_MODEL_DIR = 'serving_model_dir'
EVAL_MODEL_DIR = 'eval_model_dir'

TRAIN_BATCH_SIZE = 40
EVAL_BATCH_SIZE = 40

# Number of nodes in the first layer of the DNN
FIRST_DNN_LAYER_SIZE = 100
NUM_DNN_LAYERS = 4
DNN_DECAY_FACTOR = 0.7


def train_and_maybe_evaluate(hparams):
  """Run the training and evaluate using the high level API.

  Args:
    hparams: Holds hyperparameters used to train the model as name/value pairs.

  Returns:
    The estimator that was used for training (and maybe eval)
  """
  schema = taxi.read_schema(hparams.schema_file)
  tf_transform_output = tft.TFTransformOutput(hparams.tf_transform_dir)

  train_input = lambda: model.input_fn(
      hparams.train_files, tf_transform_output, batch_size=TRAIN_BATCH_SIZE)

  eval_input = lambda: model.input_fn(
      hparams.eval_files, tf_transform_output, batch_size=EVAL_BATCH_SIZE)

  train_spec = tf_estimator.TrainSpec(
      train_input, max_steps=hparams.train_steps)

  serving_receiver_fn = lambda: model.example_serving_receiver_fn(
      tf_transform_output, schema)

  exporter = tf_estimator.FinalExporter('chicago-taxi', serving_receiver_fn)
  eval_spec = tf_estimator.EvalSpec(
      eval_input,
      steps=hparams.eval_steps,
      exporters=[exporter],
      name='chicago-taxi-eval')

  run_config = tf_estimator.RunConfig(
      save_checkpoints_steps=999, keep_checkpoint_max=1)

  serving_model_dir = os.path.join(hparams.output_dir, SERVING_MODEL_DIR)
  run_config = run_config.replace(model_dir=serving_model_dir)

  estimator = model.build_estimator(
      tf_transform_output,

      # Construct layers sizes with exponetial decay
      hidden_units=[
          max(2, int(FIRST_DNN_LAYER_SIZE * DNN_DECAY_FACTOR**i))
          for i in range(NUM_DNN_LAYERS)
      ],
      config=run_config)

  tf_estimator.train_and_evaluate(estimator, train_spec, eval_spec)

  return estimator


def run_experiment(hparams):
  """Train the model then export it for tf.model_analysis evaluation.

  Args:
    hparams: Holds hyperparameters used to train the model as name/value pairs.
  """
  estimator = train_and_maybe_evaluate(hparams)

  schema = taxi.read_schema(hparams.schema_file)
  tf_transform_output = tft.TFTransformOutput(hparams.tf_transform_dir)

  # Save a model for tfma eval
  eval_model_dir = os.path.join(hparams.output_dir, EVAL_MODEL_DIR)

  receiver_fn = lambda: model.eval_input_receiver_fn(
      tf_transform_output, schema)

  tfma.export.export_eval_savedmodel(
      estimator=estimator,
      export_dir_base=eval_model_dir,
      eval_input_receiver_fn=receiver_fn)


def main():
  parser = argparse.ArgumentParser()
  # Input Arguments
  parser.add_argument(
      '--train-files',
      help='GCS or local paths to training data',
      nargs='+',
      required=True)

  parser.add_argument(
      '--tf-transform-dir',
      help='Tf-transform directory with model from preprocessing step',
      required=True)

  parser.add_argument(
      '--output-dir',
      help="""\
          Directory under which which the serving model (under /serving_model_dir)\
          and the tf-mode-analysis model (under /eval_model_dir) will be written\
          """,
      required=True)

  parser.add_argument(
      '--eval-files',
      help='GCS or local paths to evaluation data',
      nargs='+',
      required=True)
  # Training arguments
  parser.add_argument(
      '--job-dir',
      help='GCS location to write checkpoints and export models',
      required=True)

  # Argument to turn on all logging
  parser.add_argument(
      '--verbosity',
      choices=['DEBUG', 'ERROR', 'FATAL', 'INFO', 'WARN'],
      default='INFO',
  )
  # Experiment arguments
  parser.add_argument(
      '--train-steps',
      help='Count of steps to run the training job for',
      required=True,
      type=int)
  parser.add_argument(
      '--eval-steps',
      help='Number of steps to run evalution for at each checkpoint',
      default=100,
      type=int)
  parser.add_argument(
      '--schema-file', help='File holding the schema for the input data')

  args = parser.parse_args()

  # Set python level verbosity
  tf.compat.v1.logging.set_verbosity(args.verbosity)
  # Set C++ Graph Execution level verbosity
  os.environ['TF_CPP_MIN_LOG_LEVEL'] = str(
      getattr(tf.compat.v1.logging, args.verbosity) / 10)

  # Run the training job
  hparams = tf.contrib.training.HParams(**args.__dict__)
  run_experiment(hparams)


if __name__ == '__main__':
  main()
