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

"""Simple training function that loads a pretrained model from the torch hub and saves it."""

import argparse
import time
from pathlib import Path

import torch


def parse_args():
  """Parse ingestion arguments."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--preprocessed-dataset-path",
      type=str,
      help="Path to the preprocessed dataset.",
      required=True)
  parser.add_argument(
      "--trained-model-path",
      type=str,
      help="Output path to the trained model.",
      required=True)
  parser.add_argument(
      "--base-artifact-path",
      type=str,
      help="Base path to store pipeline artifacts.",
      required=True)
  return parser.parse_args()


def train_model(
    preprocessed_dataset_path: str,
    trained_model_path: str,
    base_artifact_path: str):
  """Placeholder method to load a model from the torch hub and save it.

  Args:
    preprocessed_dataset_path (str): Path to the preprocessed dataset
    trained_model_path (str): Output path for the trained model
    base_artifact_path (str): path to the base directory of where artifacts can be stored for
      this component
  """
  # timestamp for the component execution
  timestamp = time.time()

  # create model or load a pretrained one
  model = torch.hub.load('pytorch/vision:v0.10.0', 'vgg16', pretrained=True)

  # to implement: train on preprocessed dataset
  # <insert training loop>

  # create directory to export the model to
  target_path = f"{base_artifact_path}/training/trained_model_{timestamp}.pt"
  target_path_gcsfuse = target_path.replace("gs://", "/gcs/")
  Path(target_path_gcsfuse).parent.mkdir(parents=True, exist_ok=True)

  # save and export the model
  torch.save(model.state_dict(), target_path_gcsfuse)

  # Write the model path to the component output file
  Path(trained_model_path).parent.mkdir(parents=True, exist_ok=True)
  with open(trained_model_path, 'w') as f:
    f.write(target_path)


if __name__ == "__main__":
  args = parse_args()
  train_model(**vars(args))
