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

"""This file contains the transformations and utility functions for
the pipeline."""
import uuid

import numpy as np

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from datasets import load_dataset


def get_dataset(categories: list, split: str = "train"):
  """
    Takes a list of categories and a split (train/test/dev) and returns the
    corresponding subset of the dataset

    Args:
      categories (list): list of emotion categories to use
      split (str): The split of the dataset to use. Can be either "train", "dev", or "test".
      Defaults to train

    Returns:
      A list of text and a list of labels
    """
  labels = ["sadness", "joy", "love", "anger", "fear", "surprise"]
  label_map = {
      class_name: class_id
      for class_id, class_name in enumerate(labels)
  }
  labels_subset = np.array([label_map[class_name] for class_name in categories])
  emotion_dataset = load_dataset("emotion", download_mode="force_redownload")
  X, y = np.array(emotion_dataset[split]["text"]), np.array(
      emotion_dataset[split]["label"])
  subclass_idxs = [idx for idx, label in enumerate(y) if label in labels_subset]
  X_subset, y_subset = X[subclass_idxs], y[subclass_idxs]
  return X_subset.tolist(), y_subset.tolist()


class AssignUniqueID(beam.DoFn):
  """A DoFn for assigning Unique ID to each text."""
  def process(self, element, *args, **kwargs):
    uid = str(uuid.uuid4())
    yield {"id": uid, "text": element}


class ConvertToPubSubMessage(beam.DoFn):
  """A DoFn for converting into PubSub message format."""
  def process(self, element, *args, **kwargs):
    yield PubsubMessage(
        data=element["text"].encode("utf-8"), attributes={"id": element["id"]})
