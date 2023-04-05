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
the anomaly_detection pipeline."""
import json

import numpy as np

import apache_beam as beam
import config as cfg
import hdbscan
import torch
import yagmail
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.ml.inference.sklearn_inference import _validate_inference_args
from transformers import AutoTokenizer
from transformers import DistilBertModel

# [START tokenization]
Tokenizer = AutoTokenizer.from_pretrained(cfg.TOKENIZER_NAME)


def tokenize_sentence(input_dict):
  """
    Takes a dictionary with a text and an id, tokenizes the text, and
    returns a tuple of the text and id and the tokenized text

    Args:
      input_dict: a dictionary with the text and id of the sentence

    Returns:
      A tuple of the text and id, and a dictionary of the tokens.
    """
  text, uid = input_dict["text"], input_dict["id"]
  tokens = Tokenizer([text], padding=True, truncation=True, return_tensors="pt")
  tokens = {key: torch.squeeze(val) for key, val in tokens.items()}
  return (text, uid), tokens


# [END tokenization]


# [START DistilBertModelWrapper]
class ModelWrapper(DistilBertModel):
  """Wrapper to DistilBertModel to get embeddings when calling
    forward function."""
  def forward(self, **kwargs):
    output = super().forward(**kwargs)
    sentence_embedding = (
        self.mean_pooling(output,
                          kwargs["attention_mask"]).detach().cpu().numpy())
    return sentence_embedding

  # Mean Pooling - Take attention mask into account for correct averaging
  def mean_pooling(self, model_output, attention_mask):
    """
        Calculates the mean of token embeddings

        Args:
          model_output: The output of the model.
          attention_mask: This is a tensor that contains 1s for all input tokens and
          0s for all padding tokens.

        Returns:
          The mean of the token embeddings.
        """
    token_embeddings = model_output[
        0]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float())
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9)


# [END DistilBertModelWrapper]


# [START CustomSklearnModelHandlerNumpy]
class CustomSklearnModelHandlerNumpy(SklearnModelHandlerNumpy):
  # limit batch size to 1 can be removed once: https://github.com/apache/beam/issues/21863 is fixed
  def batch_elements_kwargs(self):
    """Limit batch size to 1 for inference"""
    return {"max_batch_size": 1}

  # run_inference can be removed once: https://github.com/apache/beam/issues/22572 is fixed
  def run_inference(self, batch, model, inference_args=None):
    """Runs inferences on a batch of numpy arrays.

        Args:
          batch: A sequence of examples as numpy arrays. They should
            be single examples.
          model: A numpy model or pipeline. Must implement predict(X).
            Where the parameter X is a numpy array.
          inference_args: Any additional arguments for an inference.

        Returns:
          An Iterable of type PredictionResult.
        """
    _validate_inference_args(inference_args)
    vectorized_batch = np.vstack(batch)
    predictions = hdbscan.approximate_predict(model, vectorized_batch)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]


# [END CustomSklearnModelHandlerNumpy]


class NormalizeEmbedding(beam.DoFn):
  """A DoFn for normalization of text embedding."""
  def process(self, element, *args, **kwargs):
    """
        For each element in the input PCollection, normalize the embedding vector, and
        yield a new element with the normalized embedding added

        Args:
          element: The element to be processed.
        """
    (text, uid), prediction = element
    embedding = prediction.inference
    l2_norm = np.linalg.norm(embedding)
    yield {"text": text, "id": uid, "embedding": embedding / l2_norm}


class DecodePubSubMessage(beam.DoFn):
  """A DoFn for decoding PubSub message into a dictionary."""
  def process(self, element, *args, **kwargs):
    """
        For each element in the input PCollection, retrieve the id and decode the bytes into string

        Args:
          element: The element that is being processed.
        """
    yield {
        "text": element.data.decode("utf-8"),
        "id": element.attributes["id"],
    }


class DecodePrediction(beam.DoFn):
  """A DoFn for decoding the prediction from RunInference."""
  def process(self, element):
    """
    The `process` function takes the output of RunInference and returns a dictionary
    with the text, uid, and cluster.**

    Args:
      element: The input element to be processed.
    """
    (text, uid), prediction = element
    cluster = prediction.inference.item()
    bq_dict = {"text": text, "id": uid, "cluster": cluster}
    yield bq_dict


class TriggerEmailAlert(beam.DoFn):
  """A DoFn for sending email using yagmail."""
  def setup(self):
    """
    Opens the cred.json file and initializes the yag SMTP client.
    """
    with open("./cred.json") as json_file:
      cred = json.load(json_file)
      self.yag_smtp_client = yagmail.SMTP(**cred)

  def process(self, element):
    """
        Takes a tuple of (text, id) and a prediction, and if the prediction is -1,
        it sends an email to the specified address

        Args:
          element: The element that is being processed.
        """
    (text, uid), prediction = element
    cluster = prediction.inference.item()
    if cluster == -1:
      body = f"Tweet-Id is {uid} and text is {text}"
      self.yag_smtp_client.send(
          to=cfg.EMAIL_ADDRESS, subject="Anomaly Detected", contents=body)
