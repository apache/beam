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
import apache_beam as beam
import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from transformers import DistilBertForSequenceClassification
from transformers import DistilBertTokenizer


class CustomPytorchModelHandlerKeyedTensor(PytorchModelHandlerKeyedTensor):
  """Wrapper around PytorchModelHandlerKeyedTensor to load a model on CPU."""
  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    model = self._model_class(**self._model_params)
    model.to(self._device)
    file = FileSystems.open(self._state_dict_path, "rb")
    model.load_state_dict(torch.load(file, map_location=self._device))
    model.eval()
    return model


# Can be removed once https://github.com/apache/beam/issues/21863 is fixed
class HuggingFaceStripBatchingWrapper(DistilBertForSequenceClassification):
  """Wrapper around HuggingFace model because RunInference requires a batch
    as a list of dicts instead of a dict of lists. Another workaround
    can be found here where they disable batching instead.
    https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py"""
  def forward(self, **kwargs):
    output = super().forward(**kwargs)
    return [dict(zip(output, v)) for v in zip(*output.values())]


class Tokenize(beam.DoFn):
  """A DoFn for tokenizing texts"""
  def __init__(self, model_name: str):
    """Initialises a tokenizer based on the model_name"""
    self._model_name = model_name

  def setup(self):
    """Loads the tokenizer"""
    self._tokenizer = DistilBertTokenizer.from_pretrained(self._model_name)

  def process(self, text_input: str):
    """Prepocesses the text using the tokenizer"""
    # We need to pad the tokens tensors to max length to make sure
    # that all the tensors are of the same length and hence
    # stack-able by the RunInference API, normally you would batch first
    # and tokenize the batch after and pad each tensor
    # the the max length in the batch.
    tokens = self._tokenizer(
        text_input, return_tensors="pt", padding="max_length", max_length=512)
    # squeeze because tokenization add an extra dimension, which is empty
    # in this case because we're tokenizing one element at a time.
    tokens = {key: torch.squeeze(val) for key, val in tokens.items()}
    return [(text_input, tokens)]


class PostProcessor(beam.DoFn):
  """Postprocess the RunInference output"""
  def process(self, element):
    """
        Takes the input text and the prediction result, and returns a dictionary
        with the input text and the softmax probabilities

        Args:
          element: The tuple of input text and the prediction result

        Returns:
          A list of dictionaries, each containing the input text
          and the softmax output.
        """
    text_input, prediction_result = element
    softmax = (
        torch.nn.Softmax(dim=-1)(
            prediction_result.inference["logits"]).detach().numpy())
    return [{"input": text_input, "softmax": softmax}]
