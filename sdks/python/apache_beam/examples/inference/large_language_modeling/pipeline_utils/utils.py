#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from transformers import T5ForConditionalGeneration
from apache_beam.io.filesystems import FileSystems
import torch
from transformers import AutoTokenizer
import apache_beam as beam


# ModelHandlerwrapper can be removed once:
# https://github.com/apache/beam/issues/22572 is fixed
# [START PytorchModelHandlerTensorWrapper]
class ModelHandlerWrapper(PytorchModelHandlerTensor):
  """Wrapper to PyTorchModelHandlerTensor to load the
    weights in model.model instead of model."""
  def load_model(self):
    model = self._model_class(**self._model_params)
    model.to(self._device)
    file = FileSystems.open(self._state_dict_path, 'rb')
    model.model.load_state_dict(torch.load(file))
    model.eval()
    return model


# [END PytorchModelHandlerTensorWrapper]


# Modelwrapper can be removed once:
# https://github.com/apache/beam/issues/22572 is fixed
# [START T5ForConditionalGenerationModelWrapper]
class ModelWrapper(torch.nn.Module):
  """Wrapper to T5forCOnditionalGeneration to use generate() for
    getting output when calling forward function."""
  def __init__(self, **kwargs):
    super().__init__()
    self.model = T5ForConditionalGeneration(**kwargs)

  def forward(self, input_ids):
    output = self.model.generate(input_ids)
    return output


# [END T5ForConditionalGenerationModelWrapper]


class Preprocess(beam.DoFn):
  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    """
        Process the raw text input to a format suitable for
        T5ForConditionalGeneration Model Inference

        Args:
          element: A Pcollection

        Returns:
          The input_ids are being returned.
        """
    input_ids = self._tokenizer(
        element, return_tensors="pt", padding="max_length",
        max_length=512).input_ids
    return input_ids


class Postprocess(beam.DoFn):
  def __init__(self, tokenizer: AutoTokenizer):
    self._tokenizer = tokenizer

  def process(self, element):
    """
        Process the PredictionResult to print the translated texts

        Args:
          element: The RunInference output to be processed.
        """
    decoded_inputs = self._tokenizer.decode(
        element.example, skip_special_tokens=True)
    decoded_outputs = self._tokenizer.decode(
        element.inference, skip_special_tokens=True)
    print(f"{decoded_inputs} \t Output: {decoded_outputs}")