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
import signal
import typing

import apache_beam as beam
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from apache_beam.transforms import ptransform
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from transformers import BertConfig
from transformers import BertForMaskedLM
from transformers import BertTokenizer

# This script provides a run inference transform with pre and post processing.
# The model used is a BertLM, base uncased model.
_LOGGER = logging.getLogger(__name__)


class InferenceTransform(ptransform.PTransform):
  class PytorchModelHandlerKeyedTensorWrapper(PytorchModelHandlerKeyedTensor):
    """Wrapper to PytorchModelHandler to limit batch size to 1.
        The tokenized strings generated from BertTokenizer may have different
        lengths, which doesn't work with torch.stack() in current RunInference
        implementation since stack() requires tensors to be the same size.
        Restricting max_batch_size to 1 means there is only 1 example per
        `batch` in the run_inference() call.
        """
    def batch_elements_kwargs(self):
      return {'max_batch_size': 1}

  class Preprocess(beam.DoFn):
    def __init__(self, tokenizer):
      self._tokenizer = tokenizer
      logging.info('Starting Preprocess.')

    def process(self, text: str):
      import torch
      # remove unusable tokens marks.
      removable_tokens = ['"', '*', '<br />', "'", "(", ")"]
      for token in removable_tokens:
        text = text.replace(token, '')

      # only take first sentence.
      ending_chars = ['.', '!', '?']
      for char in ending_chars:
        if char in text:
          text = text.split(char)[0]

      # add dot to end of sentence.
      text = text + ' .'

      # mask the last word and drop very long sentences.
      if len(text.strip()) > 0 and len(text.strip()) < 512:
        logging.info('Preprocessing Line: %s', text)
        text_list = text.split()
        masked_text = ' '.join(text_list[:-2] + ['[MASK]', text_list[-1]])
        tokens = self._tokenizer(masked_text, return_tensors='pt')
        tokens = {key: torch.squeeze(val) for key, val in tokens.items()}

        # skip first row of csv file.
        if "review,sentiment" not in text.strip():
          return [(text, tokens)]

  class Postprocess(beam.DoFn):
    def __init__(self, bert_tokenizer):
      self.bert_tokenizer = bert_tokenizer
      logging.info('Starting Postprocess')

    def process(self, element: typing.Tuple[str, PredictionResult]) \
        -> typing.Iterable[str]:
      text, prediction_result = element
      inputs = prediction_result.example
      logits = prediction_result.inference['logits']
      mask_token_index = (
          inputs['input_ids'] == self.bert_tokenizer.mask_token_id).nonzero(
              as_tuple=True)[0]
      predicted_token_id = logits[mask_token_index].argmax(axis=-1)
      decoded_word = self.bert_tokenizer.decode(predicted_token_id)
      text = text.replace('.', '').strip()
      yield (
          f"{text} \n Predicted word: {decoded_word.upper()} -- "
          f"Actual word: {text.split()[-1].upper()}")

  def __init__(self, model, model_path):
    self._model = model
    logging.info(f"Downloading {self._model} model from GCS.")
    self._model_config = BertConfig.from_pretrained(self._model)
    self._tokenizer = BertTokenizer.from_pretrained(self._model)
    self._model_handler = self.PytorchModelHandlerKeyedTensorWrapper(
        state_dict_path=(model_path),
        model_class=BertForMaskedLM,
        model_params={'config': self._model_config},
        device='cuda:0')

  def expand(self, pcoll):
    return (
        pcoll
        | 'Preprocess' >> beam.ParDo(self.Preprocess(self._tokenizer))
        | 'Inference' >> RunInference(KeyedModelHandler(self._model_handler))
        | 'Postprocess' >> beam.ParDo(self.Postprocess(self._tokenizer)))

  @staticmethod
  def from_runner_api_parameter(unused_ptransform, payload, unused_context):
    return InferenceTransform(payload['model'], payload['model_path'])
