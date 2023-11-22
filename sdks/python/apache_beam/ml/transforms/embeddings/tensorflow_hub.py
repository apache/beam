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

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorflow_inference import default_tensor_inference_fn
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerTensor
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _TextEmbeddingHandler

import tensorflow as tf
import tensorflow_text as text  # required to register TF ops. # pylint: disable=unused-import
import tensorflow_hub as hub

__all__ = ['TensorflowHubTextEmbeddings']


class _YieldElements(beam.PTransform):
  def expand(self, pcoll):
    def yield_elements(elements: List[Dict[str, Any]]):
      for element in elements:
        for key, value in element.items():
          if isinstance(value, PredictionResult):
            # contains transposed numpy array where first dimension
            # is the batch size. we need to send a list of
            # predictions instead of list[list]
            pred = value.inference.numpy().tolist()
            if len(pred) == 1 and len(pred[0]) != 1:
              element[key] = pred[0]
            else:
              element[key] = pred
        yield element

    return pcoll | beam.ParDo(yield_elements)


class _TensorflowHubModelHandler(TFModelHandlerTensor):
  """
  Note: Intended for internal use only. No backwards compatibility guarantees.
  """
  def __init__(self, preprocessing_url: str, *args, **kwargs):
    self.preprocessing_url = preprocessing_url
    super().__init__(*args, **kwargs)

  def load_model(self):
    # unable to load the models with tf.keras.models.load_model so
    # using hub.KerasLayer instead
    model = hub.KerasLayer(self._model_uri)
    return model

  def run_inference(self, batch, model, inference_args, model_id=None):
    if not inference_args:
      inference_args = {}
    if not self.preprocessing_url:
      return default_tensor_inference_fn(
          model=model,
          batch=batch,
          inference_args=inference_args,
          model_id=model_id)

    vectorized_batch = tf.stack(batch, axis=0)
    preprocessor_fn = hub.KerasLayer(self.preprocessing_url)
    vectorized_batch = preprocessor_fn(vectorized_batch)
    predictions = model(vectorized_batch)
    # https://www.tensorflow.org/text/tutorials/classify_text_with_bert#using_the_bert_model # pylint: disable=line-too-long
    # pooled_output -> represents the text as a whole. This is an embeddings
    # of the whole text. The shape is [batch_size, embedding_dimension]
    # sequence_output -> represents the text as a sequence of tokens. This is
    # an embeddings of each token in the text. The shape is
    # [batch_size, max_sequence_length, embedding_dimension]
    # pooled output is the embeedings as per the documentation. so let's use
    # that.
    embeddings = predictions['pooled_output']
    return utils._convert_to_result(batch, embeddings, model_id)


class TensorflowHubTextEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      columns: List[str],
      hub_url: str,
      preprocessing_url: Optional[str] = None,
      **kwargs):
    super().__init__(columns=columns, **kwargs)
    self.model_uri = hub_url
    self.preprocessing_url = preprocessing_url

  def get_model_handler(self) -> ModelHandler:
    # override the default inference function
    return _TensorflowHubModelHandler(
        model_uri=self.model_uri,
        preprocessing_url=self.preprocessing_url,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model,
    )

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return (
        RunInference(model_handler=_TextEmbeddingHandler(self))
        # This is required since RunInference performs batching and returns
        # batches. We need to decompose the batches and return the elements
        # in their initial shape to the downstream transforms.
        | _YieldElements())
