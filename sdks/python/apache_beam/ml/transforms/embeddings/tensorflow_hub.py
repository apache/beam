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

from collections.abc import Iterable
from typing import Optional

import apache_beam as beam
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # required to register TF ops. # pylint: disable=unused-import
from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerTensor
from apache_beam.ml.inference.tensorflow_inference import default_tensor_inference_fn
from apache_beam.ml.transforms.base import EmbeddingsManager
from apache_beam.ml.transforms.base import _ImageEmbeddingHandler
from apache_beam.ml.transforms.base import _TextEmbeddingHandler

__all__ = ['TensorflowHubTextEmbeddings', 'TensorflowHubImageEmbeddings']


# TODO: https://github.com/apache/beam/issues/30288
# Replace with TFModelHandlerTensor when load_model() supports TFHUB models.
class _TensorflowHubModelHandler(TFModelHandlerTensor):
  """
  Note: Intended for internal use only. No backwards compatibility guarantees.
  """
  def __init__(self, preprocessing_url: Optional[str], *args, **kwargs):
    self.preprocessing_url = preprocessing_url
    super().__init__(*args, **kwargs)

  def load_model(self):
    # unable to load the models with tf.keras.models.load_model so
    # using hub.KerasLayer instead
    model = hub.KerasLayer(self._model_uri, **self._load_model_args)
    return model

  def _convert_prediction_result_to_list(
      self, predictions: Iterable[PredictionResult]):
    result = []
    for prediction in predictions:
      inference = prediction.inference.numpy().tolist()
      result.append(inference)
    return result

  def run_inference(self, batch, model, inference_args, model_id=None):
    if not inference_args:
      inference_args = {}
    if not self.preprocessing_url:
      predictions = default_tensor_inference_fn(
          model=model,
          batch=batch,
          inference_args=inference_args,
          model_id=model_id)
      return self._convert_prediction_result_to_list(predictions)

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
    predictions = utils._convert_to_result(batch, embeddings, model_id)
    return self._convert_prediction_result_to_list(predictions)


class TensorflowHubTextEmbeddings(EmbeddingsManager):
  def __init__(
      self,
      columns: list[str],
      hub_url: str,
      preprocessing_url: Optional[str] = None,
      **kwargs):
    """
    Embedding config for tensorflow hub models. This config can be used with
    MLTransform to embed text data. Models are loaded using the RunInference
    PTransform with the help of a ModelHandler.

    Args:
      columns: The columns containing the text to be embedded.
      hub_url: The url of the tensorflow hub model.
      preprocessing_url: The url of the preprocessing model. This is optional.
        If provided, the preprocessing model will be used to preprocess the
        text before feeding it to the main model.
      min_batch_size: The minimum batch size to be used for inference.
      max_batch_size: The maximum batch size to be used for inference.
      large_model: Whether to share the model across processes.
    """
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
    """
    Returns a RunInference object that is used to run inference on the text 
    input using _TextEmbeddingHandler.
    """
    return (
        RunInference(
            model_handler=_TextEmbeddingHandler(self),
            inference_args=self.inference_args,
        ))


class TensorflowHubImageEmbeddings(EmbeddingsManager):
  def __init__(self, columns: list[str], hub_url: str, **kwargs):
    """
    Embedding config for tensorflow hub models. This config can be used with
    MLTransform to embed image data. Models are loaded using the RunInference
    PTransform with the help of a ModelHandler.

    Args:
      columns: The columns containing the images to be embedded.
      hub_url: The url of the tensorflow hub model.
      min_batch_size: The minimum batch size to be used for inference.
      max_batch_size: The maximum batch size to be used for inference.
      large_model: Whether to share the model across processes.
    """
    super().__init__(columns=columns, **kwargs)
    self.model_uri = hub_url

  def get_model_handler(self) -> ModelHandler:
    # override the default inference function
    return _TensorflowHubModelHandler(
        model_uri=self.model_uri,
        preprocessing_url=None,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model,
    )

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    """
    Returns a RunInference object that is used to run inference on the text 
    input using _ImageEmbeddingHandler.
    """
    return (
        RunInference(
            model_handler=_ImageEmbeddingHandler(self),
            inference_args=self.inference_args,
        ))
