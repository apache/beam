#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

# Support forward references
from __future__ import annotations

import logging
import sys
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional
from typing import Union

from apache_beam.ml.inference import utils
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

# tensorflow, torch and transformers are intentionally not imported at module
# import time. They are only needed for loading models and running inference
# when the pipeline is executed (for example, on the workers), and should not
# be required for constructing and submitting pipelines.
# Following imports are kept for lint but not actually imported by Python
# during execution.
if TYPE_CHECKING:
  import tensorflow as tf
  import torch
  from transformers import AutoModel
  from transformers import Pipeline
  from transformers import TFAutoModel

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "HuggingFaceModelHandlerTensor",
    "HuggingFaceModelHandlerKeyedTensor",
    "HuggingFacePipelineModelHandler",
]

TensorInferenceFn = Callable[
    [
        # Type aliases are evaluated at import time, so framework types are
        # quoted (as forward references) to avoid importing them.
        Sequence[Union['torch.Tensor', 'tf.Tensor']],
        Union['AutoModel', 'TFAutoModel'],
        str,
        Optional[dict[str, Any]],
        Optional[str],
    ],
    Iterable[PredictionResult],
]

KeyedTensorInferenceFn = Callable[[
    Sequence[dict[str, Union['torch.Tensor', 'tf.Tensor']]],
    Union['AutoModel', 'TFAutoModel'],
    str,
    Optional[dict[str, Any]],
    Optional[str]
],
                                  Iterable[PredictionResult]]

PipelineInferenceFn = Callable[
    [Sequence[str], 'Pipeline', Optional[dict[str, Any]]],
    Iterable[PredictionResult]]


class PipelineTask(str, Enum):
  """
  PipelineTask defines all the tasks supported by the Hugging Face Pipelines
  listed at https://huggingface.co/docs/transformers/main_classes/pipelines.
  Only these tasks can be passed to HuggingFacePipelineModelHandler.
  """
  AudioClassification = 'audio-classification'
  AutomaticSpeechRecognition = 'automatic-speech-recognition'
  Conversational = 'conversational'
  DepthEstimation = 'depth-estimation'
  DocumentQuestionAnswering = 'document-question-answering'
  FeatureExtraction = 'feature-extraction'
  FillMask = 'fill-mask'
  ImageClassification = 'image-classification'
  ImageSegmentation = 'image-segmentation'
  ImageToText = 'image-to-text'
  MaskGeneration = 'mask-generation'
  NER = 'ner'
  ObjectDetection = 'object-detection'
  QuestionAnswering = 'question-answering'
  SentimentAnalysis = 'sentiment-analysis'
  Summarization = 'summarization'
  TableQuestionAnswering = 'table-question-answering'
  TextClassification = 'text-classification'
  TextGeneration = 'text-generation'
  Text2TextGeneration = 'text2text-generation'
  TextToAudio = 'text-to-audio'
  TokenClassification = 'token-classification'
  Translation = 'translation'
  VideoClassification = 'video-classification'
  VisualQuestionAnswering = 'visual-question-answering'
  VQA = 'vqa'
  ZeroShotAudioClassification = 'zero-shot-audio-classification'
  ZeroShotClassification = 'zero-shot-classification'
  ZeroShotImageClassification = 'zero-shot-image-classification'
  ZeroShotObjectDetection = 'zero-shot-object-detection'
  Translation_XX_to_YY = 'translation_XX_to_YY'


def _validate_constructor_args(model_uri, model_class):
  message = (
      "Please provide both model class and model uri to load the model."
      "Got params as model_uri={model_uri} and "
      "model_class={model_class}.")
  if not model_uri and not model_class:
    raise RuntimeError(
        message.format(model_uri=model_uri, model_class=model_class))
  elif not model_uri:
    raise RuntimeError(
        message.format(model_uri=model_uri, model_class=model_class))
  elif not model_class:
    raise RuntimeError(
        message.format(model_uri=model_uri, model_class=model_class))


def no_gpu_available_warning():
  _LOGGER.warning(
      "HuggingFaceModelHandler specified a 'GPU' device, "
      "but GPUs are not available. Switching to CPU.")


def is_gpu_available_torch():
  import torch
  if torch.cuda.is_available():
    return True
  else:
    no_gpu_available_warning()
    return False


def get_device_torch(device):
  import torch
  if device == "GPU" and is_gpu_available_torch():
    return torch.device("cuda")
  return torch.device("cpu")


def is_gpu_available_tensorflow(device):
  import tensorflow as tf
  gpu_devices = tf.config.list_physical_devices(device)
  if len(gpu_devices) == 0:
    no_gpu_available_warning()
    return False
  return True


def _is_tensorflow_tensor(value) -> bool:
  # A TensorFlow tensor can only exist if tensorflow was already imported.
  # Checking sys.modules, instead of importing tensorflow, avoids requiring
  # tensorflow to be installed (and the cost of importing it) for PyTorch.
  tensor_type = getattr(sys.modules.get('tensorflow'), 'Tensor', None)
  return tensor_type is not None and isinstance(value, tensor_type)


def _import_transformers_pipeline() -> Callable[..., Pipeline]:
  """Returns transformers.pipeline, importing it lazily."""
  try:
    from transformers import pipeline
  except ImportError as e:
    raise ImportError(
        'transformers is required to load Hugging Face pipelines, e.g. for '
        'HuggingFacePipelineModelHandler. Please install it using '
        '`pip install transformers` in the environment where the pipeline is '
        'executed (for example, on the workers).') from e
  return pipeline


def _validate_constructor_args_hf_pipeline(task, model):
  if not task and not model:
    raise RuntimeError(
        'Please provide either task or model to the '
        'HuggingFacePipelineModelHandler. If the model already defines the '
        'task, no need to specify the task.')


def _run_inference_torch_keyed_tensor(
    batch: Sequence[dict[str, torch.Tensor]],
    model: AutoModel,
    device,
    inference_args: dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  import torch

  from apache_beam.ml.inference.pytorch_inference import _convert_to_device
  device = get_device_torch(device)
  key_to_tensor_list = defaultdict(list)
  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    for example in batch:
      for key, tensor in example.items():
        key_to_tensor_list[key].append(tensor)
    key_to_batched_tensors = {}
    for key in key_to_tensor_list:
      batched_tensors = torch.stack(key_to_tensor_list[key])
      batched_tensors = _convert_to_device(batched_tensors, device)
      key_to_batched_tensors[key] = batched_tensors
    predictions = model(**key_to_batched_tensors, **inference_args)
    return utils._convert_to_result(batch, predictions, model_id)


def _run_inference_tensorflow_keyed_tensor(
    batch: Sequence[dict[str, tf.Tensor]],
    model: TFAutoModel,
    device,
    inference_args: dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  import tensorflow as tf
  if device == "GPU":
    is_gpu_available_tensorflow(device)
  key_to_tensor_list = defaultdict(list)
  for example in batch:
    for key, tensor in example.items():
      key_to_tensor_list[key].append(tensor)
  key_to_batched_tensors = {}
  for key in key_to_tensor_list:
    batched_tensors = tf.stack(key_to_tensor_list[key], axis=0)
    key_to_batched_tensors[key] = batched_tensors
  predictions = model(**key_to_batched_tensors, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


# Unlike annotations, generic base class arguments are evaluated when the class
# is defined, so framework types are quoted (as forward references) to avoid
# importing them. Type checkers resolve them via the TYPE_CHECKING imports.
class HuggingFaceModelHandlerKeyedTensor(
    ModelHandler[dict[str, Union['tf.Tensor', 'torch.Tensor']],
                 PredictionResult,
                 Union['AutoModel', 'TFAutoModel']]):
  def __init__(
      self,
      model_uri: str,
      model_class: Union[AutoModel, TFAutoModel],
      framework: str,
      device: str = "CPU",
      *,
      inference_fn: Optional[Callable[..., Iterable[PredictionResult]]] = None,
      load_model_args: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      **kwargs):
    """
    Implementation of the ModelHandler interface for HuggingFace with
    Keyed Tensors for PyTorch/Tensorflow backend.

    Example Usage model::
      pcoll | RunInference(HuggingFaceModelHandlerKeyedTensor(
        model_uri="bert-base-uncased", model_class=AutoModelForMaskedLM,
        framework='pt'))

    Args:
      model_uri (str): path to the pretrained model on the hugging face
        models hub.
      model_class: model class to load the repository from model_uri.
      framework (str): Framework to use for the model. 'tf' for TensorFlow and
        'pt' for PyTorch.
      device: For torch tensors, specify device on which you wish to
        run the model. Defaults to CPU.
      inference_fn: the inference function to use during RunInference.
        Default is _run_inference_torch_keyed_tensor or
        _run_inference_tensorflow_keyed_tensor depending on the input type.
      load_model_args (dict[str, Any]): (Optional) Keyword arguments to provide
        load options while loading models from Hugging Face Hub.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
        before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      max_batch_weight: the maximum total weight of a batch. Cannot be
        combined with batch_length_fn or batch_bucket_boundaries.
      element_size_fn: a function that returns the size (weight) of an element.
      batch_length_fn: a callable that returns the length of an element for
        length-aware batching. Cannot be used together with max_batch_weight.
      batch_bucket_boundaries: a sorted list of positive boundary values for
        length-aware batching buckets. Cannot be used together with
        max_batch_weight.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** HuggingFaceModelHandler supports
    transformers>=4.18.0.
    """
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        batch_length_fn=batch_length_fn,
        batch_bucket_boundaries=batch_bucket_boundaries,
        large_model=large_model,
        model_copies=model_copies,
        **kwargs)
    self._model_uri = model_uri
    self._model_class = model_class
    self._device = device
    self._inference_fn = inference_fn
    self._model_config_args = load_model_args if load_model_args else {}
    self._framework = framework

    _validate_constructor_args(
        model_uri=self._model_uri, model_class=self._model_class)

  def load_model(self):
    """Loads and initializes the model for processing."""
    model = self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)
    if self._framework == 'pt':
      if self._device == "GPU" and is_gpu_available_torch():
        import torch
        model.to(torch.device("cuda"))
      if callable(getattr(model, 'requires_grad_', None)):
        model.requires_grad_(False)
    return model

  def run_inference(
      self,
      batch: Sequence[dict[str, Union[tf.Tensor, torch.Tensor]]],
      model: Union[AutoModel, TFAutoModel],
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Keyed Tensors and returns an Iterable of
    Tensors Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Keyed Tensors. These Tensors should be batchable,
        as this method will call `tf.stack()`/`torch.stack()` and pass in
        batched Tensors with dimensions (batch_size, n_features, etc.) into
        the model's predict() function.
      model: A Tensorflow/PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the
        model's inference function. Unlike Tensors in `batch`,
        these parameters will not be dynamically batched.
    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args

    if self._inference_fn:
      return self._inference_fn(
          batch, model, self._device, inference_args, self._model_uri)

    if self._framework == "tf":
      return _run_inference_tensorflow_keyed_tensor(
          batch, model, self._device, inference_args, self._model_uri)
    else:
      return _run_inference_torch_keyed_tensor(
          batch, model, self._device, inference_args, self._model_uri)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri

  def get_num_bytes(
      self, batch: Sequence[Union[tf.Tensor, torch.Tensor]]) -> int:
    """
    Returns:
      The number of bytes of data for the Tensors batch.
    """
    if self._framework == "tf":
      return sum(sys.getsizeof(element) for element in batch)
    else:
      return sum(
          (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
        A namespace for metrics collected by the RunInference transform.
    """
    return "BeamML_HuggingFaceModelHandler_KeyedTensor"


def _default_inference_fn_torch(
    batch: Sequence[Union[tf.Tensor, torch.Tensor]],
    model: Union[AutoModel, TFAutoModel],
    device,
    inference_args: dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  import torch

  from apache_beam.ml.inference.pytorch_inference import _convert_to_device
  device = get_device_torch(device)
  # torch.no_grad() mitigates GPU memory issues
  # https://github.com/apache/beam/issues/22811
  with torch.no_grad():
    batched_tensors = torch.stack(batch)
    batched_tensors = _convert_to_device(batched_tensors, device)
    predictions = model(batched_tensors, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


def _default_inference_fn_tensorflow(
    batch: Sequence[Union[tf.Tensor, torch.Tensor]],
    model: Union[AutoModel, TFAutoModel],
    device,
    inference_args: dict[str, Any],
    model_id: Optional[str] = None) -> Iterable[PredictionResult]:
  import tensorflow as tf
  if device == "GPU":
    is_gpu_available_tensorflow(device)
  batched_tensors = tf.stack(batch, axis=0)
  predictions = model(batched_tensors, **inference_args)
  return utils._convert_to_result(batch, predictions, model_id)


class HuggingFaceModelHandlerTensor(ModelHandler[Union['tf.Tensor',
                                                       'torch.Tensor'],
                                                 PredictionResult,
                                                 Union['AutoModel',
                                                       'TFAutoModel']]):
  def __init__(
      self,
      model_uri: str,
      model_class: Union[AutoModel, TFAutoModel],
      device: str = "CPU",
      *,
      inference_fn: Optional[Callable[..., Iterable[PredictionResult]]] = None,
      load_model_args: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      **kwargs):
    """
    Implementation of the ModelHandler interface for HuggingFace with
    Tensors for PyTorch/Tensorflow backend.

    Depending on the type of tensors, the model framework is determined
    automatically.

    Example Usage model:
      pcoll | RunInference(HuggingFaceModelHandlerTensor(
        model_uri="bert-base-uncased", model_class=AutoModelForMaskedLM))

    Args:
      model_uri (str): path to the pretrained model on the hugging face
        models hub.
      model_class: model class to load the repository from model_uri.
      device: For torch tensors, specify device on which you wish to
        run the model. Defaults to CPU.
      inference_fn: the inference function to use during RunInference.
        Default is _run_inference_torch_keyed_tensor or
        _run_inference_tensorflow_keyed_tensor depending on the input type.
      load_model_args (dict[str, Any]): (Optional) keyword arguments to provide
        load options while loading models from Hugging Face Hub.
        Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
        before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      max_batch_weight: the maximum total weight of a batch. Cannot be
        combined with batch_length_fn or batch_bucket_boundaries.
      element_size_fn: a function that returns the size (weight) of an element.
      batch_length_fn: a callable that returns the length of an element for
        length-aware batching. Cannot be used together with max_batch_weight.
      batch_bucket_boundaries: a sorted list of positive boundary values for
        length-aware batching buckets. Cannot be used together with
        max_batch_weight.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** HuggingFaceModelHandler supports
    transformers>=4.18.0.
    """
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        batch_length_fn=batch_length_fn,
        batch_bucket_boundaries=batch_bucket_boundaries,
        large_model=large_model,
        model_copies=model_copies,
        **kwargs)
    self._model_uri = model_uri
    self._model_class = model_class
    self._device = device
    self._inference_fn = inference_fn
    self._model_config_args = load_model_args if load_model_args else {}
    self._framework = ""

    _validate_constructor_args(
        model_uri=self._model_uri, model_class=self._model_class)

  def load_model(self):
    """Loads and initializes the model for processing."""
    model = self._model_class.from_pretrained(
        self._model_uri, **self._model_config_args)
    if callable(getattr(model, 'requires_grad_', None)):
      model.requires_grad_(False)
    return model

  def run_inference(
      self,
      batch: Sequence[Union[tf.Tensor, torch.Tensor]],
      model: Union[AutoModel, TFAutoModel],
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensors Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as
        this method will call `tf.stack()`/`torch.stack()` and pass in
        batched Tensors with dimensions (batch_size, n_features, etc.)
        into the model's predict() function.
      model: A Tensorflow/PyTorch model.
      inference_args (dict[str, Any]): Non-batchable arguments required as
        inputs to the model's inference function. Unlike Tensors in `batch`,
        these parameters will not be dynamically batched.

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
    if not self._framework:
      if _is_tensorflow_tensor(batch[0]):
        self._framework = "tf"
      else:
        self._framework = "pt"

    if (self._framework == "pt" and self._device == "GPU" and
        is_gpu_available_torch()):
      import torch
      model.to(torch.device("cuda"))

    if self._inference_fn:
      return self._inference_fn(
          batch, model, inference_args, inference_args, self._model_uri)

    if self._framework == "tf":
      return _default_inference_fn_tensorflow(
          batch, model, self._device, inference_args, self._model_uri)
    else:
      return _default_inference_fn_torch(
          batch, model, self._device, inference_args, self._model_uri)

  def update_model_path(self, model_path: Optional[str] = None):
    self._model_uri = model_path if model_path else self._model_uri

  def get_num_bytes(
      self, batch: Sequence[Union[tf.Tensor, torch.Tensor]]) -> int:
    """
    Returns:
      The number of bytes of data for the Tensors batch.
    """
    if self._framework == "tf":
      return sum(sys.getsizeof(element) for element in batch)
    else:
      return sum(
          (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_HuggingFaceModelHandler_Tensor'


def _default_pipeline_inference_fn(
    batch, pipeline, inference_args) -> Iterable[PredictionResult]:
  predicitons = pipeline(batch, **inference_args)
  return predicitons


class HuggingFacePipelineModelHandler(ModelHandler[str,
                                                   PredictionResult,
                                                   'Pipeline']):
  def __init__(
      self,
      task: Union[str, PipelineTask] = "",
      model: str = "",
      *,
      device: Optional[str] = None,
      inference_fn: PipelineInferenceFn = _default_pipeline_inference_fn,
      load_pipeline_args: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      **kwargs):
    """
    Implementation of the ModelHandler interface for Hugging Face Pipelines.

    Example Usage model::
      pcoll | RunInference(HuggingFacePipelineModelHandler(
        task="fill-mask"))

    Args:
      task (str or enum.Enum): task supported by HuggingFace Pipelines.
        Accepts a string task or an enum.Enum from PipelineTask.
      model (str): path to the pretrained *model-id* on Hugging Face Models Hub
        to use custom model for the chosen task. If the `model` already defines
        the task then no need to specify the `task` parameter.
        Use the *model-id* string instead of an actual model here.
        Model-specific kwargs for `from_pretrained(..., **model_kwargs)` can be
        specified with `model_kwargs` using `load_pipeline_args`.

        Example Usage::
          model_handler = HuggingFacePipelineModelHandler(
            task="text-generation", model="meta-llama/Llama-2-7b-hf",
            load_pipeline_args={'model_kwargs':{'quantization_map':config}})

      device (str): the device (`"CPU"` or `"GPU"`) on which you wish to run
        the pipeline. Defaults to GPU. If GPU is not available then it falls
        back to CPU. Whether a GPU is available is determined when the model
        is loaded, i.e. on the workers that execute the pipeline. You can also
        use advanced option like `device_map` with key-value pair as you would
        do in the usual Hugging Face pipeline using `load_pipeline_args`.
        Ex: load_pipeline_args={'device_map':auto}).
      inference_fn: the inference function to use during RunInference.
        Default is _default_pipeline_inference_fn.
      load_pipeline_args (dict[str, Any]): keyword arguments to provide load
        options while loading pipelines from Hugging Face. Defaults to None.
      min_batch_size: the minimum batch size to use when batching inputs.
      max_batch_size: the maximum batch size to use when batching inputs.
      max_batch_duration_secs: the maximum amount of time to buffer a batch
        before emitting; used in streaming contexts.
      large_model: set to true if your model is large enough to run into
        memory pressure if you load multiple copies. Given a model that
        consumes N memory and a machine with W cores and M memory, you should
        set this to True if N*W > M.
      model_copies: The exact number of models that you would like loaded
        onto your machine. This can be useful if you exactly know your CPU or
        GPU capacity and want to maximize resource utilization.
      max_batch_weight: the maximum total weight of a batch. Cannot be
        combined with batch_length_fn or batch_bucket_boundaries.
      element_size_fn: a function that returns the size (weight) of an element.
      batch_length_fn: a callable that returns the length of an element for
        length-aware batching. Cannot be used together with max_batch_weight.
      batch_bucket_boundaries: a sorted list of positive boundary values for
        length-aware batching buckets. Cannot be used together with
        max_batch_weight.
      kwargs: 'env_vars' can be used to set environment variables
        before loading the model.

    **Supported Versions:** HuggingFacePipelineModelHandler supports
    transformers>=4.18.0. transformers and torch are only needed in the
    environment where the pipeline is executed (for example, on the workers),
    not for constructing and submitting the pipeline.
    """
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        batch_length_fn=batch_length_fn,
        batch_bucket_boundaries=batch_bucket_boundaries,
        large_model=large_model,
        model_copies=model_copies,
        **kwargs)
    self._task = task
    self._model = model
    self._inference_fn = inference_fn
    self._load_pipeline_args = load_pipeline_args if load_pipeline_args else {}
    self._framework = "pt"

    # Check if the device is specified twice. If true then the device parameter
    # of model handler is overridden.
    self._deduplicate_device_value(device)
    _validate_constructor_args_hf_pipeline(self._task, self._model)

  def _deduplicate_device_value(self, device: Optional[str]):
    current_device = device.upper() if device else None
    if (current_device and current_device != 'CPU' and current_device != 'GPU'):
      raise ValueError(
          f"Invalid device value: {device}. Please specify "
          "either CPU or GPU. Defaults to GPU if no value "
          "is provided.")
    if 'device' not in self._load_pipeline_args:
      if current_device == 'CPU':
        self._load_pipeline_args['device'] = 'cpu'
      # Otherwise, a GPU is used if one is available. This is determined in
      # load_model(), i.e. on the workers that execute the pipeline, instead of
      # on the machine that constructs and submits the pipeline.
    else:
      if current_device:
        raise ValueError(
            '`device` specified in `load_pipeline_args`. `device` '
            'parameter for HuggingFacePipelineModelHandler will be ignored.')

  def load_model(self):
    """Loads and initializes the pipeline for processing."""
    pipeline = _import_transformers_pipeline()
    load_pipeline_args = dict(self._load_pipeline_args)
    if 'device' not in load_pipeline_args:
      load_pipeline_args['device'] = (
          'cuda:0' if is_gpu_available_torch() else 'cpu')
    return pipeline(task=self._task, model=self._model, **load_pipeline_args)

  def run_inference(
      self,
      batch: Sequence[str],
      pipeline: Pipeline,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of examples passed as a string resource.
    These can either be string sentences, or string path to images or
    audio files.

    Args:
      batch: A sequence of strings resources.
      pipeline: A Hugging Face Pipeline.
      inference_args: Non-batchable arguments required as inputs to the model's
        inference function.
    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args
    predictions = self._inference_fn(batch, pipeline, inference_args)
    return utils._convert_to_result(batch, predictions)

  def update_model_path(self, model_path: Optional[str] = None):
    """
    Updates the pretrained model used by the Hugging Face Pipeline task.
    Make sure that the new model does the same task as initial model.

    Args:
      model_path (str): (Optional) Path to the new trained model
        from Hugging Face. Defaults to None.
    """
    self._model = model_path if model_path else self._model

  def get_num_bytes(self, batch: Sequence[str]) -> int:
    """
    Returns:
      The number of bytes of input batch elements.
    """
    return sum(sys.getsizeof(element) for element in batch)

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'BeamML_HuggingFacePipelineModelHandler'
