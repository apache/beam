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

"""This module defines yaml wrappings for some ML transforms."""
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from importlib import import_module
import inspect
import logging
import pkgutil
import sys
import traceback
from typing import Any
from typing import Optional
from typing import Union
import weakref

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.typehints import typehints
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import python_callable
from apache_beam.yaml import options
from apache_beam.yaml.yaml_errors import maybe_with_exception_handling_transform_fn
from apache_beam.yaml.yaml_utils import SafeLineLoader


def _list_submodules(package):
  """
  Lists all submodules within a given package.
  """
  submodules = []
  skip_modules = ['base', 'handlers', 'test', 'utils']
  for _, module_name, _ in pkgutil.walk_packages(
      package.__path__, package.__name__ + '.'):
    if any(skip_name in module_name for skip_name in skip_modules):
      continue
    submodules.append(module_name)
  return submodules


_transform_constructors = {}
try:
  from apache_beam.ml.transforms.base import MLTransform

  # Load all available ML Transform modules
  for module_name in _list_submodules(beam.ml.transforms):
    try:
      module = import_module(module_name)
      _transform_constructors |= module.__dict__
    except ImportError as e:
      logging.warning(
          'Could not load ML transform module %s: %s.  Please '
          'install the necessary module dependencies',
          module_name,
          e)
except ImportError:
  MLTransform = None  # type: ignore

_OK = 'ok'
_ERR = 'err'

_TYPE_MAP: dict[str, Any] = {
    'str': str,
    'string': str,
    'int': int,
    'integer': int,
    'float': float,
    'double': float,
    'number': float,
    'bool': bool,
    'boolean': bool,
    'bytes': bytes,
    'any': Any,
}


def _parse_type_spec(spec: Any) -> Any:
  if spec is None:
    return Any
  spec = SafeLineLoader.strip_metadata(spec)
  if isinstance(spec, type) or spec is Any:
    return spec
  if isinstance(spec, str):
    return _TYPE_MAP.get(spec.lower(), Any)
  if isinstance(spec, Mapping):
    fields = [(str(k), _parse_type_spec(v)) for k, v in spec.items()]
    return RowTypeConstraint.from_fields(fields)
  return Any


def _to_python_scalar(val: Any) -> Any:
  if hasattr(val, 'item') and callable(val.item):
    try:
      return val.item()
    except Exception:
      pass
  if hasattr(val, 'tolist') and callable(val.tolist):
    try:
      return val.tolist()
    except Exception:
      pass
  return val


class _WeakRefModelBox:
  """Weak-referenceable wrapper for models that do not natively support weakref."""
  def __init__(self, model: Any):
    self.model = model


class _FunctionModelHandler(ModelHandler[Any, Any, Any]):
  """ModelHandler backed by user-supplied load_model_fn and inference_fn."""
  def __init__(
      self,
      inference_fn: Callable[..., Any],
      load_model_fn: Optional[Callable[[], Any]] = None,
      **kwargs):
    super().__init__(**kwargs)
    self._inference_fn = inference_fn
    self._load_model_fn = load_model_fn
    self._arity: Optional[int] = None

  def _get_arity(self) -> int:
    if self._arity is not None:
      return self._arity
    try:
      sig = inspect.signature(self._inference_fn)
      params = list(sig.parameters.values())
      if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
        self._arity = 3
        return 3
      positional = [
          p for p in params if p.kind in (
              inspect.Parameter.POSITIONAL_ONLY,
              inspect.Parameter.POSITIONAL_OR_KEYWORD)
      ]
      self._arity = len(positional)
      return self._arity
    except Exception:
      self._arity = 3
      return 3

  def load_model(self) -> Any:
    if self._load_model_fn is not None:
      return self._load_model_fn()
    return None

  def run_inference(
      self,
      batch: Sequence[Any],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None) -> Iterable[Any]:
    arity = self._get_arity()
    if arity >= 3:
      return self._inference_fn(batch, model, inference_args)
    if arity == 2:
      return self._inference_fn(batch, model)
    return [self._inference_fn(item) for item in batch]


class _LazyYamlModelHandler(ModelHandler[Any, Any, Any]):
  """Defers heavy ModelHandler instantiation and imports to worker setup()."""
  def __init__(
      self,
      handler_factory: Callable[[], ModelHandler[Any, Any, Any]],
      *,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      model_copies: Optional[int] = None,
      catch_errors: bool = False,
      **kwargs):
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        large_model=large_model,
        model_copies=model_copies,
        **kwargs)
    self._handler_factory = handler_factory
    self._underlying: Optional[ModelHandler[Any, Any, Any]] = None
    self._catch_errors = catch_errors

  def with_error_catching(self, catch_errors: bool = True):
    self._catch_errors = catch_errors
    return self

  def _get_underlying(self) -> ModelHandler[Any, Any, Any]:
    if self._underlying is None:
      self._underlying = self._handler_factory()
    return self._underlying

  def set_environment_vars(self):
    super().set_environment_vars()
    self._get_underlying().set_environment_vars()

  def get_metrics_namespace(self) -> str:
    if self._underlying is not None:
      return self._underlying.get_metrics_namespace()
    return 'RunInference'

  def get_num_bytes(self, batch: Sequence[Any]) -> int:
    return self._get_underlying().get_num_bytes(batch)

  def update_model_path(self, model_path: Optional[str] = None):
    self._get_underlying().update_model_path(model_path=model_path)

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    if self._underlying is not None:
      self._underlying.validate_inference_args(inference_args)

  def load_model(self) -> Any:
    model = self._get_underlying().load_model()
    if model is None:
      return _WeakRefModelBox(None)
    try:
      weakref.ref(model)
      return model
    except TypeError:
      return _WeakRefModelBox(model)

  def run_inference(
      self,
      batch: Sequence[Any],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None) -> Iterable[Any]:
    raw_model = model.model if isinstance(model, _WeakRefModelBox) else model
    underlying = self._get_underlying()
    if not self._catch_errors:
      return list(underlying.run_inference(batch, raw_model, inference_args))

    # Fast path: attempt full batch inference first.
    try:
      preds = list(underlying.run_inference(batch, raw_model, inference_args))
      return [(_OK, p) for p in preds]
    except Exception as batch_exn:
      if len(batch) <= 1:
        return [(
            _ERR,
            (
                type(batch_exn),
                repr(batch_exn),
                traceback.format_exception(*sys.exc_info())))]

    # Poison-pill isolation: retry per element against the already-loaded model
    # without evicting or reloading it so N-1 valid elements succeed.
    results: list[tuple[str, Any]] = []
    for item in batch:
      try:
        single_preds = list(
            underlying.run_inference([item], raw_model, inference_args))
        results.append((_OK, single_preds[0] if single_preds else None))
      except Exception as item_exn:
        results.append((
            _ERR,
            (
                type(item_exn),
                repr(item_exn),
                traceback.format_exception(*sys.exc_info()))))
    return results

  def __getstate__(self):
    state = dict(self.__dict__)
    state['_underlying'] = None
    return state


def _make_input_field_preprocess_fn(
    input_field: Union[str, list[str]],
    mode: str = 'raw') -> Callable[[Any], Any]:
  if isinstance(input_field, str):
    field_name = input_field

    def _extract_single(row):
      row_dict = row._asdict() if hasattr(row, '_asdict') else dict(row)
      if field_name not in row_dict:
        raise KeyError(f"input_field '{field_name}' not found in row: {row}")
      val = row_dict[field_name]
      if val is None:
        raise ValueError(f"input_field '{field_name}' is None in row: {row}")
      if mode == 'numpy':
        import numpy as np
        return np.asarray(val)
      if mode == 'pandas':
        import pandas as pd
        return pd.DataFrame([{field_name: val}])
      if mode == 'torch':
        import torch
        return torch.as_tensor(val)
      if mode == 'torch_keyed':
        import torch
        return {field_name: torch.as_tensor(val)}
      return val

    return _extract_single

  field_names = list(input_field)

  def _extract_multi(row):
    row_dict = row._asdict() if hasattr(row, '_asdict') else dict(row)
    vals = []
    for f in field_names:
      if f not in row_dict:
        raise KeyError(f"input_field '{f}' not found in row: {row}")
      if row_dict[f] is None:
        raise ValueError(f"input_field '{f}' is None in row: {row}")
      vals.append(row_dict[f])
    if mode == 'numpy':
      import numpy as np
      return np.asarray(vals)
    if mode == 'pandas':
      import pandas as pd
      return pd.DataFrame([{f: v for f, v in zip(field_names, vals)}])
    if mode == 'torch':
      import torch
      return torch.as_tensor(vals)
    if mode == 'torch_keyed':
      import torch
      return {f: torch.as_tensor(v) for f, v in zip(field_names, vals)}
    return {f: v for f, v in zip(field_names, vals)}

  return _extract_multi


class ModelHandlerProvider:
  handler_types: dict[str, Callable[..., "ModelHandlerProvider"]] = {}

  def __init__(
      self,
      handler,
      preprocess: Optional[Union[dict[str, str], Callable[[Any], Any]]] = None,
      postprocess: Optional[Union[dict[str, str], Callable[[Any], Any]]] = None,
      input_field: Optional[Union[str, list[str]]] = None,
      input_mode: str = 'raw'):
    self._handler = handler
    if callable(preprocess):
      self._preprocess_fn = preprocess
    elif preprocess is not None:
      self._preprocess_fn = self.parse_processing_transform(
          preprocess, 'preprocess')
    elif input_field is not None:
      self._preprocess_fn = _make_input_field_preprocess_fn(
          input_field, mode=input_mode)
    else:
      self._preprocess_fn = self.default_preprocess_fn()

    if callable(postprocess):
      self._postprocess_fn = postprocess
    elif postprocess is not None:
      self._postprocess_fn = self.parse_processing_transform(
          postprocess, 'postprocess')
    else:
      self._postprocess_fn = self.default_postprocess_fn()
    self._has_custom_postprocess = postprocess is not None

  def inference_output_type(self):
    return Any

  @staticmethod
  def parse_processing_transform(processing_transform, typ):
    def _parse_config(callable=None, path=None, name=None):
      if callable and (path or name):
        raise ValueError(
            f"Cannot specify 'callable' with 'path' and 'name' for {typ} "
            f"function.")
      if path and name:
        return python_callable.PythonCallableWithSource.load_from_script(
            FileSystems.open(path).read().decode(), name)
      elif callable:
        return python_callable.PythonCallableWithSource(callable)
      else:
        raise ValueError(
            f"Must specify one of 'callable' or 'path' and 'name' for {typ} "
            f"function.")

    if processing_transform:
      if isinstance(processing_transform, dict):
        return _parse_config(**processing_transform)
      else:
        raise ValueError("Invalid model_handler specification.")

  def underlying_handler(self):
    return self._handler

  @staticmethod
  def default_preprocess_fn():
    raise ValueError(
        'Model Handler does not implement a default preprocess '
        'method. Please define a preprocessing method using the '
        '\'preprocess\' tag. This is required in most cases because '
        'most models will have a different input shape, so the model '
        'cannot generalize how the input Row should be transformed. For '
        'an example preprocess method, see VertexAIModelHandlerJSONProvider')

  def _preprocess_fn_internal(self):
    return lambda row: (row, self._preprocess_fn(row))

  @staticmethod
  def default_postprocess_fn():
    return lambda x: x

  def _postprocess_fn_internal(self):
    return lambda result: (result[0], self._postprocess_fn(result[1]))

  @staticmethod
  def validate(model_handler_spec):
    raise NotImplementedError(type(ModelHandlerProvider))

  @classmethod
  def register_handler_type(cls, type_name):
    def apply(constructor):
      cls.handler_types[type_name] = constructor
      return constructor

    return apply

  @classmethod
  def create_handler(cls, model_handler_spec) -> "ModelHandlerProvider":
    typ = model_handler_spec['type']
    config = model_handler_spec['config']
    try:
      result = cls.handler_types[typ](**config)
      if not hasattr(result, 'to_json'):
        result.to_json = lambda: model_handler_spec
      return result
    except Exception as exn:
      raise ValueError(
          f'Unable to instantiate model handler of type {typ}. {exn}')


@ModelHandlerProvider.register_handler_type('VertexAI')
@ModelHandlerProvider.register_handler_type('VertexAIModelHandlerJSON')
class VertexAIModelHandlerJSONProvider(ModelHandlerProvider):
  def __init__(
      self,
      endpoint_id: str,
      project: str,
      location: str,
      preprocess: Optional[dict[str, str]] = None,
      postprocess: Optional[dict[str, str]] = None,
      experiment: Optional[str] = None,
      network: Optional[str] = None,
      private: bool = False,
      invoke_route: Optional[str] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      input_field: Optional[Union[str, list[str]]] = None,
      **kwargs):
    """
    ModelHandler for Vertex AI.

    This Model Handler can be used with RunInference to load a model hosted
    on VertexAI. Every model that is hosted on VertexAI should have three
    distinct, required, parameters - `endpoint_id`, `project` and `location`.
    These parameters tell the Model Handler how to access the model's endpoint
    so that input data can be sent using an API request, and inferences can be
    received as a response.

    This Model Handler also requires a `preprocess` function to be defined.
    Preprocessing and Postprocessing are described in more detail in the
    RunInference docs:
    https://beam.apache.org/releases/yamldoc/current/#runinference

    Every model will have a unique input, but all requests should be
    JSON-formatted. For example, most language models such as Llama and Gemma
    expect a JSON with the key "prompt" (among other optional keys). In Python,
    JSON can be expressed as a dictionary.

    For example: ::

        - type: RunInference
          config:
            inference_tag: 'my_inference'
            model_handler:
              type: VertexAIModelHandlerJSON
              config:
                endpoint_id: 9876543210
                project: my-project
                location: us-east1
                preprocess:
                  callable: 'lambda x: {"prompt": x.prompt, "max_tokens": 50}'

    In the above example, which mimics a call to a Llama 3 model hosted on
    VertexAI, the preprocess function (in this case a lambda) takes in a Beam
    Row with a single field, "prompt", and maps it to a dict with the same
    field. It also specifies an optional parameter, "max_tokens", that tells the
    model the allowed token size (in this case input + output token size).

    Args:
      endpoint_id: the numerical ID of the Vertex AI endpoint to query.
      project: the GCP project name where the endpoint is deployed.
      location: the GCP location where the endpoint is deployed.
      preprocess: A python callable, defined either inline, or using a file,
        that is invoked on the input row before sending to the model to be
        loaded by this ModelHandler. This parameter is required by the
        `VertexAIModelHandlerJSON` ModelHandler.
      postprocess: A python callable, defined either inline, or using a file,
        that is invoked on the PredictionResult output by the ModelHandler
        before parsing into the output Beam Row under the field name defined
        by the inference_tag.
      experiment: Experiment label to apply to the
        queries. See
        https://cloud.google.com/vertex-ai/docs/experiments/intro-vertex-ai-experiments
        for more information.
      network: The full name of the Compute Engine
        network the endpoint is deployed on; used for private
        endpoints. The network or subnetwork Dataflow pipeline
        option must be set and match this network for pipeline
        execution.
        Ex: "projects/12345/global/networks/myVPC"
      private: If the deployed Vertex AI endpoint is
        private, set to true. Requires a network to be provided
        as well.
      invoke_route: The custom route path to use when invoking
        endpoints with arbitrary prediction routes. When specified, uses
        `Endpoint.invoke()` instead of `Endpoint.predict()`. The route
        should start with a forward slash, e.g., "/predict/v1".
        See
        https://cloud.google.com/vertex-ai/docs/predictions/use-arbitrary-custom-routes
        for more information.
      min_batch_size: The minimum batch size to use when batching
        inputs.
      max_batch_size: The maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: The maximum amount of time to buffer
        a batch before emitting; used in streaming contexts.
    """

    if private and network is None:
      raise ValueError(
          "A VPC network must be provided to use a private endpoint.")

    clean_kwargs = {k: v for k, v in kwargs.items() if not k.startswith('_')}

    def _build_vertex_handler():
      try:
        from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
      except ImportError:
        raise ValueError(
            'Unable to import VertexAIModelHandlerJSON. Please '
            'install gcp dependencies: `pip install apache_beam[gcp]`')

      return VertexAIModelHandlerJSON(
          endpoint_id=str(endpoint_id),
          project=project,
          location=location,
          experiment=experiment,
          network=network,
          private=private,
          invoke_route=invoke_route,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
          max_batch_duration_secs=max_batch_duration_secs,
          **clean_kwargs)

    _handler = _LazyYamlModelHandler(
        _build_vertex_handler,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        large_model=large_model)

    super().__init__(_handler, preprocess, postprocess, input_field=input_field)

  @staticmethod
  def validate(model_handler_spec):
    if not model_handler_spec:
      raise ValueError("VertexAI model handler requires configuration.")
    missing = [
        k for k in ('endpoint_id', 'project', 'location')
        if not model_handler_spec.get(k)
    ]
    if missing:
      raise ValueError(
          f"VertexAI model handler missing required parameters: {missing}")

  def inference_output_type(self):
    return RowTypeConstraint.from_fields([('example', Any), ('inference', Any),
                                          ('model_id', Optional[str])])


@ModelHandlerProvider.register_handler_type('HuggingFacePipeline')
@ModelHandlerProvider.register_handler_type('HuggingFacePipelineModelHandler')
class HuggingFacePipelineModelHandlerProvider(ModelHandlerProvider):
  def __init__(
      self,
      task: Optional[str] = None,
      model: Optional[str] = None,
      preprocess: Optional[dict[str, str]] = None,
      postprocess: Optional[dict[str, str]] = None,
      device: Optional[Any] = None,
      inference_fn: Optional[dict[str, str]] = None,
      load_pipeline_args: Optional[dict[str, Any]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      input_field: Optional[Union[str, list[str]]] = None,
      **kwargs):
    """ModelHandler for Hugging Face Pipelines."""
    if not task and not model:
      raise ValueError(
          "HuggingFacePipelineModelHandler requires either 'task' or "
          "'model' to be specified.")
    self._task = task
    self._model = model
    kwargs = {k: v for k, v in kwargs.items() if not k.startswith('_')}

    inference_fn_obj = self.parse_processing_transform(
        inference_fn, 'inference_fn') if inference_fn else None

    def _build_hf_handler():
      try:
        from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
      except ImportError:
        raise ValueError(
            'Unable to import HuggingFacePipelineModelHandler. Please '
            'install transformers dependencies.')

      handler_kwargs: dict[str, Any] = {}
      if inference_fn_obj:
        handler_kwargs['inference_fn'] = inference_fn_obj

      return HuggingFacePipelineModelHandler(
          task=task,
          model=model,
          device=device,
          load_pipeline_args=load_pipeline_args,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
          max_batch_duration_secs=max_batch_duration_secs,
          large_model=large_model,
          **handler_kwargs,
          **kwargs)

    _handler = _LazyYamlModelHandler(
        _build_hf_handler,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        large_model=large_model)

    super().__init__(_handler, preprocess, postprocess, input_field=input_field)

  @staticmethod
  def validate(config):
    if not config or (not config.get('task') and not config.get('model')):
      raise ValueError(
          "HuggingFacePipelineModelHandler requires either 'task' or "
          "'model' to be specified.")

  def inference_output_type(self):
    return Any


@ModelHandlerProvider.register_handler_type('PyTorch')
@ModelHandlerProvider.register_handler_type('PytorchModelHandlerTensor')
class PytorchModelHandlerProvider(ModelHandlerProvider):
  def __init__(
      self,
      state_dict_path: Optional[str] = None,
      model_class: Optional[Union[dict[str, str], Callable[..., Any]]] = None,
      model_params: Optional[dict[str, Any]] = None,
      torch_script_model_path: Optional[str] = None,
      device: str = 'CPU',
      preprocess: Optional[dict[str, str]] = None,
      postprocess: Optional[dict[str, str]] = None,
      inference_fn: Optional[dict[str, str]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      keyed_tensor: bool = False,
      input_field: Optional[Union[str, list[str]]] = None,
      **kwargs):
    if not torch_script_model_path and not (state_dict_path and model_class):
      raise ValueError(
          "PyTorch model handler requires either 'torch_script_model_path' or "
          "both 'state_dict_path' and 'model_class'.")
    clean_kwargs = {k: v for k, v in kwargs.items() if not k.startswith('_')}
    model_class_obj = (
        model_class
        if callable(model_class) else self.parse_processing_transform(
            model_class, 'model_class') if model_class else None)
    inference_fn_obj = (
        self.parse_processing_transform(inference_fn, 'inference_fn')
        if inference_fn else None)

    def _build_pytorch_handler():
      try:
        from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
        from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
      except ImportError:
        raise ValueError(
            'Unable to import PyTorch ModelHandler. Please install torch.')
      handler_cls = (
          PytorchModelHandlerKeyedTensor
          if keyed_tensor else PytorchModelHandlerTensor)
      extra: dict[str, Any] = {}
      if inference_fn_obj is not None:
        extra['inference_fn'] = inference_fn_obj
      return handler_cls(
          state_dict_path=state_dict_path,
          model_class=model_class_obj,
          model_params=model_params,
          torch_script_model_path=torch_script_model_path,
          device=device,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
          max_batch_duration_secs=max_batch_duration_secs,
          large_model=large_model,
          **extra,
          **clean_kwargs)

    _handler = _LazyYamlModelHandler(
        _build_pytorch_handler,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        large_model=large_model)
    super().__init__(
        _handler,
        preprocess,
        postprocess,
        input_field=input_field,
        input_mode='torch_keyed' if keyed_tensor else 'torch')

  @staticmethod
  def validate(config):
    if not config or (
        not config.get('torch_script_model_path') and
        not (config.get('state_dict_path') and config.get('model_class'))):
      raise ValueError(
          "PyTorch model handler requires either 'torch_script_model_path' or "
          "both 'state_dict_path' and 'model_class'.")


@ModelHandlerProvider.register_handler_type('PytorchModelHandlerKeyedTensor')
class PytorchKeyedTensorModelHandlerProvider(PytorchModelHandlerProvider):
  def __init__(self, **kwargs):
    kwargs.setdefault('keyed_tensor', True)
    super().__init__(**kwargs)


@ModelHandlerProvider.register_handler_type('Sklearn')
@ModelHandlerProvider.register_handler_type('SklearnModelHandlerNumpy')
class SklearnModelHandlerProvider(ModelHandlerProvider):
  def __init__(
      self,
      model_uri: str = '',
      model_file_type: str = 'pickle',
      input_data_type: str = 'numpy',
      preprocess: Optional[dict[str, str]] = None,
      postprocess: Optional[dict[str, str]] = None,
      inference_fn: Optional[dict[str, str]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      input_field: Optional[Union[str, list[str]]] = None,
      **kwargs):
    if not model_uri:
      raise ValueError("Sklearn model handler requires 'model_uri'.")
    clean_kwargs = {k: v for k, v in kwargs.items() if not k.startswith('_')}
    inference_fn_obj = (
        self.parse_processing_transform(inference_fn, 'inference_fn')
        if inference_fn else None)
    is_pandas = str(input_data_type).lower() == 'pandas'

    def _build_sklearn_handler():
      try:
        from apache_beam.ml.inference.sklearn_inference import ModelFileType
        from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
        from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerPandas
      except ImportError:
        raise ValueError(
            'Unable to import Sklearn ModelHandler. Please install '
            'scikit-learn.')
      file_type = (
          ModelFileType.JOBLIB
          if str(model_file_type).lower() == 'joblib' else ModelFileType.PICKLE)
      handler_cls = (
          SklearnModelHandlerPandas if is_pandas else SklearnModelHandlerNumpy)
      extra: dict[str, Any] = {}
      if inference_fn_obj is not None:
        extra['inference_fn'] = inference_fn_obj
      return handler_cls(
          model_uri=model_uri,
          model_file_type=file_type,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
          max_batch_duration_secs=max_batch_duration_secs,
          large_model=large_model,
          **extra,
          **clean_kwargs)

    _handler = _LazyYamlModelHandler(
        _build_sklearn_handler,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        large_model=large_model)
    super().__init__(
        _handler,
        preprocess,
        postprocess,
        input_field=input_field,
        input_mode='pandas' if is_pandas else 'numpy')

  @staticmethod
  def validate(config):
    if not config or not config.get('model_uri'):
      raise ValueError("Sklearn model handler requires 'model_uri'.")


@ModelHandlerProvider.register_handler_type('SklearnModelHandlerPandas')
class SklearnPandasModelHandlerProvider(SklearnModelHandlerProvider):
  def __init__(self, **kwargs):
    kwargs.setdefault('input_data_type', 'pandas')
    super().__init__(**kwargs)


@ModelHandlerProvider.register_handler_type('Custom')
@ModelHandlerProvider.register_handler_type('CustomModelHandler')
class CustomModelHandlerProvider(ModelHandlerProvider):
  def __init__(
      self,
      callable: Optional[Union[str, Callable[..., Any]]] = None,  # pylint: disable=redefined-builtin
      path: Optional[str] = None,
      name: Optional[str] = None,
      inference_fn: Optional[Union[dict[str, str], str, Callable[...,
                                                                 Any]]] = None,
      load_model_fn: Optional[Union[dict[str, str], str, Callable[[],
                                                                  Any]]] = None,
      model_kwargs: Optional[dict[str, Any]] = None,
      preprocess: Optional[dict[str, str]] = None,
      postprocess: Optional[dict[str, str]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      large_model: bool = False,
      input_field: Optional[Union[str, list[str]]] = None,
      **kwargs):
    if not (callable or (path and name) or inference_fn):
      raise ValueError(
          "Custom model handler requires either 'callable', 'path' and 'name', "
          "or 'inference_fn'.")
    clean_kwargs = {k: v for k, v in kwargs.items() if not k.startswith('_')}
    builtins_callable = __builtins__['callable'] if isinstance(
        __builtins__, dict) else __builtins__.callable

    def _resolve_fn(spec, label):
      if spec is None:
        return None
      if builtins_callable(spec):
        return spec
      if isinstance(spec, str):
        return python_callable.PythonCallableWithSource(spec)
      if isinstance(spec, dict):
        return self.parse_processing_transform(spec, label)
      raise ValueError(f"Invalid {label} specification: {spec}")

    factory_spec = ({
        'callable': callable, 'path': path, 'name': name
    } if (callable or path or name) else None)
    factory_fn = (
        callable
        if builtins_callable(callable) else self.parse_processing_transform(
            factory_spec, 'custom_model_handler') if factory_spec else None)
    resolved_inference_fn = _resolve_fn(inference_fn, 'inference_fn')
    resolved_load_fn = _resolve_fn(load_model_fn, 'load_model_fn')

    def _build_custom_handler():
      if factory_fn is not None:
        init_kwargs = dict(model_kwargs or {})
        init_kwargs.update(clean_kwargs)
        return (
            factory_fn if isinstance(factory_fn, ModelHandler) else factory_fn(
                **init_kwargs))
      return _FunctionModelHandler(
          inference_fn=resolved_inference_fn,
          load_model_fn=resolved_load_fn,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
          max_batch_duration_secs=max_batch_duration_secs,
          large_model=large_model)

    _handler = _LazyYamlModelHandler(
        _build_custom_handler,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        large_model=large_model)
    super().__init__(_handler, preprocess, postprocess, input_field=input_field)

  @staticmethod
  def validate(config):
    if not config or not (config.get('callable') or
                          (config.get('path') and config.get('name')) or
                          config.get('inference_fn')):
      raise ValueError(
          "Custom model handler requires either 'callable', 'path' and 'name', "
          "or 'inference_fn'.")


class _SafePreprocessDoFn(beam.DoFn):
  def __init__(
      self,
      preprocess_fn: Callable[[Any], Any],
      dead_letter_tag: Optional[str] = None):
    self._preprocess_fn = preprocess_fn
    self._dead_letter_tag = dead_letter_tag

  def process(self, row: Any, *args, **kwargs):
    try:
      yield (row, self._preprocess_fn(row))
    except Exception as exn:
      if self._dead_letter_tag:
        yield pvalue.TaggedOutput(
            self._dead_letter_tag,
            (
                row,
                (
                    type(exn),
                    repr(exn),
                    traceback.format_exception(*sys.exc_info()))))
      else:
        raise


class _PostprocessAndAttachDoFn(beam.DoFn):
  def __init__(
      self,
      postprocess_fn: Callable[[Any], Any],
      output_field: str,
      flatten_predictions: bool = False,
      unwrap_prediction_result: bool = False,
      dead_letter_tag: Optional[str] = None):
    self._postprocess_fn = postprocess_fn
    self._output_field = output_field
    self._flatten_predictions = flatten_predictions
    self._unwrap_prediction_result = unwrap_prediction_result
    self._dead_letter_tag = dead_letter_tag

  @staticmethod
  def _unwrap(pred: Any) -> Any:
    val = pred.inference if isinstance(pred, PredictionResult) else pred
    if (isinstance(val, (list, tuple)) and len(val) == 1 and
        (isinstance(val[0], Mapping) or hasattr(val[0], '_asdict'))):
      val = val[0]
    return val

  def process(self, element: tuple[Any, Any], *args, **kwargs):
    row, raw_outcome = element
    if self._dead_letter_tag is not None:
      status, payload = raw_outcome
      if status == _ERR:
        yield pvalue.TaggedOutput(self._dead_letter_tag, (row, payload))
        return
      raw_pred = payload
    else:
      raw_pred = raw_outcome

    try:
      processed = self._postprocess_fn(raw_pred)
      row_dict = dict(row._asdict()) if hasattr(row, '_asdict') else dict(row)

      if self._flatten_predictions:
        val = self._unwrap(processed)
        if hasattr(val, '_asdict'):
          val = val._asdict()
        if isinstance(val, Mapping):
          for k, v in val.items():
            row_dict[f"{self._output_field}_{k}"] = _to_python_scalar(v)
        else:
          row_dict[self._output_field] = _to_python_scalar(val)
      else:
        if self._unwrap_prediction_result:
          val = self._unwrap(processed)
          if isinstance(val, Mapping):
            val = beam.Row(**{k: _to_python_scalar(v) for k, v in val.items()})
          else:
            val = _to_python_scalar(val)
        else:
          val = processed
        row_dict[self._output_field] = val

      yield beam.Row(**row_dict)
    except Exception as exn:
      if self._dead_letter_tag:
        yield pvalue.TaggedOutput(
            self._dead_letter_tag,
            (
                row,
                (
                    type(exn),
                    repr(exn),
                    traceback.format_exception(*sys.exc_info()))))
      else:
        raise


class _YamlRunInferenceTransform(beam.PTransform):
  def __init__(
      self,
      model_handler_provider: ModelHandlerProvider,
      output_field: str,
      inference_args: Optional[dict[str, Any]] = None,
      flatten_predictions: bool = False,
      output_type: Optional[Any] = None,
      declarative_mode: bool = False):
    self._provider = model_handler_provider
    self._output_field = output_field
    self._inference_args = inference_args
    self._flatten_predictions = flatten_predictions
    self._output_type = (
        SafeLineLoader.strip_metadata(output_type)
        if output_type is not None else None)
    self._declarative_mode = declarative_mode
    self._exception_handling_args: Optional[dict[str, Any]] = None

  def with_exception_handling(self, **kwargs):
    self._exception_handling_args = kwargs
    return self

  def _output_fields_for_inference(self) -> list[tuple[str, Any]]:
    if self._flatten_predictions:
      if isinstance(self._output_type, Mapping):
        return [(f"{self._output_field}_{k}", _parse_type_spec(v))
                for k, v in self._output_type.items()]
      return [
          (f"{self._output_field}_label", str),
          (f"{self._output_field}_score", float),
      ]
    if self._output_type is not None:
      return [(self._output_field, _parse_type_spec(self._output_type))]
    inferred_type = (
        Any if
        (self._declarative_mode and not self._provider._has_custom_postprocess)
        else self._provider.inference_output_type())
    return [(self._output_field, inferred_type)]

  def infer_output_type(self, input_type):
    try:
      if isinstance(input_type, RowTypeConstraint):
        base_fields = list(input_type._fields)
      else:
        base_fields = list(named_fields_from_element_type(input_type))
    except Exception:
      return typehints.Any
    if not base_fields:
      return typehints.Any
    new_fields = self._output_fields_for_inference()
    new_names = {name for name, _ in new_fields}
    filtered_base = [(name, typ) for name, typ in base_fields
                     if name not in new_names]
    return RowTypeConstraint.from_fields(filtered_base + new_fields)

  def expand(self, pcoll):
    inferred_schema = self.infer_output_type(pcoll.element_type)
    underlying = self._provider.underlying_handler()
    catch_errors = self._exception_handling_args is not None
    if isinstance(underlying, _LazyYamlModelHandler):
      underlying.with_error_catching(catch_errors)

    unwrap_pred = (
        self._declarative_mode and not self._provider._has_custom_postprocess)

    if catch_errors:
      assert self._exception_handling_args is not None
      main_tag = self._exception_handling_args.get('main_tag', 'good')
      dead_letter_tag = self._exception_handling_args.get(
          'dead_letter_tag', 'bad')

      pre_res = (
          pcoll
          | "Preprocess" >> beam.ParDo(
              _SafePreprocessDoFn(
                  self._provider._preprocess_fn,
                  dead_letter_tag=dead_letter_tag)).with_outputs(
                      dead_letter_tag, main=main_tag))
      inferred_pcoll = (
          pre_res[main_tag]
          | RunInference(
              model_handler=KeyedModelHandler(underlying),
              inference_args=self._inference_args))
      post_res = (
          inferred_pcoll
          | "PostprocessAndAttach" >> beam.ParDo(
              _PostprocessAndAttachDoFn(
                  postprocess_fn=self._provider._postprocess_fn,
                  output_field=self._output_field,
                  flatten_predictions=self._flatten_predictions,
                  unwrap_prediction_result=unwrap_pred,
                  dead_letter_tag=dead_letter_tag)).with_outputs(
                      dead_letter_tag, main=main_tag))
      main_out = post_res[main_tag]
      if inferred_schema != typehints.Any:
        main_out.element_type = inferred_schema
      all_errors = ((pre_res[dead_letter_tag], post_res[dead_letter_tag])
                    | "FlattenInferenceErrors" >> beam.Flatten())
      return {main_tag: main_out, dead_letter_tag: all_errors}

    pre_pcoll = pcoll | "Preprocess" >> beam.ParDo(
        _SafePreprocessDoFn(self._provider._preprocess_fn))
    inferred_pcoll = pre_pcoll | RunInference(
        model_handler=KeyedModelHandler(underlying),
        inference_args=self._inference_args)
    main_out = inferred_pcoll | "PostprocessAndAttach" >> beam.ParDo(
        _PostprocessAndAttachDoFn(
            postprocess_fn=self._provider._postprocess_fn,
            output_field=self._output_field,
            flatten_predictions=self._flatten_predictions,
            unwrap_prediction_result=unwrap_pred))
    if inferred_schema != typehints.Any:
      main_out.element_type = inferred_schema
    return main_out


@beam.ptransform.ptransform_fn
@maybe_with_exception_handling_transform_fn
def run_inference(
    pcoll,
    model_handler: Optional[dict[str, Any]] = None,
    inference_tag: Optional[str] = 'inference',
    inference_args: Optional[dict[str, Any]] = None,
    *,
    provider: Optional[str] = None,
    model_config: Optional[dict[str, Any]] = None,
    input_field: Optional[Union[str, list[str]]] = None,
    output_field: Optional[str] = None,
    flatten_predictions: bool = False,
    output_type: Optional[Any] = None,
    min_batch_size: Optional[int] = None,
    max_batch_size: Optional[int] = None,
    max_batch_duration_secs: Optional[int] = None,
    large_model: Optional[bool] = None) -> beam.PCollection[beam.Row]:  # pylint: disable=line-too-long
  """
  A transform that takes the input rows, containing examples (or features), for
  use on an ML model. The transform then appends the inferences
  (or predictions) for those examples to the input row.

  A ModelHandler must be passed to the `model_handler` parameter. The
  ModelHandler is responsible for configuring how the ML model will be loaded
  and how input data will be passed to it. Every ModelHandler has a config tag,
  similar to how a transform is defined, where the parameters are defined.

  For example: ::

      - type: RunInference
        config:
          model_handler:
            type: ModelHandler
            config:
              param_1: arg1
              param_2: arg2
              ...

  By default, the RunInference transform will return the
  input row with a single field appended named by the `inference_tag` parameter
  ("inference" by default) that contains the inference directly returned by the
  underlying ModelHandler, after any optional postprocessing.

  For example, if the input had the following: ::

      Row(question="What is a car?")

  The output row would look like: ::

      Row(question="What is a car?", inference=...)

  where the `inference` tag can be overridden with the `inference_tag`
  parameter.

  However, if one specified the following transform config: ::

      - type: RunInference
        config:
          inference_tag: my_inference
          model_handler: ...

  The output row would look like: ::

      Row(question="What is a car?", my_inference=...)

  See more complete documentation on the underlying
  [RunInference](https://beam.apache.org/documentation/ml/inference-overview/)
  transform.

  ### Preprocessing input data

  In most cases, the model will be expecting data in a particular data format,
  whether it be a Python Dict, PyTorch tensor, etc. However, the outputs of all
  built-in Beam YAML transforms are Beam Rows. To allow for transforming
  the Beam Row into a data format the model recognizes, each ModelHandler is
  equipped with a `preprocessing` parameter for performing necessary data
  preprocessing. It is possible for a ModelHandler to define a default
  preprocessing function, but in most cases, one will need to be specified by
  the caller.

  For example, using `callable`: ::

      pipeline:
        type: chain

        transforms:
          - type: Create
            config:
              elements:
                - question: "What is a car?"
                - question: "Where is the Eiffel Tower located?"

          - type: RunInference
            config:
              model_handler:
                type: ModelHandler
                config:
                  param_1: arg1
                  param_2: arg2
                  preprocess:
                    callable: 'lambda row: {"prompt": row.question}'
                  ...

  In the above example, the Create transform generates a collection of two Beam
  Row elements, each with a single field - "question". The model, however,
  expects a Python Dict with a single key, "prompt". In this case, we can
  specify a simple Lambda function (alternatively could define a full function),
  to map the data.

  ### Postprocessing predictions

  It is also possible to define a postprocessing function to postprocess the
  data output by the ModelHandler. See the documentation for the ModelHandler
  you intend to use (list defined below under `model_handler` parameter doc).

  In many cases, before postprocessing, the object
  will be a
  [PredictionResult](https://beam.apache.org/releases/pydoc/BEAM_VERSION/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.PredictionResult). # pylint: disable=line-too-long
  This type behaves very similarly to a Beam Row and fields can be accessed
  using dot notation. However, make sure to check the docs for your ModelHandler
  to see which fields its PredictionResult contains or if it returns a
  different object altogether.

  For example: ::

      - type: RunInference
        config:
          model_handler:
            type: ModelHandler
            config:
              param_1: arg1
              param_2: arg2
              postprocess:
                callable: |
                  def fn(x: PredictionResult):
                    return beam.Row(x.example, x.inference, x.model_id)
              ...

  The above example demonstrates converting the original output data type (in
  this case it is PredictionResult), and converts to a Beam Row, which allows
  for easier mapping in a later transform.

  ### File-based pre/postprocessing functions

  For both preprocessing and postprocessing, it is also possible to specify a
  Python UDF (User-defined function) file that contains the function. This is
  possible by specifying the `path` to the file (local file or GCS path) and
  the `name` of the function in the file.

  For example: ::

      - type: RunInference
        config:
          model_handler:
            type: ModelHandler
            config:
              param_1: arg1
              param_2: arg2
              preprocess:
                path: gs://my-bucket/path/to/preprocess.py
                name: my_preprocess_fn
              postprocess:
                path: gs://my-bucket/path/to/postprocess.py
                name: my_postprocess_fn
              ...

  Args:
    model_handler: Specifies the parameters for the respective
          model_handler in a YAML/JSON format. To see the full set of
          handler_config parameters, see their corresponding doc pages:

            - [VertexAIModelHandlerJSON](https://beam.apache.org/releases/pydoc/current/apache_beam.yaml.yaml_ml.VertexAIModelHandlerJSONProvider) # pylint: disable=line-too-long
            - [HuggingFacePipelineModelHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.yaml.yaml_ml.HuggingFacePipelineModelHandlerProvider) # pylint: disable=line-too-long
    inference_tag: The tag to use for the returned inference. Default is
      'inference'.
    inference_args: Extra arguments for models whose inference call requires
      extra parameters. Make sure to check the underlying ModelHandler docs to
      see which args are allowed.

  """

  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')

  declarative_mode = provider is not None
  if provider is not None and model_handler is not None:
    raise ValueError(
        "Cannot specify both 'provider' and 'model_handler' in RunInference.")
  if provider is not None:
    clean_model_config = (
        dict(SafeLineLoader.strip_metadata(model_config))
        if model_config else {})
    if input_field is not None and 'input_field' not in clean_model_config:
      clean_model_config['input_field'] = input_field
    if min_batch_size is not None and 'min_batch_size' not in clean_model_config:
      clean_model_config['min_batch_size'] = min_batch_size
    if max_batch_size is not None and 'max_batch_size' not in clean_model_config:
      clean_model_config['max_batch_size'] = max_batch_size
    if (max_batch_duration_secs is not None and
        'max_batch_duration_secs' not in clean_model_config):
      clean_model_config['max_batch_duration_secs'] = max_batch_duration_secs
    if large_model is not None and 'large_model' not in clean_model_config:
      clean_model_config['large_model'] = large_model
    model_handler = {'type': provider, 'config': clean_model_config}

  if not isinstance(model_handler, dict):
    raise ValueError(
        'Invalid model_handler specification. Expected dict but was '
        f'{type(model_handler)}.')
  expected_model_handler_params = {'type', 'config'}
  given_model_handler_params = set(
      SafeLineLoader.strip_metadata(model_handler).keys())
  extra_params = given_model_handler_params - expected_model_handler_params
  if extra_params:
    raise ValueError(f'Unexpected parameters in model_handler: {extra_params}')
  missing_params = expected_model_handler_params - given_model_handler_params
  if missing_params:
    raise ValueError(f'Missing parameters in model_handler: {missing_params}')
  typ = model_handler['type']
  model_handler_provider_type = ModelHandlerProvider.handler_types.get(
      typ, None)
  if not model_handler_provider_type:
    raise NotImplementedError(f'Unknown model handler type: {typ}.')

  handler_cfg = dict(SafeLineLoader.strip_metadata(model_handler['config']))
  if input_field is not None and 'input_field' not in handler_cfg:
    handler_cfg['input_field'] = input_field
  if min_batch_size is not None and 'min_batch_size' not in handler_cfg:
    handler_cfg['min_batch_size'] = min_batch_size
  if max_batch_size is not None and 'max_batch_size' not in handler_cfg:
    handler_cfg['max_batch_size'] = max_batch_size
  if (max_batch_duration_secs is not None and
      'max_batch_duration_secs' not in handler_cfg):
    handler_cfg['max_batch_duration_secs'] = max_batch_duration_secs
  if large_model is not None and 'large_model' not in handler_cfg:
    handler_cfg['large_model'] = large_model

  normalized_spec = {'type': typ, 'config': handler_cfg}
  model_handler_provider_type.validate(handler_cfg)
  model_handler_provider = ModelHandlerProvider.create_handler(normalized_spec)

  target_output_field = str(output_field if output_field else inference_tag)

  return pcoll | _YamlRunInferenceTransform(
      model_handler_provider=model_handler_provider,
      output_field=target_output_field,
      inference_args=inference_args,
      flatten_predictions=flatten_predictions,
      output_type=output_type,
      declarative_mode=declarative_mode)


def _config_to_obj(spec):
  if 'type' not in spec:
    raise ValueError(f"Missing type in ML transform spec {spec}")
  if 'config' not in spec:
    raise ValueError(f"Missing config in ML transform spec {spec}")
  constructor = _transform_constructors.get(spec['type'])
  if constructor is None:
    raise ValueError("Unknown ML transform type: %r" % spec['type'])
  return constructor(**spec['config'])


@beam.ptransform.ptransform_fn
def ml_transform(
    pcoll,
    write_artifact_location: Optional[str] = None,
    read_artifact_location: Optional[str] = None,
    transforms: Optional[list[Any]] = None):
  if MLTransform is None:
    raise ValueError(
        'No MLTransform found. Please install tensorflow-transform or '
        'sentence-transformers to use this transform.')
  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')
  result_ml_transform = MLTransform(
      write_artifact_location=write_artifact_location,
      read_artifact_location=read_artifact_location,
      transforms=[_config_to_obj(t) for t in transforms] if transforms else [])

  if transforms:
    embedding_transforms = [
        t for t in transforms if t.get('type', '').endswith('Embeddings')
    ]
    if embedding_transforms:
      from apache_beam.typehints import List
      try:
        if pcoll.element_type:
          columns_to_change = {
              col
              for t_spec in embedding_transforms
              for col in t_spec.get('config', {}).get('columns', [])
          }
          new_fields = named_fields_from_element_type(pcoll.element_type)
          final_fields = [
              (name, List[float] if name in columns_to_change else typ)
              for name, typ in new_fields
          ]
          output_schema = RowTypeConstraint.from_fields(final_fields)
          return pcoll | result_ml_transform.with_output_types(output_schema)
      except TypeError:
        # If we can't get a schema, just return the result.
        pass
  return pcoll | result_ml_transform


if MLTransform is not None:
  ml_transform.__doc__ = MLTransform.__doc__
