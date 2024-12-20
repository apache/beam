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
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference import RunInference
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.utils import python_callable
from apache_beam.yaml import options
from apache_beam.yaml.yaml_utils import SafeLineLoader

try:
  from apache_beam.ml.transforms import tft
  from apache_beam.ml.transforms.base import MLTransform
  # TODO(robertwb): Is this all of them?
  _transform_constructors = tft.__dict__
except ImportError:
  tft = None  # type: ignore


class ModelHandlerProvider:
  handler_types: Dict[str, Callable[..., "ModelHandlerProvider"]] = {}

  def __init__(
      self,
      handler,
      preprocess: Optional[Dict[str, str]] = None,
      postprocess: Optional[Dict[str, str]] = None):
    self._handler = handler
    self._preprocess_fn = self.parse_processing_transform(
        preprocess, 'preprocess') or self.default_preprocess_fn()
    self._postprocess_fn = self.parse_processing_transform(
        postprocess, 'postprocess') or self.default_postprocess_fn()

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


@ModelHandlerProvider.register_handler_type('VertexAIModelHandlerJSON')
class VertexAIModelHandlerJSONProvider(ModelHandlerProvider):
  def __init__(
      self,
      endpoint_id: str,
      project: str,
      location: str,
      preprocess: Dict[str, str],
      postprocess: Optional[Dict[str, str]] = None,
      experiment: Optional[str] = None,
      network: Optional[str] = None,
      private: bool = False,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      env_vars: Optional[Dict[str, Any]] = None):
    """
    ModelHandler for Vertex AI.

    This Model Handler can be used with RunInference to load a model hosted
    on VertexAI. Every model that is hosted on VertexAI should have three
    distinct, required, parameters - `endpoint_id`, `project` and `location`.
    These parameters tell the Model Handler how to access the model's endpoint
    so that input data can be sent using an API request, and inferences can be
    received as a response.

    This Model Handler also required a `preprocess` function to be defined.
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
      min_batch_size: The minimum batch size to use when batching
        inputs.
      max_batch_size: The maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: The maximum amount of time to buffer
        a batch before emitting; used in streaming contexts.
      env_vars: Environment variables.
    """

    try:
      from apache_beam.ml.inference.vertex_ai_inference import VertexAIModelHandlerJSON
    except ImportError:
      raise ValueError(
          'Unable to import VertexAIModelHandlerJSON. Please '
          'install gcp dependencies: `pip install apache_beam[gcp]`')

    _handler = VertexAIModelHandlerJSON(
        endpoint_id=str(endpoint_id),
        project=project,
        location=location,
        experiment=experiment,
        network=network,
        private=private,
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        env_vars=env_vars or {})

    super().__init__(_handler, preprocess, postprocess)

  @staticmethod
  def validate(model_handler_spec):
    pass

  def inference_output_type(self):
    return RowTypeConstraint.from_fields([('example', Any), ('inference', Any),
                                          ('model_id', Optional[str])])


@beam.ptransform.ptransform_fn
def run_inference(
    pcoll,
    model_handler: Dict[str, Any],
    inference_tag: Optional[str] = 'inference',
    inference_args: Optional[Dict[str, Any]] = None) -> beam.PCollection[beam.Row]:  # pylint: disable=line-too-long
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
          enrichment_handler in a YAML/JSON format. To see the full set of
          handler_config parameters, see their corresponding doc pages:

            - [VertexAIModelHandlerJSON](https://beam.apache.org/releases/pydoc/current/apache_beam.yaml.yaml_ml.VertexAIModelHandlerJSONProvider) # pylint: disable=line-too-long
    inference_tag: The tag to use for the returned inference. Default is
      'inference'.
    inference_args: Extra arguments for models whose inference call requires
      extra parameters. Make sure to check the underlying ModelHandler docs to
        see which args are allowed.

  """

  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')

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
  model_handler_provider = ModelHandlerProvider.handler_types.get(typ, None)
  if model_handler_provider and issubclass(model_handler_provider,
                                           ModelHandlerProvider):
    model_handler_provider.validate(model_handler['config'])
  else:
    raise NotImplementedError(f'Unknown model handler type: {typ}.')

  model_handler_provider = ModelHandlerProvider.create_handler(model_handler)
  schema = RowTypeConstraint.from_fields(
      list(
          RowTypeConstraint.from_user_type(
              pcoll.element_type.user_type)._fields) +
      [(inference_tag, model_handler_provider.inference_output_type())])

  return (
      pcoll | RunInference(
          model_handler=KeyedModelHandler(
              model_handler_provider.underlying_handler()).with_preprocess_fn(
                  model_handler_provider._preprocess_fn_internal()).
          with_postprocess_fn(
              model_handler_provider._postprocess_fn_internal()),
          inference_args=inference_args)
      | beam.Map(
          lambda row: beam.Row(**{
              inference_tag: row[1], **row[0]._asdict()
          })).with_output_types(schema))


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
    transforms: Optional[List[Any]] = None):
  if tft is None:
    raise ValueError(
        'tensorflow-transform must be installed to use this MLTransform')
  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')
  # TODO(robertwb): Perhaps _config_to_obj could be pushed into MLTransform
  # itself for better cross-language support?
  return pcoll | MLTransform(
      write_artifact_location=write_artifact_location,
      read_artifact_location=read_artifact_location,
      transforms=[_config_to_obj(t) for t in transforms] if transforms else [])


if tft is not None:
  ml_transform.__doc__ = MLTransform.__doc__
