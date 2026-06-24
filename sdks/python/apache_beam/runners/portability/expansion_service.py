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

"""A PipelineExpansion service.
"""
# pytype: skip-file

import copy
import traceback

from apache_beam import pipeline as beam_pipeline
from apache_beam.options import pipeline_options
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_expansion_api_pb2
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.runners import pipeline_context
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability.artifact_service import BeamFilesystemHandler
from apache_beam.transforms import environments
from apache_beam.transforms import external
from apache_beam.transforms import ptransform


class ExpansionServiceServicer(
    beam_expansion_api_pb2_grpc.ExpansionServiceServicer):
  def __init__(self, options=None, loopback_address=None):
    self._options = options or beam_pipeline.PipelineOptions(
        flags=[],
        environment_type=python_urns.EMBEDDED_PYTHON,
        sdk_location='container')
    default_environment = (environments.Environment.from_options(self._options))
    if loopback_address:
      loopback_environment = environments.Environment.from_options(
          beam_pipeline.PipelineOptions(
              environment_type=common_urns.environments.EXTERNAL.urn,
              environment_config=loopback_address))
      default_environment = environments.AnyOfEnvironment(
          [default_environment, loopback_environment])
    self._default_environment = default_environment

  def Expand(self, request, context=None):
    try:
      options = copy.deepcopy(self._options)
      options = pipeline_options.PipelineOptions.from_runner_api(
          request.pipeline_options, options)
      pipeline = beam_pipeline.Pipeline(options=options)

      def with_pipeline(component, pcoll_id=None):
        component.pipeline = pipeline
        if pcoll_id:
          component.producer, component.tag = producers[pcoll_id]
          # We need the lookup to resolve back to this id.
          context.pcollections._obj_to_id[component] = pcoll_id
        return component

      context = pipeline_context.PipelineContext(
          request.components,
          default_environment=self._default_environment,
          namespace=request.namespace,
          requirements=request.requirements)
      producers = {
          pcoll_id: (context.transforms.get_by_id(t_id), pcoll_tag)
          for t_id, t_proto in request.components.transforms.items()
          for pcoll_tag, pcoll_id in t_proto.outputs.items()
      }
      transform = with_pipeline(
          ptransform.PTransform.from_runner_api(request.transform, context))
      if len(request.output_coder_requests) == 1:
        output_coder = {
            k: context.element_type_from_coder_id(v)
            for k, v in request.output_coder_requests.items()
        }
        transform = transform.with_output_types(list(output_coder.values())[0])
      elif len(request.output_coder_requests) > 1:
        raise ValueError(
            'type annotation for multiple outputs is not allowed yet: %s' %
            request.output_coder_requests)
      inputs = transform._pvaluish_from_dict({
          tag: with_pipeline(
              context.pcollections.get_by_id(pcoll_id), pcoll_id)
          for tag, pcoll_id in request.transform.inputs.items()
      })
      if not inputs:
        inputs = pipeline
      with external.ExternalTransform.outer_namespace(request.namespace):
        result = pipeline.apply(
            transform, inputs, request.transform.unique_name)
      expanded_transform = pipeline._root_transform().parts[-1]
      # TODO(BEAM-1833): Use named outputs internally.
      if isinstance(result, dict):
        expanded_transform.outputs = result
      pipeline_proto = pipeline.to_runner_api(context=context)
      # TODO(BEAM-1833): Use named inputs internally.
      expanded_transform_id = context.transforms.get_id(expanded_transform)
      expanded_transform_proto = pipeline_proto.components.transforms.pop(
          expanded_transform_id)
      expanded_transform_proto.inputs.clear()
      expanded_transform_proto.inputs.update(request.transform.inputs)
      for transform_id in pipeline_proto.root_transform_ids:
        del pipeline_proto.components.transforms[transform_id]
      return beam_expansion_api_pb2.ExpansionResponse(
          components=pipeline_proto.components,
          transform=expanded_transform_proto,
          requirements=pipeline_proto.requirements)

    except Exception:  # pylint: disable=broad-except
      return beam_expansion_api_pb2.ExpansionResponse(
          error=traceback.format_exc())

  def artifact_service(self):
    """Returns a service to retrieve artifacts for use in a job."""
    return artifact_service.ArtifactRetrievalService(
        BeamFilesystemHandler(None).file_reader)
