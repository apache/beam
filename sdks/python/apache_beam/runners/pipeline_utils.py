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

"""Pipeline manipulation utilities useful for many runners.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-fileimport collections

import collections
import copy

from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import environments
from apache_beam.typehints import typehints


def group_by_key_input_visitor(deterministic_key_coders=True):
  # Importing here to avoid a circular dependency
  # pylint: disable=wrong-import-order, wrong-import-position
  from apache_beam.pipeline import PipelineVisitor
  from apache_beam.transforms.core import GroupByKey

  class GroupByKeyInputVisitor(PipelineVisitor):
    """A visitor that replaces `Any` element type for input `PCollection` of
    a `GroupByKey` with a `KV` type.

    TODO(BEAM-115): Once Python SDK is compatible with the new Runner API,
    we could directly replace the coder instead of mutating the element type.
    """
    def __init__(self, deterministic_key_coders=True):
      self.deterministic_key_coders = deterministic_key_coders

    def enter_composite_transform(self, transform_node):
      self.visit_transform(transform_node)

    def visit_transform(self, transform_node):
      if isinstance(transform_node.transform, GroupByKey):
        pcoll = transform_node.inputs[0]
        pcoll.element_type = typehints.coerce_to_kv_type(
            pcoll.element_type, transform_node.full_label)
        pcoll.requires_deterministic_key_coder = (
            self.deterministic_key_coders and transform_node.full_label)
        key_type, value_type = pcoll.element_type.tuple_types
        if transform_node.outputs:
          key = next(iter(transform_node.outputs.keys()))
          transform_node.outputs[key].element_type = typehints.KV[
              key_type, typehints.Iterable[value_type]]
          transform_node.outputs[key].requires_deterministic_key_coder = (
              self.deterministic_key_coders and transform_node.full_label)

  return GroupByKeyInputVisitor(deterministic_key_coders)


def validate_pipeline_graph(pipeline_proto):
  """Ensures this is a correctly constructed Beam pipeline.
  """
  def get_coder(pcoll_id):
    return pipeline_proto.components.coders[
        pipeline_proto.components.pcollections[pcoll_id].coder_id]

  def validate_transform(transform_id):
    transform_proto = pipeline_proto.components.transforms[transform_id]

    # Currently the only validation we perform is that GBK operations have
    # their coders set properly.
    if transform_proto.spec.urn == common_urns.primitives.GROUP_BY_KEY.urn:
      if len(transform_proto.inputs) != 1:
        raise ValueError("Unexpected number of inputs: %s" % transform_proto)
      if len(transform_proto.outputs) != 1:
        raise ValueError("Unexpected number of outputs: %s" % transform_proto)
      input_coder = get_coder(next(iter(transform_proto.inputs.values())))
      output_coder = get_coder(next(iter(transform_proto.outputs.values())))
      if input_coder.spec.urn != common_urns.coders.KV.urn:
        raise ValueError(
            "Bad coder for input of %s: %s" % (transform_id, input_coder))
      if output_coder.spec.urn != common_urns.coders.KV.urn:
        raise ValueError(
            "Bad coder for output of %s: %s" % (transform_id, output_coder))
      output_values_coder = pipeline_proto.components.coders[
          output_coder.component_coder_ids[1]]
      if (input_coder.component_coder_ids[0]
          != output_coder.component_coder_ids[0] or
          output_values_coder.spec.urn != common_urns.coders.ITERABLE.urn or
          output_values_coder.component_coder_ids[0]
          != input_coder.component_coder_ids[1]):
        raise ValueError(
            "Incompatible input coder %s and output coder %s for transform %s" %
            (transform_id, input_coder, output_coder))
    elif transform_proto.spec.urn == common_urns.primitives.ASSIGN_WINDOWS.urn:
      if not transform_proto.inputs:
        raise ValueError("Missing input for transform: %s" % transform_proto)
    elif transform_proto.spec.urn == common_urns.primitives.PAR_DO.urn:
      if not transform_proto.inputs:
        raise ValueError("Missing input for transform: %s" % transform_proto)

    for t in transform_proto.subtransforms:
      validate_transform(t)

  for t in pipeline_proto.root_transform_ids:
    validate_transform(t)


def _dep_key(dep):
  if dep.type_urn == common_urns.artifact_types.FILE.urn:
    payload = beam_runner_api_pb2.ArtifactFilePayload.FromString(
        dep.type_payload)
    if payload.sha256:
      type_info = 'sha256', payload.sha256
    else:
      type_info = 'path', payload.path
  elif dep.type_urn == common_urns.artifact_types.URL.urn:
    payload = beam_runner_api_pb2.ArtifactUrlPayload.FromString(
        dep.type_payload)
    if payload.sha256:
      type_info = 'sha256', payload.sha256
    else:
      type_info = 'url', payload.url
  else:
    type_info = dep.type_urn, dep.type_payload
  return type_info, dep.role_urn, dep.role_payload


def _expanded_dep_keys(dep):
  if (dep.type_urn == common_urns.artifact_types.FILE.urn and
      dep.role_urn == common_urns.artifact_roles.STAGING_TO.urn):
    payload = beam_runner_api_pb2.ArtifactFilePayload.FromString(
        dep.type_payload)
    role = beam_runner_api_pb2.ArtifactStagingToRolePayload.FromString(
        dep.role_payload)
    if role.staged_name == 'submission_environment_dependencies.txt':
      return
    elif role.staged_name == 'requirements.txt':
      with open(payload.path) as fin:
        for line in fin:
          yield 'requirements.txt', line.strip()
      return

  yield _dep_key(dep)


def _base_env_key(env, include_deps=True):
  return (
      env.urn,
      env.payload,
      tuple(sorted(env.capabilities)),
      tuple(sorted(env.resource_hints.items())),
      tuple(sorted(_dep_key(dep)
                   for dep in env.dependencies)) if include_deps else None)


def _env_key(env):
  return tuple(
      sorted(
          _base_env_key(e)
          for e in environments.expand_anyof_environments(env)))


def merge_common_environments(pipeline_proto, inplace=False):
  canonical_environments = collections.defaultdict(list)
  for env_id, env in pipeline_proto.components.environments.items():
    canonical_environments[_env_key(env)].append(env_id)

  if len(canonical_environments) == len(pipeline_proto.components.environments):
    # All environments are already sufficiently distinct.
    return pipeline_proto

  environment_remappings = {
      e: es[0]
      for es in canonical_environments.values()
      for e in es
  }

  return update_environments(pipeline_proto, environment_remappings, inplace)


def merge_superset_dep_environments(pipeline_proto):
  """Merges all environemnts A and B where A and B are equivalent except that
  A has a superset of the dependencies of B.
  """
  docker_envs = {}
  for env_id, env in pipeline_proto.components.environments.items():
    docker_env = environments.resolve_anyof_environment(
        env, common_urns.environments.DOCKER.urn)
    if docker_env.urn == common_urns.environments.DOCKER.urn:
      docker_envs[env_id] = docker_env

  has_base_and_dep = collections.defaultdict(set)
  env_scores = {
      env_id: (len(env.dependencies), env_id)
      for (env_id, env) in docker_envs.items()
  }

  for env_id, env in docker_envs.items():
    base_key = _base_env_key(env, include_deps=False)
    has_base_and_dep[base_key, None].add(env_id)
    for dep in env.dependencies:
      for dep_key in _expanded_dep_keys(dep):
        has_base_and_dep[base_key, dep_key].add(env_id)

  environment_remappings = {}
  for env_id, env in docker_envs.items():
    base_key = _base_env_key(env, include_deps=False)
    # This is the set of all environments that have at least all of env's deps.
    candidates = set.intersection(
        has_base_and_dep[base_key, None],
        *[
            has_base_and_dep[base_key, dep_key] for dep in env.dependencies
            for dep_key in _expanded_dep_keys(dep)
        ])
    # Choose the maximal one.
    best = max(candidates, key=env_scores.get)
    if best != env_id:
      environment_remappings[env_id] = best

  return update_environments(pipeline_proto, environment_remappings)


def update_environments(pipeline_proto, environment_remappings, inplace=False):
  if not environment_remappings:
    return pipeline_proto

  if not inplace:
    pipeline_proto = copy.copy(pipeline_proto)

  for t in pipeline_proto.components.transforms.values():
    if t.environment_id not in pipeline_proto.components.environments:
      # TODO(https://github.com/apache/beam/issues/30876): Remove this
      #  workaround.
      continue
    if t.environment_id and t.environment_id in environment_remappings:
      t.environment_id = environment_remappings[t.environment_id]
  for w in pipeline_proto.components.windowing_strategies.values():
    if w.environment_id not in pipeline_proto.components.environments:
      # TODO(https://github.com/apache/beam/issues/30876): Remove this
      #  workaround.
      continue
    if w.environment_id and w.environment_id in environment_remappings:
      w.environment_id = environment_remappings[w.environment_id]
  for e in set(environment_remappings.keys()) - set(
      environment_remappings.values()):
    del pipeline_proto.components.environments[e]
  return pipeline_proto
