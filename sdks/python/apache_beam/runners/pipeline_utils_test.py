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

# pytype: skip-file

import hashlib
import os
import random
import tempfile
import unittest

import apache_beam as beam
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.pipeline_utils import merge_common_environments
from apache_beam.runners.pipeline_utils import merge_superset_dep_environments
from apache_beam.runners.portability.expansion_service_test import FibTransform


class PipelineUtilitiesTest(unittest.TestCase):
  def test_equal_environments_merged(self):
    pipeline_proto = merge_common_environments(
        beam_runner_api_pb2.Pipeline(
            components=beam_runner_api_pb2.Components(
                environments={
                    'a1': beam_runner_api_pb2.Environment(urn='A'),
                    'a2': beam_runner_api_pb2.Environment(urn='A'),
                    'b1': beam_runner_api_pb2.Environment(
                        urn='B', payload=b'x'),
                    'b2': beam_runner_api_pb2.Environment(
                        urn='B', payload=b'x'),
                    'b3': beam_runner_api_pb2.Environment(
                        urn='B', payload=b'y'),
                },
                transforms={
                    't1': beam_runner_api_pb2.PTransform(
                        unique_name='t1', environment_id='a1'),
                    't2': beam_runner_api_pb2.PTransform(
                        unique_name='t2', environment_id='a2'),
                },
                windowing_strategies={
                    'w1': beam_runner_api_pb2.WindowingStrategy(
                        environment_id='b1'),
                    'w2': beam_runner_api_pb2.WindowingStrategy(
                        environment_id='b2'),
                })))
    self.assertEqual(len(pipeline_proto.components.environments), 3)
    self.assertTrue(('a1' in pipeline_proto.components.environments)
                    ^ ('a2' in pipeline_proto.components.environments))
    self.assertTrue(('b1' in pipeline_proto.components.environments)
                    ^ ('b2' in pipeline_proto.components.environments))
    self.assertEqual(
        len(
            set(
                t.environment_id
                for t in pipeline_proto.components.transforms.values())),
        1)
    self.assertEqual(
        len(
            set(
                w.environment_id for w in
                pipeline_proto.components.windowing_strategies.values())),
        1)

  def _make_dep(self, path):
    hasher = hashlib.sha256()
    if os.path.exists(path):
      with open(path, 'rb') as fin:
        hasher.update(fin.read())
    else:
      # A fake file, identified only by its path.
      hasher.update(path.encode('utf-8'))
    return beam_runner_api_pb2.ArtifactInformation(
        type_urn=common_urns.artifact_types.FILE.urn,
        type_payload=beam_runner_api_pb2.ArtifactFilePayload(
            path=path, sha256=hasher.hexdigest()).SerializeToString(),
        role_urn=common_urns.artifact_roles.STAGING_TO.urn,
        role_payload=beam_runner_api_pb2.ArtifactStagingToRolePayload(
            staged_name=os.path.basename(path)).SerializeToString())

  def _docker_env(self, id, deps=()):
    return beam_runner_api_pb2.Environment(
        urn=common_urns.environments.DOCKER.urn,
        payload=id.encode('utf8'),
        dependencies=[self._make_dep(path) for path in deps],
    )

  def test_subset_deps_environments_merged(self):
    environments = {
        'A': self._docker_env('A'),
        'Ax': self._docker_env('A', ['x']),
        'Ay': self._docker_env('A', ['y']),
        'Axy': self._docker_env('A', ['x', 'y']),
        'Bx': self._docker_env('B', ['x']),
        'Bxy': self._docker_env('B', ['x', 'y']),
        'Byz': self._docker_env('B', ['y', 'z']),
    }
    transforms = {
        env_id: beam_runner_api_pb2.PTransform(
            unique_name=env_id, environment_id=env_id)
        for env_id in environments.keys()
    }
    pipeline_proto = merge_superset_dep_environments(
        beam_runner_api_pb2.Pipeline(
            components=beam_runner_api_pb2.Components(
                environments=environments, transforms=transforms)))

    # These can all be merged into the same environment.
    self.assertEqual(
        pipeline_proto.components.transforms['A'].environment_id, 'Axy')
    self.assertEqual(
        pipeline_proto.components.transforms['Ax'].environment_id, 'Axy')
    self.assertEqual(
        pipeline_proto.components.transforms['Ay'].environment_id, 'Axy')
    self.assertEqual(
        pipeline_proto.components.transforms['Axy'].environment_id, 'Axy')
    # Despite having the same dependencies, these must be merged into their own.
    self.assertEqual(
        pipeline_proto.components.transforms['Bx'].environment_id, 'Bxy')
    self.assertEqual(
        pipeline_proto.components.transforms['Bxy'].environment_id, 'Bxy')
    # This is not a subset of any, must be left alone.
    self.assertEqual(
        pipeline_proto.components.transforms['Byz'].environment_id, 'Byz')

  def test_subset_deps_environments_merged_with_requirements_txt(self):
    with tempfile.TemporaryDirectory() as tmpdir:

      def make_file(basename, content):
        subdir = tempfile.mkdtemp(dir=tmpdir)
        path = os.path.join(subdir, basename)
        with open(path, 'w') as fout:
          fout.write(content)
        return path

      def make_py_deps(*pkgs):
        return [
            make_file('requirements.txt', '\n'.join(pkgs)),
            make_file(
                'submission_environment_dependencies.txt', str(
                    random.random())),
        ] + [make_file(pkg, pkg) for pkg in pkgs]

      environments = {
          'A': self._docker_env('A'),
          'Ax': self._docker_env('A', make_py_deps('x')),
          'Ay': self._docker_env('A', make_py_deps('y')),
          'Axy': self._docker_env('A', make_py_deps('x', 'y')),
      }
      transforms = {
          env_id: beam_runner_api_pb2.PTransform(
              unique_name=env_id, environment_id=env_id)
          for env_id in environments.keys()
      }
      pipeline_proto = merge_superset_dep_environments(
          beam_runner_api_pb2.Pipeline(
              components=beam_runner_api_pb2.Components(
                  environments=environments, transforms=transforms)))

      # These can all be merged into the same environment.
      self.assertEqual(
          pipeline_proto.components.transforms['A'].environment_id, 'Axy')
      self.assertEqual(
          pipeline_proto.components.transforms['Ax'].environment_id, 'Axy')
      self.assertEqual(
          pipeline_proto.components.transforms['Ay'].environment_id, 'Axy')
      self.assertEqual(
          pipeline_proto.components.transforms['Axy'].environment_id, 'Axy')

  def test_external_merged(self):
    p = beam.Pipeline()
    # This transform recursively creates several external environments.
    _ = p | FibTransform(4)
    pipeline_proto = p.to_runner_api()
    # All our external environments are equal and consolidated.
    # We also have a placeholder "default" environment that has not been
    # resolved do anything concrete yet.
    envs = pipeline_proto.components.environments
    self.assertEqual(
        len(envs), 2, f'should be 2 environments, instead got: {envs}')


if __name__ == '__main__':
  unittest.main()
