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

"""SdkContainerBuilder builds the portable SDK container with dependencies.

It copies the right boot dependencies, namely: apache beam sdk, python packages
from requirements.txt, python packages from extra_packages.txt, workflow
tarball, into the latest public python sdk container image, and run the
dependencies installation in advance with the boot program in setup only mode
to build the new image.
"""

from __future__ import absolute_import

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from google.protobuf.json_format import MessageToJson

from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability.stager import Stager

ARTIFACTS_CONTAINER_DIR = '/opt/apache/beam/artifacts'
ARTIFACTS_MANIFEST_FILE = 'artifacts_info.json'
DOCKERFILE_TEMPLATE = (
    """FROM apache/beam_python{major}.{minor}_sdk:latest
RUN mkdir -p {workdir}
COPY ./* {workdir}/
RUN /opt/apache/beam/boot --setup_only --artifacts {workdir}/{manifest_file}
""")

_LOGGER = logging.getLogger(__name__)


class SdkContainerBuilder(object):
  def __init__(self, options):
    self._options = options
    self._temp_src_dir = tempfile.mkdtemp()
    self._docker_registry = self._options.view_as(
        SetupOptions).docker_registry_url

  def build(self):
    container_id = str(uuid.uuid4())
    container_tag = os.path.join(
        self._docker_registry or '',
        'beam_python_prebuilt_sdk:%s' % container_id)
    self.prepare_dependencies()
    self.invoke_docker_build_and_push(container_id, container_tag)

    return container_tag

  def prepare_dependencies(self):
    tmp = tempfile.mkdtemp()
    resources = Stager.create_job_resources(self._options, tmp)
    # make a copy of the staged artifacts into the temp source folder.
    for path, name in resources:
      shutil.copyfile(path, os.path.join(self._temp_src_dir, name))
    with open(os.path.join(self._temp_src_dir, 'Dockerfile'), 'w') as file:
      file.write(
          DOCKERFILE_TEMPLATE.format(
              major=sys.version_info[0],
              minor=sys.version_info[1],
              workdir=ARTIFACTS_CONTAINER_DIR,
              manifest_file=ARTIFACTS_MANIFEST_FILE))
    self.generate_artifacts_manifests_json_file(resources, self._temp_src_dir)

  def invoke_docker_build_and_push(self, container_id, container_tag):
    try:
      subprocess.run(['docker', 'build', '.', '-t', container_tag],
                     capture_output=True,
                     check=True,
                     cwd=self._temp_src_dir)
      now = time.time()
      _LOGGER.info("Building sdk container, this may take a few minutes...")
    except subprocess.CalledProcessError as err:
      raise RuntimeError(
          'Failed to build sdk container with local docker, '
          'stderr:\n %s.' % err.stderr)
    else:
      _LOGGER.info(
          "Successfully built %s in %.2f seconds" %
          (container_tag, time.time() - now))

    if self._docker_registry:
      _LOGGER.info("Pushing prebuilt sdk container...")
      try:
        subprocess.run(['docker', 'push', container_tag],
                       capture_output=True,
                       check=True)
      except subprocess.CalledProcessError as err:
        raise RuntimeError(
            'Failed to push prebuilt sdk container %s, stderr: \n%s' %
            (container_tag, err.stderr))
      _LOGGER.info(
          "Successfully pushed %s in %.2f seconds" %
          (container_tag, time.time() - now))
    else:
      _LOGGER.info(
          "no --docker_registry option is specified in pipeline "
          "options, specify it if the new image is intended to be "
          "pushed to a registry.")

  @staticmethod
  def generate_artifacts_manifests_json_file(resources, temp_dir):
    infos = []
    for _, name in resources:
      info = beam_runner_api_pb2.ArtifactInformation(
          type_urn=common_urns.StandardArtifacts.Types.FILE.urn,
          type_payload=beam_runner_api_pb2.ArtifactFilePayload(
              path=os.path.join(ARTIFACTS_CONTAINER_DIR,
                                name)).SerializeToString(),
      )
      infos.append(json.dumps(MessageToJson(info)))
    with open(os.path.join(temp_dir, ARTIFACTS_MANIFEST_FILE), 'w') as file:
      file.write('[\n' + ',\n'.join(infos) + '\n]')
