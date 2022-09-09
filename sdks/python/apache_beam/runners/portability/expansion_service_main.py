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

import argparse
import logging
import signal
import sys

import grpc

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import expansion_service
from apache_beam.transforms import fully_qualified_named_transform
from apache_beam.utils import thread_pool_executor

_LOGGER = logging.getLogger(__name__)


def main(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-p', '--port', type=int, help='port on which to serve the job api')
  parser.add_argument('--fully_qualified_name_glob', default=None)
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(
      pipeline_args + ["--experiments=beam_fn_api", "--sdk_location=container"])

  # Set this before any pipeline construction occurs.
  # See https://github.com/apache/beam/issues/21615
  pickler.set_library(pipeline_options.view_as(SetupOptions).pickle_library)

  with fully_qualified_named_transform.FullyQualifiedNamedTransform.with_filter(
      known_args.fully_qualified_name_glob):

    server = grpc.server(thread_pool_executor.shared_unbounded_instance())
    beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
        expansion_service.ExpansionServiceServicer(pipeline_options), server)
    beam_artifact_api_pb2_grpc.add_ArtifactRetrievalServiceServicer_to_server(
        artifact_service.ArtifactRetrievalService(
            artifact_service.BeamFilesystemHandler(None).file_reader),
        server)
    server.add_insecure_port('localhost:{}'.format(known_args.port))
    server.start()
    _LOGGER.info('Listening for expansion requests at %d', known_args.port)

    def cleanup(unused_signum, unused_frame):
      _LOGGER.info('Shutting down expansion service.')
      server.stop(None)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    # blocking main thread forever.
    signal.pause()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv)
