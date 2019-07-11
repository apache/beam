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

"""A runner for executing portable pipelines on Flink."""

from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import shutil

from distutils.version import LooseVersion
from future.moves.urllib.request import urlopen, URLError

from apache_beam.version import __version__ as beam_version
from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner

PUBLISHED_FLINK_VERSIONS = ['1.6', '1.7', '1.8']
MAVEN_REPOSITORY = 'https://repo.maven.apache.org/maven2/org/apache/beam/'
MAVEN_FLINK_JOB_SERVER_TEMPLATE = '{maven_repo}beam-runners-flink-{flink_version}-job-server/{beam_version}/beam-runners-flink-{flink_version}-job-server-{beam_version}.jar'
DEV_FLINK_JOB_SERVER_TEMPLATE = '{project_root}/runners/flink/{flink_version}/job-server/build/libs/beam-runners-flink-{flink_version}-job-server-{beam_version}-SNAPSHOT.jar'
JAR_CACHE = os.path.expanduser("~/.apache_beam/cache")


class FlinkRunner(portable_runner.PortableRunner):
  def default_job_server(self, options):
    return FlinkJarJobServer(options)


class FlinkRunnerOptions(pipeline_options.PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--flink_master_url', default='[local]')
    parser.add_argument('--flink_version',
                        default=PUBLISHED_FLINK_VERSIONS[-1],
                        choices=PUBLISHED_FLINK_VERSIONS,
                        help='Flink version to use.')
    parser.add_argument('--flink_job_server_jar',
                        help='Path or URL to a flink jobserver jar.')



class FlinkJarJobServer(job_server.SubprocessJobServer):
  def __init__(self, options):
    super(FlinkJarJobServer, self).__init__()
    options = options.view_as(FlinkRunnerOptions)
    self._jar = self.local_jar(self.flink_jar(options))
    self._master_url = options.flink_master_url

  def flink_jar(self, options):
    if options.flink_job_server_jar:
      return options.flink_job_server_jar
    else:
      if beam_version.endswith('.dev'):
        # TODO: Attempt to use nightly snapshots?
        project_root = os.path.sep.join(__file__.split(os.path.sep)[:-6])
        dev_path = DEV_FLINK_JOB_SERVER_TEMPLATE.format(
            project_root=project_root,
            flink_version=options.flink_version,
            beam_version=beam_version[:-4])
        if os.path.exists(dev_path):
          logging.warning(
              'Using pre-built flink job server snapshot at %s', dev_path)
          return dev_path
        else:
          raise RuntimeError(
              'Please build the flink job server with '
              'cd %s; ./gradlew runners:flink:%s:job-server:shadowJar' % (
                  os.path.abspath(project_root), options.flink_version))
      else:
        return MAVEN_FLINK_JOB_SERVER_TEMPLATE.format(
            maven_repo=MAVEN_REPOSITORY,
            flink_version=options.flink_version,
            beam_version=beam_version)

  def local_jar(self, url):
    # TODO: Verify checksum?
    if os.path.exists(url):
      return url
    else:
      cached_jar = os.path.join(JAR_CACHE, os.path.basename(url))
      if not os.path.exists(cached_jar):
        if not os.path.exists(JAR_CACHE):
          os.makedirs(JAR_CACHE)
          # TODO: Clean up this cache according to some policy.
        try:
          url_read = urlopen(url)
          with open(cached_jar + '.tmp', 'wb') as jar_write:
            shutil.copyfileobj(url_read, jar_write, length=1 << 20)
          os.rename(cached_jar + '.tmp', cached_jar)
        except URLError as e:
          raise RuntimeError(
              'Unable to fetch remote flink job server jar at %s: %s' % (url, e))
      return cached_jar

  def subprocess_cmd_and_endpoint(self):
    artifacts_dir = self.local_temp_dir(prefix='artifacts')
    job_port, = job_server._pick_port(None)
    return (
        [
            'java',
            '-jar',
            self._jar,
            '--flink-master-url', self._master_url,
            '--artifacts-dir', artifacts_dir,
            '--job-port', job_port,
            '--artifact-port', 0,
            '--expansion-port', 0
        ],
        'localhost:%s' % job_port)
