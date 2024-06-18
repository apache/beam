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

"""A runner for executing portable pipelines on Apache Beam Prism."""

# pytype: skip-file

import logging
import os
import re
import urllib
import shutil
import stat
import zipfile

from apache_beam.io.filesystems import FileSystems
from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version
from urllib.request import urlopen


MAGIC_HOST_NAMES = ['[local]', '[auto]']
GITHUB_DOWNLOAD_PREFIX = 'https://github.com/apache/beam/releases/download/'

_LOGGER = logging.getLogger(__name__)


class PrismRunner(portable_runner.PortableRunner):
  """A runner for launching jobs on Prism, automatically downloading and starting a
  Prism instance if needed.
  """

  # Inherits run_portable_pipeline from PortableRunner.

  def default_environment(self, options):
    portable_options = options.view_as(pipeline_options.PortableOptions)
    prism_options = options.view_as(pipeline_options.PrismRunnerOptions)
    if (not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super().default_environment(options)

  def default_job_server(self, options):
      return job_server.StopOnExitJobServer(PrismJobServer(options))

  def create_job_service_handle(self, job_service, options):
    return portable_runner.JobServiceHandle(
        job_service,
        options,
        retain_unknown_options=True)


class PrismJobServer(job_server.SubprocessJobServer):
  PRISM_CACHE = os.path.expanduser("~/.apache_beam/cache/prism")
  BIN_CACHE = os.path.expanduser("~/.apache_beam/cache/prism/bin")

  def __init__(self, options):
    super().__init__()
    prism_options = options.view_as(pipeline_options.PrismRunnerOptions)
    self._path = prism_options.prism_binary_location
    self._version = prism_options.prism_beam_version_override if prism_options.prism_beam_version_override else beam_version

    job_options = options.view_as(pipeline_options.JobServerOptions)
    self._job_port = job_options.job_port

  # Finds the bin or zip in the local cache, and if not, fetches it.
  @classmethod
  def local_bin(cls, url, cache_dir=None):
    if cache_dir is None:
      cache_dir = cls.BIN_CACHE
    if os.path.exists(url):
      if zipfile.is_zipfile(url): 
        z = zipfile.ZipFile(url)
        url = z.extract(os.path.splitext(os.path.basename(url))[0], path=cache_dir)
      
      # Make the binary executable.
      st = os.stat(url)
      os.chmod(url, st.st_mode | stat.S_IEXEC)
      return url
    else:
      #TODO VALIDATE/REWRITE THE REST OF THIS FUNCTION
      cached_jar = os.path.join(cache_dir, os.path.basename(url))
      if os.path.exists(cached_jar):
        _LOGGER.info('Using cached job server jar from %s' % url)
      else:
        _LOGGER.info('Downloading job server jar from %s' % url)
        if not os.path.exists(cache_dir):
          os.makedirs(cache_dir)
          # TODO: Clean up this cache according to some policy.
        try:
          try:
            url_read = FileSystems.open(url)
          except ValueError:
            url_read = urlopen(url)
          with open(cached_jar + '.tmp', 'wb') as jar_write:
            shutil.copyfileobj(url_read, jar_write, length=1 << 20)
          os.rename(cached_jar + '.tmp', cached_jar)
        except URLError as e:
          raise RuntimeError(
              'Unable to fetch remote job server jar at %s: %s' % (url, e))
      return cached_jar

  def path_to_binary(self):
    if self._path:
      if not os.path.exists(self._path):
        url = urllib.parse.urlparse(self._path)
        if not url.scheme:
          raise ValueError(
              'Unable to parse binary URL "%s". If using a full URL, make sure '
              'the scheme is specified. If using a local file xpath, make sure '
              'the file exists; you may have to first build prism '
              'using `go build `.' %
              (self._path))
      return self._path
    else:
      if '.dev' in self._version:
        raise ValueError(
            'Unable to derive URL for dev versions "%s". Please provide an alternate '
            'version to derive the release URL' %
            (self._version))

      # TODO Add figuring out what platform we're using 
      opsys = 'linux'
      arch = 'amd64'

      return 'http://github.com/apache/beam/releases/download/%s/apache_beam-%s-prism-%s-%s.zip' % (self._version,self._version, opsys, arch)

  def subprocess_cmd_and_endpoint(self):
    bin_path = self.local_bin(self.path_to_binary())
    job_port, = subprocess_server.pick_port(self._job_port)
    subprocess_cmd = [
        bin_path
    ] + list(
        self.prism_arguments(job_port))
    return (subprocess_cmd, 'localhost:%s' % job_port)
  
  def prism_arguments(
      self, job_port):
    return [
        '--job_port',
        job_port,
        '--serve_http',
        False,
    ]
