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

# this will make using the list parameterized generic happy
# on python 3.8 so we aren't revisiting this code after we
# sunset it
from __future__ import annotations

import logging
import os
import platform
import shutil
import stat
import subprocess
import typing
import urllib
import zipfile
from urllib.error import URLError
from urllib.request import urlopen

from apache_beam.io.filesystems import FileSystems
from apache_beam.options import pipeline_options
from apache_beam.runners.portability import job_server
from apache_beam.runners.portability import portable_runner
from apache_beam.transforms import environments
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version

# pytype: skip-file

# Prefix for constructing a download URL
GITHUB_DOWNLOAD_PREFIX = 'https://github.com/apache/beam/releases/download/'
# Prefix for constructing a release URL, so we can derive a download URL
GITHUB_TAG_PREFIX = 'https://github.com/apache/beam/releases/tag/'

_LOGGER = logging.getLogger(__name__)


class PrismRunner(portable_runner.PortableRunner):
  """A runner for launching jobs on Prism, automatically downloading and
  starting a Prism instance if needed.
  """
  def default_environment(
      self,
      options: pipeline_options.PipelineOptions) -> environments.Environment:
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super().default_environment(options)

  def default_job_server(self, options):
    return job_server.StopOnExitJobServer(PrismJobServer(options))

  def create_job_service_handle(self, job_service, options):
    return portable_runner.JobServiceHandle(
        job_service, options, retain_unknown_options=True)


class PrismJobServer(job_server.SubprocessJobServer):
  PRISM_CACHE = os.path.expanduser("~/.apache_beam/cache/prism")
  BIN_CACHE = os.path.expanduser("~/.apache_beam/cache/prism/bin")

  def __init__(self, options):
    super().__init__()
    prism_options = options.view_as(pipeline_options.PrismRunnerOptions)
    # Options flow:
    # If the path is set, always download and unzip the provided path,
    # even if a binary is cached.
    self._path = prism_options.prism_location
    # Which version to use when constructing the prism download url.
    if prism_options.prism_beam_version_override:
      self._version = prism_options.prism_beam_version_override
    else:
      self._version = 'v' + beam_version

    job_options = options.view_as(pipeline_options.JobServerOptions)
    self._job_port = job_options.job_port

  @classmethod
  def maybe_unzip_and_make_executable(cls, url: str, bin_cache: str) -> str:
    if zipfile.is_zipfile(url):
      z = zipfile.ZipFile(url)
      url = z.extract(
          os.path.splitext(os.path.basename(url))[0], path=bin_cache)

    # Make sure the binary is executable.
    st = os.stat(url)
    os.chmod(url, st.st_mode | stat.S_IEXEC)
    return url

  # Finds the bin or zip in the local cache, and if not, fetches it.
  @classmethod
  def local_bin(
      cls, url: str, bin_cache: str = '', ignore_cache: bool = False) -> str:
    # ignore_cache sets whether we should always be downloading and unzipping
    # the file or not, to avoid staleness issues.
    if bin_cache == '':
      bin_cache = cls.BIN_CACHE
    if os.path.exists(url):
      _LOGGER.info('Using local prism binary from %s' % url)
      return cls.maybe_unzip_and_make_executable(url, bin_cache=bin_cache)
    else:
      cached_bin = os.path.join(bin_cache, os.path.basename(url))
      if os.path.exists(cached_bin) and not ignore_cache:
        _LOGGER.info('Using cached prism binary from %s' % url)
      else:
        _LOGGER.info('Downloading prism binary from %s' % url)
        if not os.path.exists(bin_cache):
          os.makedirs(bin_cache)
        try:
          try:
            url_read = FileSystems.open(url)
          except ValueError:
            url_read = urlopen(url)
          with open(cached_bin + '.tmp', 'wb') as zip_write:
            shutil.copyfileobj(url_read, zip_write, length=1 << 20)
          os.rename(cached_bin + '.tmp', cached_bin)
        except URLError as e:
          raise RuntimeError(
              'Unable to fetch remote prism binary at %s: %s' % (url, e))
      return cls.maybe_unzip_and_make_executable(
          cached_bin, bin_cache=bin_cache)

  def construct_download_url(self, root_tag: str, sys: str, mach: str) -> str:
    """Construct the prism download URL with the appropriate release tag.
    This maps operating systems and machine architectures to the compatible
    and canonical names used by the Go build targets.

    platform.system() provides compatible listings, so we need to filter out
    the unsupported versions."""
    opsys = sys.lower()
    if opsys not in ['linux', 'windows', 'darwin']:
      raise ValueError(
          'Operating System "%s" unsupported for constructing a Prism release '
          'binary URL.' % (opsys))

    # platform.machine() will vary by system, but many names are compatible.
    arch = mach.lower()
    if arch in ['amd64', 'x86_64', 'x86-64', 'x64']:
      arch = 'amd64'
    if arch in ['arm64', 'aarch64_be', 'aarch64', 'armv8b', 'armv8l']:
      arch = 'arm64'

    if arch not in ['amd64', 'arm64']:
      raise ValueError(
          'Machine archictecture "%s" unsupported for constructing a Prism '
          'release binary URL.' % (opsys))
    return (
        GITHUB_DOWNLOAD_PREFIX +
        f"{root_tag}/apache_beam-{self._version}-prism-{opsys}-{arch}.zip")

  def path_to_binary(self) -> str:
    if self._path is not None:
      # The path is overidden, check various cases.
      if os.path.exists(self._path):
        # The path is local and exists, use directly.
        return self.bin_path

      # Check if the path is a URL.
      url = urllib.parse.urlparse(self._path)
      if not url.scheme:
        raise ValueError(
            'Unable to parse binary URL "%s". If using a full URL, make '
            'sure the scheme is specified. If using a local file xpath, '
            'make sure the file exists; you may have to first build prism '
            'using `go build `.' % (self._path))

      # We have a URL, see if we need to construct a valid file name.
      if self._path.startswith(GITHUB_DOWNLOAD_PREFIX):
        # If this URL starts with the download prefix, let it through.
        return self._path
      # The only other valid option is a github release page.
      if not self._path.startswith(GITHUB_TAG_PREFIX):
        raise ValueError(
            'Provided --prism_location URL is not an Apache Beam Github '
            'Release page URL or download URL: %s' % (self._path))
      # Get the root tag for this URL
      root_tag = os.path.basename(os.path.normpath(self._path))
      return self.construct_download_url(
          root_tag, platform.system(), platform.machine())

    if '.dev' not in self._version:
      # Not a development version, so construct the production download URL
      return self.construct_download_url(
          self._version, platform.system(), platform.machine())

    # This is a development version! Assume Go is installed.
    # Set the install directory to the cache location.
    envdict = dict(os.environ) | {"GOBIN": self.BIN_CACHE}
    PRISMPKG = "github.com/apache/beam/sdks/v2/go/cmd/prism"

    process = subprocess.run(["go", "install", PRISMPKG],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             env=envdict)
    if process.returncode == 0:
      # Successfully installed
      return '%s/prism' % (self.BIN_CACHE)

    # We failed to build for some reason.
    output = process.stdout.decode("utf-8")
    if "not in a module" not in output and "no required module provides" not in output:
      # This branch handles two classes of failures:
      # 1. Go isn't installed, so it needs to be installed by the Beam SDK developer.
      # 2. Go is installed, and they are building in a local version of Prism,
      #    but there was a compile error that the developer should address.
      # Either way, the @latest fallback either would fail, or hide the error, so fail now.
      _LOGGER.info(output)
      raise ValueError(
          'Unable to install a local of Prism: "%s";\n'
          'Likely Go is not installed, or a local change to Prism did not compile.\n'
          'Please install Go (see https://go.dev/doc/install) to enable automatic local builds.\n'
          'Alternatively provide a binary with the --prism_location flag.'
          '\nCaptured output:\n %s' % (self._version, output))

    # Go is installed and claims we're not in a Go module that has access to the Prism package.

  # Fallback to using the @latest version of prism, which works everywhere.
    process = subprocess.run(["go", "install", PRISMPKG + "@latest"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             env=envdict)

    if process.returncode == 0:
      return '%s/prism' % (self.BIN_CACHE)

    output = process.stdout.decode("utf-8")
    raise ValueError(
        'We were unable to execute the subprocess "%s" to automatically build prism. \n'
        'Alternatively provide an alternate binary with the --prism_location flag.'
        '\nCaptured output:\n %s' % (process.args, output))

  def subprocess_cmd_and_endpoint(
      self) -> typing.Tuple[typing.List[typing.Any], str]:
    bin_path = self.local_bin(
        self.path_to_binary(), ignore_cache=(self._path is not None))
    job_port, = subprocess_server.pick_port(self._job_port)
    subprocess_cmd = [bin_path] + self.prism_arguments(job_port)
    return (subprocess_cmd, f"localhost:{job_port}")

  def prism_arguments(self, job_port) -> typing.List[typing.Any]:
    return [
        '--job_port',
        job_port,
        '--serve_http',
        False,
    ]
