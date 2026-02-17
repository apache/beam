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

import datetime
import hashlib
import json
import logging
import os
import platform
import re
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
from apache_beam.utils import shared
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
  shared_handle = shared.Shared()

  def default_environment(
      self,
      options: pipeline_options.PipelineOptions) -> environments.Environment:
    portable_options = options.view_as(pipeline_options.PortableOptions)
    if (not portable_options.environment_type and
        not portable_options.output_executable_path):
      portable_options.environment_type = 'LOOPBACK'
    return super().default_environment(options)

  def default_job_server(self, options):
    debug_options = options.view_as(pipeline_options.DebugOptions)
    get_job_server = lambda: job_server.StopOnExitJobServer(
        PrismJobServer(options))
    if debug_options.lookup_experiment("disable_prism_server_singleton"):
      return get_job_server()
    return PrismRunner.shared_handle.acquire(get_job_server)

  def create_job_service_handle(self, job_service, options):
    return portable_runner.JobServiceHandle(
        job_service, options, retain_unknown_options=True)


def _md5sum(filename, block_size=8192) -> str:
  md5 = hashlib.md5()
  with open(filename, 'rb') as f:
    while True:
      data = f.read(block_size)
      if not data:
        break
      md5.update(data)
  return md5.hexdigest()


def _rename_if_different(src, dst):
  assert (os.path.isfile(src))

  if os.path.isfile(dst):
    if _md5sum(src) != _md5sum(dst):
      # Remove existing binary to prevent exception on Windows during
      # os.rename.
      # See: https://docs.python.org/3/library/os.html#os.rename
      os.remove(dst)
      os.rename(src, dst)
    else:
      _LOGGER.info(
          'Found %s and %s with the same md5. Skipping overwrite.' % (src, dst))
      os.remove(src)
  else:
    os.rename(src, dst)


class PrismRunnerLogFilter(logging.Filter):
  COMMON_FIELDS = set(["level", "source", "msg", "time"])

  def filter(self, record):
    if record.funcName == 'log_stdout':
      try:
        message = record.getMessage()
        json_record = json.loads(message)
        level_str = json_record["level"]
        # Example level with offset: 'ERROR+2'
        if "+" in level_str or "-" in level_str:
          match = re.match(r"([A-Z]+)([+-]\d+)", level_str)
          if match:
            base, offset = match.groups()
            base_level = getattr(logging, base, logging.INFO)
            record.levelno = base_level + int(offset)
          else:
            record.levelno = getattr(logging, level_str, logging.INFO)
        else:
          record.levelno = getattr(logging, level_str, logging.INFO)
        record.levelname = logging.getLevelName(record.levelno)
        if "source" in json_record:
          record.funcName = json_record["source"]["function"]
          record.pathname = json_record["source"]["file"]
          record.filename = os.path.basename(record.pathname)
          record.lineno = json_record["source"]["line"]
        record.created = datetime.datetime.fromisoformat(
            json_record["time"]).timestamp()
        extras = {
            k: v
            for k, v in json_record.items()
            if k not in PrismRunnerLogFilter.COMMON_FIELDS
        }

        if json_record["msg"] == "log from SDK worker":
          # TODO: Use location and time inside the nested message to set record
          record.name = "SdkWorker" + "@" + json_record["worker"]["ID"]
          record.msg = json_record["sdk"]["msg"]
        else:
          record.name = "PrismRunner"
          record.msg = (
              f"{json_record['msg']} "
              f"({', '.join(f'{k}={v!r}' for k, v in extras.items())})")
      except (json.JSONDecodeError,
              KeyError,
              ValueError,
              TypeError,
              AttributeError):
        # The log parsing/filtering is best-effort.
        pass

    return True  # Always return True to allow the record to pass.


class PrismJobServer(job_server.SubprocessJobServer):
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

    self._log_level = prism_options.prism_log_level
    self._log_kind = prism_options.prism_log_kind

    # override console to json with log filter enabled
    if self._log_kind == "console":
      self._log_kind = "json"
      self._log_filter = PrismRunnerLogFilter()

  # the method is only kept for testing and backward compatibility
  @classmethod
  def local_bin(
      cls, url: str, bin_cache: str = '', ignore_cache: bool = False) -> str:
    url, ignore_cache = cls._download_to_local_path(url,
                                                    bin_cache,
                                                    ignore_cache)
    return cls._prepare_executable(url, bin_cache, ignore_cache)

  # the method is only kept for testing and backward compatibility
  def path_to_binary(self) -> str:
    return self._resolve_source_path()

  # the method is only kept for testing and backward compatibility
  def construct_download_url(self, root_tag: str, sys: str, mach: str) -> str:
    return self._construct_download_url(self._version, root_tag, sys, mach)

  @staticmethod
  def _prepare_executable(
      url: str, bin_cache: str, ignore_cache: bool = True) -> str:
    """
    Given a path to a local artifact (zip or binary), makes it an
    executable binary file.

    Returns the path to the final, executable binary.
    """
    assert (os.path.isfile(url))

    if zipfile.is_zipfile(url):
      target = os.path.splitext(os.path.basename(url))[0]
      target_url = os.path.join(bin_cache, target)
      if not ignore_cache and os.path.exists(target_url):
        _LOGGER.info(
            'Using cached prism binary from %s for %s' % (target_url, url))
      else:
        # Only unzip the zip file if the url is a zip file and ignore_cache is
        # True (cache disabled)
        _LOGGER.info("Unzipping prism from %s to %s" % (url, target_url))
        z = zipfile.ZipFile(url)

        bin_cache_tmp = os.path.join(bin_cache, 'tmp')
        if not os.path.exists(bin_cache_tmp):
          os.makedirs(bin_cache_tmp)
        target_tmp_url = z.extract(target, path=bin_cache_tmp)

        _rename_if_different(target_tmp_url, target_url)
    else:
      target_url = url

    _LOGGER.info("Prism binary path resolved to: %s", target_url)
    # Make sure the binary is executable.
    try:
      st = os.stat(target_url)
      os.chmod(target_url, st.st_mode | stat.S_IEXEC)
    except PermissionError:
      _LOGGER.warning(
          'Could not change permissions of prism binary; invoking may fail if '
          + 'current process does not have exec permissions on binary.')
    return target_url

  @staticmethod
  def _download_to_local_path(
      url: str,
      bin_cache: str = '',
      ignore_cache: bool = False) -> tuple[str, bool]:
    """
    Ensures the artifact is on local disk, downloading it if necessary.
    Returns the path to the local (potentially cached) artifact.
    """
    # ignore_cache sets whether we should always be downloading and unzipping
    # the file or not, to avoid staleness issues.
    if bin_cache == '':
      bin_cache = PrismJobServer.BIN_CACHE
    if os.path.exists(url):
      _LOGGER.info('Using local prism binary/zip from %s' % url)
      cached_file = url
    else:
      cached_file = os.path.join(bin_cache, os.path.basename(url))
      if os.path.exists(cached_file) and not ignore_cache:
        _LOGGER.info(
            'Using cached prism binary/zip from %s for %s' % (cached_file, url))
      else:
        _LOGGER.info('Downloading prism from %s' % url)
        if not os.path.exists(bin_cache):
          os.makedirs(bin_cache)
        try:
          try:
            url_read = FileSystems.open(url)
          except ValueError:
            url_read = urlopen(url)
          with open(cached_file + '.tmp', 'wb') as zip_write:
            shutil.copyfileobj(url_read, zip_write, length=1 << 20)

          _rename_if_different(cached_file + '.tmp', cached_file)
        except URLError as e:
          raise RuntimeError(
              'Unable to fetch remote prism binary at %s: %s' % (url, e))
        # If we download a new prism, then we should always use it but not
        # the cached one.
        ignore_cache = True
    return cached_file, ignore_cache

  @staticmethod
  def _construct_download_url(
      version: str, root_tag: str, sys: str, mach: str) -> str:
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
          'Machine architecture "%s" unsupported for constructing a Prism '
          'release binary URL.' % (opsys))

    # Some special handling is needed when creating url for release candidates.
    # For example, v2.66.0rc2 should have the following url
    # https://github.com/apache/beam/releases/download/v2.66.0-RC2/apache_beam-v2.66.0-prism-xxx-yyy.zip
    if 'rc' in version:
      version = version.split('rc')[0]

    if 'rc' in root_tag:
      root_tag = '-RC'.join(root_tag.split('rc'))

    return (
        GITHUB_DOWNLOAD_PREFIX +
        f"{root_tag}/apache_beam-{version}-prism-{opsys}-{arch}.zip")

  @staticmethod
  def _resolve_from_location_override(path, version) -> str:
    """Handles the case where --prism_location is explicitly set."""
    # The path is overridden, check various cases.
    if os.path.exists(path):
      # The path is local and exists, use directly.
      return path

    try:
      if FileSystems.exists(path):
        # The path is in one of the supported filesystems.
        return path
    except ValueError:
      # If there is a value error raised by Filesystems, try to resolve
      # the path with the following steps.
      pass

    # Check if the path is a URL.
    url = urllib.parse.urlparse(path)
    if not url.scheme:
      raise ValueError(
          'Unable to parse binary URL "%s". If using a full URL, make '
          'sure the scheme is specified. If using a local file xpath, '
          'make sure the file exists; you may have to first build prism '
          'using `go build `.' % (path))

    # We have a URL, see if we need to construct a valid file name.
    if path.startswith(GITHUB_DOWNLOAD_PREFIX):
      # If this URL starts with the download prefix, let it through.
      return path
    # The only other valid option is a github release page.
    if not path.startswith(GITHUB_TAG_PREFIX):
      raise ValueError(
          'Provided --prism_location URL is not an Apache Beam Github '
          'Release page URL or download URL: %s' % (path))
    # Get the root tag for this URL
    root_tag = os.path.basename(os.path.normpath(path))
    return PrismJobServer._construct_download_url(
        version, root_tag, platform.system(), platform.machine())

  @staticmethod
  def _install_from_source(version):
    """Builds and installs Prism from a Go source package.
    It first tries the local module, then falls back to @latest.
    """
    # This is a development version! Assume Go is installed.
    # Set the install directory to the cache location.
    envdict = {**os.environ, "GOBIN": PrismJobServer.BIN_CACHE}
    PRISMPKG = "github.com/apache/beam/sdks/v2/go/cmd/prism"

    _LOGGER.info(
        'Installing prism from local source into "%s".',
        PrismJobServer.BIN_CACHE)
    process = subprocess.run(["go", "install", PRISMPKG],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             env=envdict,
                             check=False)
    if process.returncode == 0:
      # Successfully installed
      return '%s/prism' % (PrismJobServer.BIN_CACHE)

    # We failed to build for some reason.
    output = process.stdout.decode("utf-8")
    if ("not in a module" not in output) and ("no required module provides"
                                              not in output):
      # This branch handles two classes of failures:
      # 1. Go isn't installed, so it needs to be installed by the Beam SDK
      #   developer.
      # 2. Go is installed, and they are building in a local version of Prism,
      #    but there was a compile error that the developer should address.
      # Either way, the @latest fallback either would fail, or hide the error,
      # so fail now.
      _LOGGER.info(output)
      raise ValueError(
          'Unable to install a local of Prism: "%s";\n'
          'Likely Go is not installed, or a local change to Prism did not '
          'compile.\nPlease install Go (see https://go.dev/doc/install) to '
          'enable automatic local builds.\n'
          'Alternatively provide a binary with the --prism_location flag.'
          '\nCaptured output:\n %s' % (version, output))

    # Go is installed and claims we're not in a Go module that has access to
    # the Prism package.

    # Fallback to using the @latest version of prism, which works everywhere.
    _LOGGER.info(
        'Installing prism from "%s@latest" into "%s".',
        PRISMPKG,
        PrismJobServer.BIN_CACHE)
    process = subprocess.run(["go", "install", PRISMPKG + "@latest"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             env=envdict,
                             check=False)

    if process.returncode == 0:
      return '%s/prism' % (PrismJobServer.BIN_CACHE)

    output = process.stdout.decode("utf-8")
    raise ValueError(
        'We were unable to execute the subprocess "%s" to automatically '
        'build prism.\nAlternatively provide an alternate binary with the '
        '--prism_location flag.'
        '\nCaptured output:\n %s' % (process.args, output))

  def _resolve_source_path(self) -> str:
    """Resolves and returns the source for the Prism binary.

    The resolution follows this order:

    1. A user-provided location (local path, GCS, or URL).
    2. A pre-built binary from GitHub for a release version.
    3. Build from local Go source for a development version.
    """
    if self._path:
      return self._resolve_from_location_override(self._path, self._version)

    if '.dev' not in self._version:
      # Not a development version, so construct the production download URL
      return self._construct_download_url(
          self._version, self._version, platform.system(), platform.machine())

    return self._install_from_source(self._version)

  def _get_executable_path(self) -> str:
    """Orchestrates the process of getting a ready-to-use Prism binary."""
    source = self._resolve_source_path()
    if source == "%s/prism" % (self.BIN_CACHE):
      # source is from go installation, so it is already a local binary
      return self._prepare_executable(source, self.BIN_CACHE, True)

    # Always re-download/extract if a custom path was provided to avoid
    # staleness
    ignore_cache = self._path is not None

    local_path, ignore_cache = self._download_to_local_path(source,
                                                            self.BIN_CACHE,
                                                            ignore_cache)

    return self._prepare_executable(local_path, self.BIN_CACHE, ignore_cache)

  def subprocess_cmd_and_endpoint(
      self) -> typing.Tuple[typing.List[typing.Any], str]:
    bin_path = self._get_executable_path()
    job_port, = subprocess_server.pick_port(self._job_port)
    subprocess_cmd = [bin_path] + self.prism_arguments(job_port)
    return (subprocess_cmd, f"localhost:{job_port}")

  def prism_arguments(self, job_port) -> typing.List[typing.Any]:
    return [
        '--job_port',
        job_port,
        '--log_level',
        self._log_level,
        '--log_kind',
        self._log_kind,
        # Go does not support "-flag x" format for boolean flags.
        '--serve_http=false',
    ]
