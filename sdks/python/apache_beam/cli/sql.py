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

"""Launches the Apache Beam SQL shell (`BeamSqlLine`) for `beam sql`.

`scripts/beam-sql.sh` builds a self-contained shaded uber-JAR with Maven
Wrapper, caches it in `~/.beam/cache/`, and starts `BeamSqlLine`. This module
does the same in Python with identical cache paths and filenames, so cached
JARs and `~/.beam/maven-wrapper/` are shared between `beam-sql.sh` and
`beam sql`, while checking for a real Java 11+ runtime (catching macOS's
`/usr/bin/java` stub and older JDKs) without needing `curl`, `sed`, or GNU
`sort -V`.
"""

import contextlib
import errno
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

_EXIT_FAILED = 1
_EXIT_USAGE = 2
_EXIT_MISSING_DEPENDENCY = 4


class SqlError(Exception):
  """An error in `beam sql`, reported to the user with `exit_code`."""
  def __init__(self, message, exit_code):
    super().__init__(message)
    self.exit_code = exit_code


_MAIN_CLASS = 'org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine'
_WRAPPER_MAIN_CLASS = 'org.apache.maven.wrapper.MavenWrapperMain'

# Supported runners and the extra Maven dependency each needs, matching
# `scripts/beam-sql.sh`. DirectRunner is already a transitive dependency of
# `beam-sdks-java-extensions-sql-jdbc`.
_RUNNER_ARTIFACTS = {
    'direct': None,
    'dataflow': 'beam-runners-google-cloud-dataflow-java',
}

_VALID_ARTIFACT_PART = re.compile(r'^[a-z0-9][a-z0-9._-]*$')
_ANSI_SEQ_RE = re.compile(
    r'\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b[@-_]')
_CTRL_CHARS_RE = re.compile(r'[\x00-\x1f\x7f-\x9f]')

_SQL_LOCKS_GUARD = threading.Lock()
_SQL_INPROC_LOCKS: dict[str, threading.RLock] = {}
_SQL_LOCK_DEPTH: dict[tuple[str, int], int] = {}


def _sanitize_log_token(text: str) -> str:
  return _CTRL_CHARS_RE.sub('', _ANSI_SEQ_RE.sub('', str(text)))


def _is_valid_jar(jar_path: str) -> bool:
  return (
      os.path.isfile(jar_path) and not os.path.islink(jar_path) and
      zipfile.is_zipfile(jar_path))


@contextlib.contextmanager
def _file_lock(lock_path: str):
  abs_key = os.path.abspath(str(lock_path))
  lock_dir = os.path.dirname(abs_key) or '.'
  with _SQL_LOCKS_GUARD:
    inproc_lk = _SQL_INPROC_LOCKS.setdefault(abs_key, threading.RLock())
  with inproc_lk:
    depth_key = (abs_key, threading.get_ident())
    depth = _SQL_LOCK_DEPTH.get(depth_key, 0)
    if depth > 0:
      _SQL_LOCK_DEPTH[depth_key] = depth + 1
      try:
        yield
      finally:
        _SQL_LOCK_DEPTH[depth_key] = depth
      return

    if any(os.path.islink(p) for p in (lock_dir, lock_path, abs_key)):
      raise SqlError(f'refusing symlinked lock path {lock_path!r}', _EXIT_USAGE)
    os.makedirs(lock_dir, mode=0o700, exist_ok=True)
    if any(os.path.islink(p) for p in (lock_dir, lock_path, abs_key)):
      raise SqlError(f'refusing symlinked lock path {lock_path!r}', _EXIT_USAGE)
    open_flags = os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
    try:
      fd = os.open(abs_key, open_flags, 0o600)
    except OSError as exc:
      if exc.errno == getattr(errno, 'ELOOP', -1):
        raise SqlError(
            f'refusing symlinked lock path {lock_path!r}',
            _EXIT_USAGE) from None
      raise
    if not stat.S_ISREG(os.fstat(fd).st_mode):
      os.close(fd)
      raise SqlError(
          f'lock path {lock_path!r} must be a regular file', _EXIT_USAGE)

    try:
      if os.name == 'nt':
        import msvcrt
        os.lseek(fd, 0, os.SEEK_SET)
        with contextlib.suppress(OSError):
          if os.fstat(fd).st_size == 0:
            os.write(fd, b'\0')
            os.lseek(fd, 0, os.SEEK_SET)
        while True:
          try:
            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
            break
          except OSError:
            time.sleep(0.05)
      else:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_EX)
      _SQL_LOCK_DEPTH[depth_key] = 1
      yield
    finally:
      _SQL_LOCK_DEPTH.pop(depth_key, None)
      with contextlib.suppress(OSError):
        if os.name == 'nt':
          import msvcrt
          os.lseek(fd, 0, os.SEEK_SET)
          msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
          import fcntl
          fcntl.flock(fd, fcntl.LOCK_UN)
      os.close(fd)


def _installed_beam_version() -> str:
  """Reads the installed `apache-beam` version without importing the SDK."""
  version_file = Path(__file__).resolve().parents[1] / 'version.py'
  if version_file.is_file():
    match = re.search(
        r"__version__\s*=\s*['\"]([^'\"]+)['\"]",
        version_file.read_text(encoding='utf-8'))
    if match:
      return match.group(1)
  import importlib.metadata
  try:
    return importlib.metadata.version('apache-beam')
  except importlib.metadata.PackageNotFoundError:
    from apache_beam import version as beam_version
    return beam_version.__version__


def run(
    version=None,
    runner='direct',
    ios=(),
    list_versions=False,
    list_ios=False,
    list_runners=False,
    debug=False,
    sqlline_args=(),
    beam_home=None):
  """Runs `beam sql` and returns the process exit code."""
  if list_versions:
    return _list_versions()
  if list_ios:
    return _list_ios()

  resolved_version = _resolve_version(version)
  if list_runners:
    return _list_runners(resolved_version)

  runner_key = _normalize_runner(runner)
  clean_ios = _normalize_ios(ios)
  java_bin, java_home = _find_java()

  home = beam_home or os.path.expanduser(os.environ.get('BEAM_HOME', '~/.beam'))
  if os.path.islink(home):
    raise SqlError(
        f'refusing symlinked BEAM_HOME directory {home!r}', _EXIT_USAGE)
  cache_dir = os.path.join(home, 'cache')
  os.makedirs(cache_dir, mode=0o700, exist_ok=True)
  jar_path = os.path.join(
      cache_dir,
      f"beam-{resolved_version}_runner-{runner_key}_ios-{'-'.join(clean_ios)}.jar"
  )
  if os.path.islink(cache_dir) or os.path.islink(jar_path):
    raise SqlError(
        f'refusing symlinked Beam SQL cache path {jar_path!r}', _EXIT_USAGE)

  with _file_lock(f'{jar_path}.lock'):
    cached_ready = _is_valid_jar(jar_path)

  if cached_ready:
    print(
        f'Using cached Beam SQL JAR: {_sanitize_log_token(jar_path)}',
        file=sys.stderr)
  else:
    wrapper_jar = _ensure_maven_wrapper(home)
    _build_shaded_jar(
        wrapper_jar,
        java_home=java_home,
        version=resolved_version,
        runner_key=runner_key,
        ios=clean_ios,
        jar_path=jar_path,
        debug=debug,
        java_bin=java_bin)

  cmd = [java_bin, '-cp', jar_path, _MAIN_CLASS, *sqlline_args]
  if debug:
    print('+ ' + ' '.join(_sanitize_log_token(c) for c in cmd), file=sys.stderr)
  with subprocess.Popen(cmd) as proc:
    while True:
      try:
        rc = proc.wait()
        return 128 - rc if rc < 0 else rc
      except KeyboardInterrupt:
        pass


def _resolve_version(version):
  """Returns the Maven release version to use for Beam SQL artifacts."""
  if version:
    if '..' in version or not _VALID_ARTIFACT_PART.match(version.lower()):
      raise SqlError(f'invalid Beam version {version!r}', _EXIT_USAGE)
    return re.sub(r'-snapshot$', '-SNAPSHOT', version, flags=re.IGNORECASE)
  installed = _installed_beam_version()
  if ('.dev' not in installed and
      not re.search(r'rc\d+$', installed, re.IGNORECASE) and
      '..' not in installed and _VALID_ARTIFACT_PART.match(installed.lower())):
    return re.sub(r'-snapshot$', '-SNAPSHOT', installed, flags=re.IGNORECASE)
  # A development checkout (`2.XX.0.dev`) has no published Maven JARs; fall
  # back to the latest release on Maven Central, matching `beam-sql.sh`.
  versions = _fetch_maven_versions()
  if not versions:
    raise SqlError(
        f'installed Apache Beam version is {installed!r} (unreleased) and '
        'Maven Central could not be reached; pass --version <release>',
        _EXIT_FAILED)
  latest = versions[0]
  print(
      f'Installed Beam is {_sanitize_log_token(installed)}; using latest '
      f'released Beam SQL {_sanitize_log_token(latest)} '
      '(override with --version).',
      file=sys.stderr)
  return latest


def _normalize_runner(runner):
  name = (runner or 'direct').strip().lower()
  if name.endswith('runner') and name != 'runner':
    name = name[:-len('runner')]
  if name not in _RUNNER_ARTIFACTS:
    raise SqlError(
        f"unsupported runner {runner!r} for beam sql; "
        f"supported runners: {', '.join(_RUNNER_ARTIFACTS)}",
        _EXIT_USAGE)
  return name


def _normalize_ios(ios):
  seen = set()
  for raw in ios:
    name = raw.strip().lower()
    if not name or '..' in name or not _VALID_ARTIFACT_PART.match(name):
      raise SqlError(
          f'invalid --io connector {raw!r}; run `beam sql --list-ios` to see '
          'available connectors',
          _EXIT_USAGE)
    seen.add(name)
  return sorted(seen)


_JAVA_VERSION_RE = re.compile(r'version "(\d+)(?:\.(\d+))?')
_MIN_JAVA_MAJOR = 11
_WARN_UNSUPPORTED_JAVA_MAJOR = 26


def _find_java():
  """Locates `java`, verifies Java 11+, and returns `(java_bin, java_home)`."""
  java_exe = 'java.exe' if os.name == 'nt' else 'java'
  env_home = os.environ.get('JAVA_HOME', '').strip()
  if env_home:
    java_bin = os.path.join(env_home, 'bin', java_exe)
    if not os.path.isfile(java_bin) and os.name == 'nt':
      fallback = os.path.join(env_home, 'bin', 'java')
      if os.path.isfile(fallback):
        java_bin = fallback
    if not os.path.isfile(java_bin):
      raise SqlError(
          f'JAVA_HOME={env_home!r} does not contain bin/{java_exe}; '
          'Beam SQL requires Java 11 or newer',
          _EXIT_MISSING_DEPENDENCY)
  else:
    java_bin = shutil.which('java')
    if not java_bin:
      raise SqlError(
          "'java' command not found; Beam SQL requires Java 11 or newer. "
          'Install OpenJDK 11+ or set JAVA_HOME.',
          _EXIT_MISSING_DEPENDENCY)

  try:
    proc = subprocess.run([java_bin, '-version'],
                          capture_output=True,
                          text=True,
                          check=False)
  except OSError as e:
    raise SqlError(
        f'could not execute {java_bin!r}: {e}\n'
        '  Beam SQL requires Java 11 or newer; install OpenJDK 11+ or set '
        'JAVA_HOME',
        _EXIT_MISSING_DEPENDENCY) from None

  match = _JAVA_VERSION_RE.search(f'{proc.stderr}\n{proc.stdout}')
  if proc.returncode != 0 or not match:
    raise SqlError(
        'no working Java runtime found; Beam SQL requires Java 11 or newer. '
        'Install OpenJDK 11+ or set JAVA_HOME.',
        _EXIT_MISSING_DEPENDENCY)

  first, second = int(match.group(1)), int(match.group(2) or 0)
  major = second if first == 1 else first
  if major < _MIN_JAVA_MAJOR:
    raise SqlError(
        f'Beam SQL requires Java {_MIN_JAVA_MAJOR} or newer '
        f'(found Java {major} at {java_bin}). '
        'Install OpenJDK 11+ or set JAVA_HOME.',
        _EXIT_MISSING_DEPENDENCY)
  if major >= _WARN_UNSUPPORTED_JAVA_MAJOR:
    print(
        f'warning: Java {major} ({java_bin}) may be newer than Beam SQL '
        'supports; if queries fail with a ByteBuddy error, set JAVA_HOME to '
        'Java 11, 17, or 21',
        file=sys.stderr)

  java_home = env_home or os.path.dirname(
      os.path.dirname(os.path.realpath(java_bin)))
  return java_bin, java_home


# Same Maven Wrapper, Iceberg, GCS connector, and repository constants as
# `scripts/beam-sql.sh` on upstream master (#40958, #41090).
_MAVEN_WRAPPER_VERSION = '3.2.0'
_MAVEN_VERSION = '3.9.6'
_MAVEN_SHADE_PLUGIN_VERSION = '3.5.1'
_DEFAULT_ICEBERG_VERSION = '1.12.0'
_DEFAULT_BIGDATAOSS_GCS_CONNECTOR_VERSION = '4.0.4'
_APACHE_SNAPSHOT_REPOSITORY_URL = (
    'https://repository.apache.org/content/repositories/snapshots/')
_MAVEN_WRAPPER_JAR_URL = (
    'https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/'
    f'maven-wrapper/{_MAVEN_WRAPPER_VERSION}/'
    f'maven-wrapper-{_MAVEN_WRAPPER_VERSION}.jar')
_MAVEN_DISTRIBUTION_URL = (
    'https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/'
    f'{_MAVEN_VERSION}/apache-maven-{_MAVEN_VERSION}-bin.zip')


def _replace_with_nt_retry(
    tmp_path: str, dest_path: str, *, accept_existing_valid_jar: bool = False):
  for attempt in range(6):
    try:
      os.replace(tmp_path, dest_path)
      return
    except PermissionError:
      if os.name != 'nt':
        raise
      if attempt < 5:
        time.sleep(0.01)
        continue
      if accept_existing_valid_jar and _is_valid_jar(dest_path):
        return
      raise


def _ensure_maven_wrapper(beam_home):
  """Returns cached `maven-wrapper.jar` path, downloading it once safely."""
  wrapper_dir = os.path.join(beam_home, 'maven-wrapper')
  if os.path.islink(wrapper_dir):
    raise SqlError(
        f'refusing symlinked Maven Wrapper directory {wrapper_dir!r}',
        _EXIT_USAGE)
  wrapper_jar = os.path.join(
      wrapper_dir, '.mvn', 'wrapper', 'maven-wrapper.jar')
  wrapper_props = os.path.join(
      wrapper_dir, '.mvn', 'wrapper', 'maven-wrapper.properties')
  with _file_lock(os.path.join(beam_home, '.maven-wrapper.lock')):
    if (_is_valid_jar(wrapper_jar) and os.path.isfile(wrapper_props) and
        not os.path.islink(wrapper_props)):
      return wrapper_jar

    print(
        f'Downloading Maven Wrapper into {_sanitize_log_token(wrapper_dir)}...',
        file=sys.stderr)
    props_dir = os.path.dirname(wrapper_jar)
    os.makedirs(props_dir, mode=0o700, exist_ok=True)
    fd, tmp_p = tempfile.mkstemp(prefix='.tmp_props_', dir=props_dir)
    try:
      with os.fdopen(fd, 'w', encoding='utf-8') as f:
        f.write(f'distributionUrl={_MAVEN_DISTRIBUTION_URL}\n')
      _replace_with_nt_retry(tmp_p, wrapper_props)
    finally:
      with contextlib.suppress(OSError):
        if os.path.exists(tmp_p):
          os.remove(tmp_p)
    _download_https(_MAVEN_WRAPPER_JAR_URL, wrapper_jar)
    if not _is_valid_jar(wrapper_jar):
      with contextlib.suppress(OSError):
        os.remove(wrapper_jar)
      raise SqlError(
          f'downloaded Maven Wrapper JAR at {wrapper_jar} is corrupt',
          _EXIT_FAILED)
    return wrapper_jar


class _HttpsOnlyRedirectHandler(urllib.request.HTTPRedirectHandler):
  """Refuses any redirect from `https://` to a non-`https://` scheme."""
  def redirect_request(self, req, fp, code, msg, headers, newurl):
    if not str(newurl).lower().startswith('https://'):
      raise urllib.error.URLError(
          f'refusing non-HTTPS redirect target {newurl!r}')
    return super().redirect_request(req, fp, code, msg, headers, newurl)


def _https_urlopen(url: str, timeout: int):
  if not url.startswith('https://'):
    raise ValueError(f'expected https:// URL, got {url!r}')
  opener = urllib.request.build_opener(
      urllib.request.HTTPSHandler(), _HttpsOnlyRedirectHandler())
  return opener.open(url, timeout=timeout)


def _download_https(url, dest_path, max_bytes: int = 32 * 1024 * 1024):
  """Downloads an `https://` URL atomically to `dest_path`."""
  if not url.startswith('https://'):
    raise ValueError(f'expected https:// URL, got {url!r}')
  dest_dir = os.path.dirname(os.path.abspath(dest_path)) or '.'
  os.makedirs(dest_dir, mode=0o700, exist_ok=True)
  fd, tmp_path = tempfile.mkstemp(
      prefix='.tmp_dl_', suffix='.part', dir=dest_dir)
  fd_open = True
  try:
    with _https_urlopen(url, timeout=30) as resp:
      data = resp.read(max_bytes + 1)
    if len(data) > max_bytes:
      raise SqlError(
          f'download from {url} exceeded maximum size ({max_bytes:,} bytes)',
          _EXIT_FAILED)
    f_obj = os.fdopen(fd, 'wb')
    fd_open = False
    with f_obj as f:
      f.write(data)
    _replace_with_nt_retry(
        tmp_path,
        dest_path,
        accept_existing_valid_jar=str(dest_path).lower().endswith('.jar'))
  except (urllib.error.URLError, OSError) as e:
    raise SqlError(f'failed to download {url}: {e}', _EXIT_FAILED) from None
  finally:
    if fd_open:
      with contextlib.suppress(OSError):
        os.close(fd)
    with contextlib.suppress(OSError):
      if os.path.exists(tmp_path):
        os.remove(tmp_path)


def _validated_dep_version(env_var: str, default: str) -> str:
  val = (os.environ.get(env_var) or default).strip()
  if '..' in val or not _VALID_ARTIFACT_PART.match(val.lower()):
    raise SqlError(f'invalid {env_var}={val!r}', _EXIT_USAGE)
  return val


def _dep_xml(group_id: str, artifact_id: str, version_expr: str) -> str:
  return (
      '    <dependency>'
      f'<groupId>{xml_escape(group_id)}</groupId>'
      f'<artifactId>{xml_escape(artifact_id)}</artifactId>'
      f'<version>{version_expr}</version></dependency>')


def _render_pom(version, runner_key, ios):
  """Generates the `pom.xml` used to shade `BeamSqlLine` and selected IOs."""
  iceberg_ver = xml_escape(
      _validated_dep_version('ICEBERG_VERSION', _DEFAULT_ICEBERG_VERSION))
  gcs_ver = xml_escape(
      _validated_dep_version(
          'BIGDATAOSS_GCS_CONNECTOR_VERSION',
          _DEFAULT_BIGDATAOSS_GCS_CONNECTOR_VERSION))

  deps = []
  # IO dependencies precede `beam-sdks-java-extensions-sql-jdbc` so connector
  # classes take precedence during shading (matching `scripts/beam-sql.sh`).
  for io_name in ios:
    deps.append(
        _dep_xml(
            'org.apache.beam',
            f'beam-sdks-java-io-{io_name}',
            '${beam.version}'))
    if io_name == 'delta':
      deps.append(
          _dep_xml(
              'org.apache.beam',
              'beam-sdks-java-extensions-sql-delta',
              '${beam.version}'))
      for gcs_art in ('gcs-connector', 'util-hadoop', 'gcsio', 'util'):
        deps.append(_dep_xml('com.google.cloud.bigdataoss', gcs_art, gcs_ver))
    elif io_name == 'iceberg':
      deps.append(
          _dep_xml(
              'org.apache.beam',
              'beam-sdks-java-extensions-sql-iceberg',
              '${beam.version}'))
      for ice_art in ('iceberg-aws', 'iceberg-aws-bundle', 'iceberg-gcp'):
        deps.append(_dep_xml('org.apache.iceberg', ice_art, iceberg_ver))

  deps.append(
      _dep_xml(
          'org.apache.beam',
          'beam-sdks-java-extensions-sql-jdbc',
          '${beam.version}'))
  if _RUNNER_ARTIFACTS[runner_key]:
    deps.append(
        _dep_xml(
            'org.apache.beam', _RUNNER_ARTIFACTS[runner_key],
            '${beam.version}'))
  repos_block = ''
  if 'snapshot' in str(version).lower():
    repos_block = (
        '\n  <repositories><repository><id>apache.snapshots</id>'
        f'<url>{_APACHE_SNAPSHOT_REPOSITORY_URL}</url>'
        '<releases><enabled>false</enabled></releases>'
        '<snapshots><enabled>true</enabled><updatePolicy>always</updatePolicy>'
        '</snapshots></repository></repositories>')
  shade_mr = 'org.apache.maven.plugins.shade.resource.ManifestResourceTransformer'
  shade_sr = 'org.apache.maven.plugins.shade.resource.ServicesResourceTransformer'
  return (
      '<project xmlns="http://maven.apache.org/POM/4.0.0"\n'
      '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
      '         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 '
      'http://maven.apache.org/xsd/maven-4.0.0.xsd">\n'
      '  <modelVersion>4.0.0</modelVersion>\n'
      '  <groupId>org.apache.beam</groupId>\n'
      '  <artifactId>beam-sql-shell-runner</artifactId>\n'
      '  <version>1.0</version>\n'
      f'  <dependencies>\n{chr(10).join(deps)}\n  </dependencies>{repos_block}\n'
      f'  <properties><beam.version>{xml_escape(version)}</beam.version></properties>\n'
      '  <build><plugins><plugin>\n'
      '    <groupId>org.apache.maven.plugins</groupId>\n'
      '    <artifactId>maven-shade-plugin</artifactId>\n'
      f'    <version>{_MAVEN_SHADE_PLUGIN_VERSION}</version>\n'
      '    <executions><execution><phase>package</phase><goals><goal>shade</goal></goals>\n'
      '      <configuration>\n'
      f'        <transformers><transformer implementation="{shade_mr}">'
      '<manifestEntries><Multi-Release>true</Multi-Release></manifestEntries>'
      f'</transformer><transformer implementation="{shade_sr}"/></transformers>\n'
      '        <filters><filter><artifact>*:*</artifact><excludes>'
      '<exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude>'
      '<exclude>META-INF/*.RSA</exclude></excludes></filter></filters>\n'
      '      </configuration></execution></executions>\n'
      '  </plugin></plugins></build>\n'
      '</project>\n')


def _build_shaded_jar(
    wrapper_jar,
    java_home,
    version,
    runner_key,
    ios,
    jar_path,
    debug,
    *,
    java_bin=None):
  """Builds the Beam SQL shaded uber-JAR and caches it atomically."""
  with _file_lock(f'{jar_path}.lock'):
    if _is_valid_jar(jar_path):
      print(
          f'Using cached Beam SQL JAR: {_sanitize_log_token(jar_path)}',
          file=sys.stderr)
      return

    safe_ios = [_sanitize_log_token(x) for x in ios]
    io_desc = f', ios: {", ".join(safe_ios)}' if safe_ios else ''
    print(
        f'Building Beam SQL v{_sanitize_log_token(version)} JAR '
        f'(runner: {_sanitize_log_token(runner_key)}{io_desc}); '
        'this takes a moment on first run...',
        file=sys.stderr)
    env = os.environ.copy()
    env['JAVA_HOME'] = java_home
    resolved_java = java_bin or os.path.join(
        java_home, 'bin', 'java.exe' if os.name == 'nt' else 'java')
    wrapper_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(wrapper_jar))))

    with tempfile.TemporaryDirectory() as work_dir:
      pom_path = os.path.join(work_dir, 'pom.xml')
      Path(pom_path).write_text(
          _render_pom(version, runner_key, ios), encoding='utf-8')
      cmd = [
          resolved_java,
          f'-Dmaven.multiModuleProjectDirectory={wrapper_dir}',
          '-cp',
          wrapper_jar,
          _WRAPPER_MAIN_CLASS,
          '-f',
          pom_path,
      ]
      if not debug:
        cmd.append('-q')
      cmd.extend(['--batch-mode', 'package'])
      if debug:
        print(
            '+ ' + ' '.join(_sanitize_log_token(c) for c in cmd),
            file=sys.stderr)
        proc = subprocess.run(cmd, env=env, check=False)
        details = ''
      else:
        proc = subprocess.run(
            cmd, env=env, capture_output=True, text=True, check=False)
        details = (proc.stderr or proc.stdout or '').strip()

      built_jar = os.path.join(
          work_dir, 'target', 'beam-sql-shell-runner-1.0.jar')
      if proc.returncode != 0 or not _is_valid_jar(built_jar):
        if 'Could not find artifact org.apache.beam:' in details:
          match_ln = next((
              ln for ln in reversed(details.splitlines())
              if 'Could not find artifact org.apache.beam:' in ln),
                          details.splitlines()[-1])
          last_ln = _sanitize_log_token(match_ln)
          raise SqlError(
              f'Maven could not resolve a Beam {_sanitize_log_token(version)} '
              f'artifact:\n  {last_ln}\n'
              '  check --version (`beam sql --list-versions`) and --io '
              '(`beam sql --list-ios`)',
              _EXIT_USAGE)
        clean_details = '\n'.join(
            _sanitize_log_token(ln) for ln in details.splitlines())
        suffix = f':\n{clean_details}' if clean_details else ''
        raise SqlError(
            f'Maven build for Beam SQL JAR failed{suffix}', _EXIT_FAILED)

      jar_dir = os.path.dirname(os.path.abspath(jar_path)) or '.'
      os.makedirs(jar_dir, mode=0o700, exist_ok=True)
      fd, tmp_jar = tempfile.mkstemp(
          prefix='.tmp_jar_', suffix='.part', dir=jar_dir)
      os.close(fd)
      try:
        shutil.copyfile(built_jar, tmp_jar)
        _replace_with_nt_retry(
            tmp_jar, jar_path, accept_existing_valid_jar=True)
      finally:
        with contextlib.suppress(OSError):
          if os.path.exists(tmp_jar):
            os.remove(tmp_jar)
    print(
        f'Cached Beam SQL JAR at {_sanitize_log_token(jar_path)}',
        file=sys.stderr)


_METADATA_URL = (
    'https://repo1.maven.org/maven2/org/apache/beam/'
    'beam-sdks-java-core/maven-metadata.xml')
_KNOWN_IOS = (
    'amazon-web-services2 amqp azure azure-cosmos cassandra cdap clickhouse '
    'csv debezium delta elasticsearch google-ads google-cloud-platform '
    'hadoop-format hbase hcatalog iceberg influxdb jdbc jms json kafka '
    'kinesis kudu mongodb mqtt neo4j parquet pulsar rabbitmq redis '
    'singlestore snowflake solace solr sparkreceiver splunk synthetic thrift '
    'tika xml').split()
_EXCLUDED_IO_RE = re.compile(r'(tests?|expansion-service|parent|upgrade)')
_EXCLUDED_RUNNER_RE = re.compile(
    r'(tests?|parent|core-construction|core-java|extensions|job-server|'
    r'legacy-worker|windmill|examples|experimental|orchestrator|'
    r'java-fn-execution|java-job-service|gcp-gcemd|gcp-gcsproxy|'
    r'local-java-core|portability-java|prism-java|reference-java)')


def _fetch_text(url, max_bytes: int = 2 * 1024 * 1024):
  if not url.startswith('https://'):
    raise ValueError(f'expected https:// URL, got {url!r}')
  try:
    with _https_urlopen(url, timeout=15) as resp:
      raw = resp.read(max_bytes + 1)
      return None if len(raw) > max_bytes else raw.decode('utf-8')
  except (urllib.error.URLError, OSError, UnicodeDecodeError):
    return None


def _fetch_maven_versions():
  xml_text = _fetch_text(_METADATA_URL)
  if not xml_text or '<!DOCTYPE' in xml_text or '<!ENTITY' in xml_text:
    return []
  try:
    root = ET.fromstring(xml_text)
  except ET.ParseError:
    return []
  versions = []
  for el in root.findall('.//versions/version'):
    if not el.text:
      continue
    raw_v = _sanitize_log_token(el.text).strip()
    if (raw_v and raw_v == el.text.strip() and
        'SNAPSHOT' not in raw_v.upper() and '..' not in raw_v and
        _VALID_ARTIFACT_PART.match(raw_v.lower())):
      versions.append(raw_v)
  return sorted(
      versions,
      key=lambda v: [(0, int(p)) if p.isdigit() else (1, p)
                     for p in re.split(r'[.-]', v)],
      reverse=True)


def _query_maven_artifacts(prefix, excluded_re, version=None):
  version_filter = (
      f'+AND+v:{urllib.parse.quote(str(version), safe="._-")}'
      if version else '')
  url = (
      'https://search.maven.org/solrsearch/select?'
      f'q=g:org.apache.beam+AND+a:{prefix}*{version_filter}&rows=100&wt=json')
  body = _fetch_text(url)
  if not body:
    return []
  try:
    docs = json.loads(body).get('response', {}).get('docs', [])
    results = set()
    for doc in docs:
      art = str(doc.get('a', ''))
      if not art.startswith(prefix):
        continue
      raw_suf = art[len(prefix):]
      clean_suf = _sanitize_log_token(raw_suf)
      if (clean_suf and clean_suf == raw_suf and '..' not in clean_suf and
          _VALID_ARTIFACT_PART.match(clean_suf.lower()) and
          not excluded_re.search(clean_suf)):
        results.add(clean_suf)
    return sorted(results)
  except (ValueError, KeyError, TypeError):
    return []


def _list_versions():
  versions = _fetch_maven_versions()
  if not versions:
    raise SqlError(
        'could not retrieve Beam versions from Maven Central; '
        'check your network connection',
        _EXIT_FAILED)
  print('10 latest Apache Beam releases:')
  for ver in versions[:10]:
    print(f'  {_sanitize_log_token(ver)}')
  return 0


def _list_ios():
  ios = _query_maven_artifacts('beam-sdks-java-io-', _EXCLUDED_IO_RE)
  if not ios:
    print(
        'warning: could not reach Maven Central; showing known IO connectors:',
        file=sys.stderr)
    print(
        textwrap.fill(
            ', '.join(_KNOWN_IOS),
            width=78,
            initial_indent='  ',
            subsequent_indent='  '))
    return _EXIT_FAILED
  print('Available Apache Beam SQL IO connectors:')
  print(
      textwrap.fill(
          ' '.join(_sanitize_log_token(x) for x in ios),
          width=78,
          initial_indent='  ',
          subsequent_indent='  '))
  return 0


def _list_runners(version):
  safe_ver = _sanitize_log_token(version)
  runners = _query_maven_artifacts(
      'beam-runners-', _EXCLUDED_RUNNER_RE, version=version)
  if not runners:
    print(
        f'warning: could not fetch runners for Beam {safe_ver} from Maven '
        'Central; supported by `beam sql`:',
        file=sys.stderr)
    print('  direct    - DirectRunner (runs locally, default)')
    print('  dataflow  - DataflowRunner (runs on Google Cloud Dataflow)')
    return _EXIT_FAILED
  print(f'Available runners in Maven Central (Beam {safe_ver}):')
  for raw in runners:
    safe_raw = _sanitize_log_token(raw)
    if safe_raw == 'direct-java':
      print('  direct    - DirectRunner (supported by beam sql, default)')
    elif safe_raw == 'google-cloud-dataflow-java':
      print('  dataflow  - DataflowRunner (supported by beam sql)')
    else:
      clean = re.sub(r'(-java$|^gcp-|^local-)', '', safe_raw)
      print(f'  {clean:<9} - {safe_raw}')
  print(
      "\nNote: `beam sql --runner` currently supports 'direct' and 'dataflow'.")
  return 0
