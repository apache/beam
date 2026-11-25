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

import contextlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
import xml.etree.ElementTree as ET
import zipfile
from unittest import mock

from apache_beam.cli import sql
from apache_beam.cli.sql import SqlError


def _proc(returncode=0, stdout='', stderr=''):
  return subprocess.CompletedProcess(
      args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def _write_dummy_jar(path: str) -> None:
  os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
  with zipfile.ZipFile(path, 'w') as zf:
    zf.writestr('META-INF/MANIFEST.MF', 'Manifest-Version: 1.0\n')


class SqlShellTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.beam_home = os.path.join(self.tmp, '.beam')

  def test_cache_hit_miss_and_corrupt_jar_rebuild(self):
    cache_dir = os.path.join(self.beam_home, 'cache')
    os.makedirs(cache_dir)
    for runner, ios, expected_name in [
        ('direct', [], 'beam-2.75.0_runner-direct_ios-.jar'),
        ('DataflowRunner', ['parquet', 'CSV', 'csv'],
         'beam-2.75.0_runner-dataflow_ios-csv-parquet.jar'),
    ]:
      jar_path = os.path.join(cache_dir, expected_name)
      _write_dummy_jar(jar_path)
      popen_calls = []

      def fake_popen(cmd, **_kw):
        popen_calls.append(cmd)
        proc = mock.MagicMock()
        proc.__enter__.return_value = proc
        proc.wait.side_effect = [KeyboardInterrupt(), 0]
        return proc

      err_dbg = io.StringIO()
      with (
          mock.patch.dict(os.environ, {}, clear=True),
          mock.patch('shutil.which', return_value='/usr/bin/java'),
          mock.patch('subprocess.run',
                     return_value=_proc(0, stderr='openjdk version "17.0.9"')),
          mock.patch('subprocess.Popen', side_effect=fake_popen),
          contextlib.redirect_stderr(err_dbg),
      ):
        rc = sql.run(
            version='2.75.0',
            runner=runner,
            ios=ios,
            debug=True,
            sqlline_args=['-e', 'SELECT 1'],
            beam_home=self.beam_home)
      self.assertEqual(rc, 0)
      self.assertIn('+ /usr/bin/java', err_dbg.getvalue())
      self.assertEqual(
          popen_calls[-1],
          ['/usr/bin/java', '-cp', jar_path, sql._MAIN_CLASS, '-e', 'SELECT 1'])

    # Cache miss builds shaded JAR via MavenWrapperMain; corrupt JAR rebuilds.
    corrupt_jar = os.path.join(
        cache_dir, 'beam-2.75.0_runner-dataflow_ios-csv.jar')
    with open(corrupt_jar, 'wb') as f:
      f.write(b'corrupt-truncated-not-a-zip')
    pom_captured, wrapper_cmds = [], []

    def fake_build(cmd, **_kw):
      if cmd[-1] == '-version':
        return _proc(0, stderr='openjdk version "21.0.2"')
      if sql._WRAPPER_MAIN_CLASS in cmd:
        wrapper_cmds.append(cmd)
        pom_file = cmd[cmd.index('-f') + 1]
        with open(pom_file, encoding='utf-8') as f:
          pom_captured.append(f.read())
        _write_dummy_jar(
            os.path.join(
                os.path.dirname(pom_file),
                'target',
                'beam-sql-shell-runner-1.0.jar'))
      return _proc(0)

    sigint_proc = mock.MagicMock()
    sigint_proc.__enter__.return_value = sigint_proc
    sigint_proc.wait.return_value = -2

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/opt/jdk21/bin/java'),
        mock.patch.object(sql,
                          '_download_https',
                          side_effect=lambda _u, d, **_k: _write_dummy_jar(d)),
        mock.patch('subprocess.run', side_effect=fake_build),
        mock.patch('subprocess.Popen', return_value=sigint_proc),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      self.assertEqual(
          sql.run(
              version='2.75.0',
              runner='dataflow',
              ios=['csv'],
              beam_home=self.beam_home),
          130)
    self.assertEqual(len(wrapper_cmds), 1)
    self.assertTrue(zipfile.is_zipfile(corrupt_jar))
    ET.fromstring(pom_captured[0])
    self.assertIn('beam-sdks-java-io-csv', pom_captured[0])
    self.assertIn('beam-runners-google-cloud-dataflow-java', pom_captured[0])
    self.assertIn('ServicesResourceTransformer', pom_captured[0])

  def test_java_checks_windows_java_exe_and_version_resolution(self):
    fake_jdk = os.path.join(self.tmp, 'jdk21')
    os.makedirs(os.path.join(fake_jdk, 'bin'))
    with open(os.path.join(fake_jdk, 'bin', 'java.exe'), 'wb') as f:
      f.write(b'MZ')
    with (
        mock.patch.dict(os.environ, {'JAVA_HOME': fake_jdk}, clear=True),
        mock.patch.object(sql.os, 'name', 'nt'),
        mock.patch('subprocess.run',
                   return_value=_proc(0, stderr='openjdk version "26.0.2"')),
        contextlib.redirect_stderr(io.StringIO()) as warn_err,
    ):
      found_bin, found_home = sql._find_java()
    self.assertTrue(found_bin.endswith('java.exe'))
    self.assertEqual(found_home, fake_jdk)
    self.assertIn('warning: Java 26', warn_err.getvalue())

    for which_java, version_proc, msg in [
        (None, _proc(0), 'not found'),
        ('/usr/bin/java', _proc(1, stderr='Unable to locate a Java Runtime.'),
         'no working Java runtime'),
        ('/usr/bin/java', _proc(0, stderr='java version "1.8.0_392"'),
         'found Java 8'),
    ]:
      with (
          mock.patch.dict(os.environ, {}, clear=True),
          mock.patch('shutil.which', return_value=which_java),
          mock.patch('subprocess.run', return_value=version_proc),
      ):
        with self.assertRaisesRegex(SqlError, msg) as ctx:
          sql.run(version='2.75.0', beam_home=self.beam_home)
        self.assertEqual(ctx.exception.exit_code, 4)

    empty_jdk = os.path.join(self.tmp, 'empty_jdk')
    os.makedirs(empty_jdk)
    with mock.patch.dict(os.environ, {'JAVA_HOME': empty_jdk}, clear=True):
      with self.assertRaisesRegex(SqlError, 'does not contain') as c_jh:
        sql._find_java()
      self.assertEqual(c_jh.exception.exit_code, 4)
    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/usr/bin/java'),
        mock.patch('subprocess.run', side_effect=OSError('exec format error')),
    ):
      with self.assertRaisesRegex(SqlError, 'could not execute'):
        sql._find_java()

    for kwargs, pattern in [
        ({'version': '2.75.0', 'runner': 'spark'}, 'unsupported runner'),
        ({'version': '2.75.0', 'ios': ['../bad']}, 'invalid --io'),
        ({'version': '2.75;rm'}, 'invalid Beam version'),
    ]:
      with self.assertRaisesRegex(SqlError, pattern) as ctx2:
        sql.run(beam_home=self.beam_home, **kwargs)
      self.assertEqual(ctx2.exception.exit_code, 2)

    self.assertTrue(bool(sql._installed_beam_version()))
    self.assertEqual(sql._resolve_version('2.79.0-snapshot'), '2.79.0-SNAPSHOT')
    self.assertEqual(sql._resolve_version('2.79.0-Snapshot'), '2.79.0-SNAPSHOT')
    with mock.patch.object(sql,
                           '_installed_beam_version',
                           return_value='2.75.0'):
      self.assertEqual(sql._resolve_version(None), '2.75.0')
    with mock.patch.object(sql,
                           '_installed_beam_version',
                           return_value='2.79.0-snapshot'):
      self.assertEqual(sql._resolve_version(None), '2.79.0-SNAPSHOT')
    metadata = (
        '<metadata><versioning><versions>'
        '<version>2.9.0</version><version>2.75.0</version>'
        '<version>2.76.0-SNAPSHOT</version></versions></versioning></metadata>')
    with (
        mock.patch.object(sql,
                          '_installed_beam_version',
                          return_value='2.76.0.dev'),
        mock.patch.object(sql, '_fetch_text', return_value=metadata),
        contextlib.redirect_stderr(io.StringIO()),
    ):
      self.assertEqual(sql._resolve_version(None), '2.75.0')
    with (
        mock.patch.object(sql,
                          '_installed_beam_version',
                          return_value='2.79.0.dev'),
        mock.patch.object(sql, '_fetch_maven_versions', return_value=[]),
    ):
      with self.assertRaisesRegex(SqlError,
                                  'Maven Central could not be reached'):
        sql._resolve_version(None)

  def test_pom_rendering_and_list_flags_online_and_offline(self):
    pom_xml = sql._render_pom('2.79.0', 'dataflow', ['delta', 'iceberg'])
    root = ET.fromstring(pom_xml)
    ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
    pom_deps = root.findall('./m:dependencies/m:dependency', ns)
    self.assertGreaterEqual(len(pom_deps), 12)
    for dep_el in pom_deps:
      excl_pairs = [(
          ex.findtext('m:groupId', default='', namespaces=ns),
          ex.findtext('m:artifactId', default='', namespaces=ns))
                    for ex in dep_el.findall('./m:exclusions/m:exclusion', ns)]
      self.assertIn(('org.conscrypt', '*'), excl_pairs)
    self.assertIn('beam-sdks-java-extensions-sql-delta', pom_xml)
    self.assertIn('beam-sdks-java-extensions-sql-iceberg', pom_xml)
    self.assertIn('<Multi-Release>true</Multi-Release>', pom_xml)
    self.assertNotIn('<id>apache.snapshots</id>', pom_xml)
    self.assertIn(
        '<id>apache.snapshots</id>',
        sql._render_pom('2.79.0-SNAPSHOT', 'direct', ['delta']))
    self.assertLess(
        pom_xml.index('beam-sdks-java-extensions-sql-delta'),
        pom_xml.index('beam-sdks-java-extensions-sql-jdbc'))
    with mock.patch.dict(os.environ, {'ICEBERG_VERSION': '../bad'},
                         clear=False):
      with self.assertRaisesRegex(SqlError, 'invalid ICEBERG_VERSION'):
        sql._render_pom('2.79.0', 'direct', ['iceberg'])

    solr_ios = json.dumps({
        'response': {
            'docs': [{
                'a': 'beam-sdks-java-io-parquet'
            }, {
                'a': 'beam-sdks-java-io-csv'
            }, {
                'a': 'beam-sdks-java-io-kafka-upgrade'
            }]
        }
    })
    ios_urls = []
    out = io.StringIO()
    with mock.patch.object(
        sql,
        '_fetch_text',
        side_effect=lambda u: (ios_urls.append(u) or solr_ios)), \
         contextlib.redirect_stdout(out):
      self.assertEqual(sql.run(list_ios=True), 0)
    self.assertNotIn('+AND+v:', ios_urls[0])
    self.assertIn('csv parquet', out.getvalue())
    self.assertNotIn('upgrade', out.getvalue())
    with mock.patch.object(sql, '_fetch_text', return_value=None), \
         contextlib.redirect_stdout(io.StringIO()), \
         contextlib.redirect_stderr(io.StringIO()):
      self.assertEqual(sql.run(list_ios=True), 1)

    with mock.patch.object(sql, '_fetch_maven_versions', return_value=['2.79.0']), \
         contextlib.redirect_stdout(io.StringIO()) as out_v:
      self.assertEqual(sql.run(list_versions=True), 0)
      self.assertIn('2.79.0', out_v.getvalue())
    with mock.patch.object(sql, '_fetch_maven_versions', return_value=[]):
      with self.assertRaisesRegex(SqlError, 'could not retrieve Beam versions'):
        sql.run(list_versions=True)

    solr_runners = json.dumps({
        'response': {
            'docs': [{
                'a': 'beam-runners-direct-java'
            }, {
                'a': 'beam-runners-google-cloud-dataflow-java'
            }, {
                'a': 'beam-runners-flink-1.18'
            }]
        }
    })
    runner_urls = []
    with mock.patch.object(
        sql,
        '_fetch_text',
        side_effect=lambda u: (runner_urls.append(u) or solr_runners)), \
         contextlib.redirect_stdout(io.StringIO()) as out_r:
      self.assertEqual(sql.run(version='2.79.0', list_runners=True), 0)
      self.assertIn('DataflowRunner', out_r.getvalue())
    self.assertIn('+AND+v:2.79.0', runner_urls[0])
    with mock.patch.object(sql, '_fetch_text', return_value=None), \
         contextlib.redirect_stdout(io.StringIO()), \
         contextlib.redirect_stderr(io.StringIO()):
      self.assertEqual(sql.run(version='2.79.0', list_runners=True), 1)

  def test_security_locks_concurrency_and_maven_failures(self):
    with self.assertRaises(ValueError):
      sql._download_https(
          'http://example.com/a.jar', os.path.join(self.tmp, 'a.jar'))
    with self.assertRaises(ValueError):
      sql._fetch_text('http://example.com/meta.xml')
    with self.assertRaises(sql.urllib.error.URLError):
      sql._HttpsOnlyRedirectHandler().redirect_request(
          None, None, 302, 'Found', {}, 'http://evil.test/x')

    dest = os.path.join(self.tmp, 'dl', 'ok.jar')
    with mock.patch.object(sql,
                           '_https_urlopen',
                           side_effect=lambda _u, timeout: contextlib.
                           nullcontext(io.BytesIO(b'hi'))):
      sql._download_https(
          'https://repo.maven.apache.org/ok.jar', dest, max_bytes=8)
    with mock.patch.object(sql,
                           '_https_urlopen',
                           side_effect=lambda _u, timeout: contextlib.
                           nullcontext(io.BytesIO(b'x' * 16))):
      with self.assertRaisesRegex(SqlError, 'exceeded maximum size'):
        sql._download_https(
            'https://repo.maven.apache.org/big.jar', dest, max_bytes=4)
    real_mkstemp = tempfile.mkstemp
    opened_fds = []

    def tracking_mkstemp(*args, **kwargs):
      fd, path = real_mkstemp(*args, **kwargs)
      opened_fds.append(fd)
      return fd, path

    fail_dest = os.path.join(self.tmp, 'dl_err', 'fail.jar')
    with (
        mock.patch('tempfile.mkstemp', side_effect=tracking_mkstemp),
        mock.patch.object(
            sql,
            '_https_urlopen',
            side_effect=sql.urllib.error.URLError('connection reset')),
    ):
      with self.assertRaisesRegex(SqlError, 'failed to download') as dl_ctx:
        sql._download_https('https://repo.maven.apache.org/fail.jar', fail_dest)
      self.assertEqual(dl_ctx.exception.exit_code, 1)
    self.assertEqual(len(opened_fds), 1)
    with self.assertRaises(OSError):
      os.fstat(opened_fds[0])
    self.assertEqual(os.listdir(os.path.dirname(fail_dest)), [])
    with mock.patch.object(sql,
                           '_https_urlopen',
                           side_effect=lambda _u, timeout: contextlib.
                           nullcontext(io.BytesIO(b'<!DOCTYPE foo>'))):
      self.assertEqual(sql._fetch_maven_versions(), [])

    if hasattr(os, 'symlink'):
      os.makedirs(self.beam_home, exist_ok=True)
      sym_w = os.path.join(self.beam_home, 'maven-wrapper')
      os.symlink(self.tmp, sym_w)
      with self.assertRaisesRegex(SqlError, 'refusing symlinked'):
        sql._ensure_maven_wrapper(self.beam_home)
      os.unlink(sym_w)
      sym_lock = os.path.join(self.tmp, 'sym.lock')
      os.symlink(dest, sym_lock)
      with self.assertRaisesRegex(SqlError, 'refusing symlinked lock path'):
        with sql._file_lock(sym_lock):
          pass
      os.unlink(sym_lock)

    wrapper_jar = os.path.join(
        self.beam_home, 'maven-wrapper', '.mvn', 'wrapper', 'maven-wrapper.jar')
    _write_dummy_jar(wrapper_jar)
    mvn_missing = (
        '[ERROR] Could not find artifact '
        'org.apache.beam:beam-sdks-java-io-x:2.75.0\n'
        '[ERROR] To see the full stack trace, re-run with -e.\n'
        '[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN')
    for err_line, pattern, code in [
        (mvn_missing, r'Maven could not resolve[\s\S]*beam-sdks-java-io-x', 2),
        (
            '\x1b[31m[ERROR] compiler crash\x1b[0m',
            'Maven build for Beam SQL JAR',
            1),
    ]:
      with (
          mock.patch('subprocess.run', return_value=_proc(1, stderr=err_line)),
          contextlib.redirect_stderr(io.StringIO()),
      ):
        with self.assertRaisesRegex(SqlError, pattern) as c_mv:
          sql._build_shaded_jar(
              wrapper_jar,
              '/opt/jdk',
              '2.75.0',
              'direct', [],
              os.path.join(self.tmp, f'f_{code}.jar'),
              False,
              java_bin='/opt/jdk/bin/java')
        self.assertEqual(c_mv.exception.exit_code, code)
        self.assertNotIn('\x1b', str(c_mv.exception))

    # Re-entrant lock, 4-thread single-build race, and Windows retry/msvcrt
    lock_f = os.path.join(self.tmp, 'reentrant.lock')
    with sql._file_lock(lock_f), sql._file_lock(lock_f):
      self.assertTrue(os.path.isfile(lock_f))
    shared_jar = os.path.join(self.beam_home, 'cache', 'shared.jar')
    builds, guard = [], threading.Lock()

    def fake_mvn(cmd, **_kw):
      with guard:
        builds.append(1)
      pom_p = cmd[cmd.index('-f') + 1]
      _write_dummy_jar(
          os.path.join(
              os.path.dirname(pom_p), 'target',
              'beam-sql-shell-runner-1.0.jar'))
      return _proc(0)

    with mock.patch('subprocess.run', side_effect=fake_mvn), \
         contextlib.redirect_stderr(io.StringIO()):
      ts = [
          threading.Thread(
              target=sql._build_shaded_jar,
              args=(
                  wrapper_jar,
                  '/opt/jdk',
                  '2.75.0',
                  'direct', [],
                  shared_jar,
                  False),
              kwargs={'java_bin': '/opt/jdk/bin/java'}) for _ in range(4)
      ]
      for t in ts:
        t.start()
      for t in ts:
        t.join()
    self.assertEqual(len(builds), 1)
    self.assertTrue(zipfile.is_zipfile(shared_jar))

    fake_msvcrt = mock.MagicMock(LK_LOCK=1, LK_UNLCK=0)
    with mock.patch.object(sql.os, 'name', 'nt'), \
         mock.patch.dict('sys.modules', {'msvcrt': fake_msvcrt}):
      with sql._file_lock(os.path.join(self.tmp, 'nt.lock')):
        pass
    self.assertEqual(fake_msvcrt.locking.call_count, 2)

  def test_find_java_reads_java_home_property_for_launcher_shims(self):
    # macOS `/usr/bin/java` and Windows `javapath\java.exe` are launcher shims
    # whose `realpath` does not point into the JDK; `java.home` from
    # `-XshowSettings:properties -version` must win, falling back to `realpath`
    # only if the property is absent.
    macos_props = (
        'Property settings:\n'
        '    java.home = /Library/Java/JavaVirtualMachines/'
        'temurin-21.jdk/Contents/Home\n'
        'openjdk version "21.0.5" 2024-10-15\n')
    seen_cmds = []

    def fake_java_props(cmd, **_kw):
      seen_cmds.append(cmd)
      return _proc(0, stderr=macos_props)

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/usr/bin/java'),
        mock.patch('os.path.realpath', return_value='/usr/bin/java'),
        mock.patch('subprocess.run', side_effect=fake_java_props),
    ):
      found_bin, found_home = sql._find_java()
    self.assertEqual(
        seen_cmds[0],
        ['/usr/bin/java', '-XshowSettings:properties', '-version'])
    self.assertEqual(found_bin, '/usr/bin/java')
    self.assertEqual(
        found_home,
        '/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home')

    win_shim = r'C:\Program Files\Common Files\Oracle\Java\javapath\java.exe'
    win_jdk = r'C:\Program Files\Eclipse Adoptium\jdk-21.0.5.11-hotspot'
    win_props = (
        f'Property settings:\r\n    java.home = {win_jdk}\r\n'
        'openjdk version "21.0.5"\r\n')
    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value=win_shim),
        mock.patch('subprocess.run', return_value=_proc(0, stderr=win_props)),
    ):
      _, win_home = sql._find_java()
    self.assertEqual(win_home, win_jdk)

    # Fallback when `java.home = ...` is absent from `-version` output.
    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch('shutil.which', return_value='/opt/jdk-17/bin/java'),
        mock.patch('os.path.realpath', return_value='/opt/jdk-17/bin/java'),
        mock.patch('subprocess.run',
                   return_value=_proc(0, stderr='openjdk version "17.0.9"')),
    ):
      _, fallback_home = sql._find_java()
    self.assertEqual(fallback_home, '/opt/jdk-17')

  def test_e2e_csv_filter_aggregate_to_parquet(self):
    in_ci = bool(
        os.environ.get('CI') or os.environ.get('GITHUB_ACTIONS') or
        os.environ.get('JENKINS_HOME'))
    try:
      _, detected_home = sql._find_java()
    except SqlError as exc:
      if in_ci:
        raise
      self.skipTest(str(exc))

    self.assertNotEqual(detected_home, '/usr')

    work_dir = os.path.join(self.tmp, 'e2e_sql')
    os.makedirs(work_dir)
    with open(os.path.join(work_dir, 'orders.csv'), 'w', encoding='utf-8') as f:
      f.write('alice,120\nbob,40\nalice,80\ncarol,200\n')
    os.makedirs(os.path.join(work_dir, 'totals'))
    os.makedirs(os.path.join(work_dir, 'totals_from_file'))

    sql_script = os.path.join(work_dir, 'query.sql')
    with open(sql_script, 'w', encoding='utf-8') as f:
      f.write(
          'CREATE EXTERNAL TABLE orders (customer VARCHAR, amount INTEGER) '
          "TYPE text LOCATION 'orders.csv';\n"
          'CREATE EXTERNAL TABLE totals (customer VARCHAR, total_amount BIGINT) '
          "TYPE parquet LOCATION 'totals_from_file/';\n"
          'INSERT INTO totals SELECT customer, SUM(amount) FROM orders '
          'WHERE amount >= 50 GROUP BY customer;\n')

    from apache_beam.cli import main as cli

    old_cwd = os.getcwd()
    try:
      os.chdir(work_dir)
      with mock.patch.dict(os.environ, {'BEAM_HOME': self.beam_home}):
        rc_e = cli.main([
            'sql',
            '--io',
            'parquet',
            '-e',
            'CREATE EXTERNAL TABLE orders (customer VARCHAR, amount INTEGER) '
            "TYPE text LOCATION 'orders.csv';",
            '-e',
            'CREATE EXTERNAL TABLE totals '
            '(customer VARCHAR, total_amount BIGINT) '
            "TYPE parquet LOCATION 'totals/';",
            '-e',
            'INSERT INTO totals SELECT customer, SUM(amount) FROM orders '
            'WHERE amount >= 50 GROUP BY customer;',
        ])
        self.assertEqual(rc_e, 0)

        cached_err = io.StringIO()
        with contextlib.redirect_stderr(cached_err):
          rc_f = cli.main(['sql', '--io', 'parquet', '-f', 'query.sql'])
        self.assertEqual(rc_f, 0)
        self.assertIn('Using cached Beam SQL JAR:', cached_err.getvalue())
    finally:
      os.chdir(old_cwd)

    expected_rows = [
        {
            'customer': 'alice', 'total_amount': 200
        },
        {
            'customer': 'carol', 'total_amount': 200
        },
    ]
    for out_subdir in ('totals', 'totals_from_file'):
      out_path = os.path.join(work_dir, out_subdir)
      shards = [
          os.path.join(out_path, name) for name in os.listdir(out_path)
          if not name.startswith('.') and
          os.path.isfile(os.path.join(out_path, name))
      ]
      self.assertTrue(shards, f'expected Parquet shard files in {out_path}')
      with open(shards[0], 'rb') as pf:
        self.assertEqual(pf.read(4), b'PAR1')
      try:
        import pyarrow.parquet as pq
      except ImportError:
        pq = None
      if pq is not None:
        rows = sorted(
            pq.read_table(out_path).to_pylist(), key=lambda r: r['customer'])
        self.assertEqual(rows, expected_rows)


if __name__ == '__main__':
  unittest.main()
