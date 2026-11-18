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
import glob
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.cli import main as cli
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import subprocess_server

try:
  import jsonschema
  from apache_beam.yaml import main as yaml_main
except ImportError:
  jsonschema = None
  yaml_main = None

# Paths are formatted with `json.dumps` so Windows backslashes (such as `\U`
# and `\t` in `C:\Users\...\temp`) are escaped as valid YAML strings.
PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: %(input)s}
    - type: Filter
      config: {language: python, keep: "age > 20"}
    - type: WriteToJson
      config: {path: %(output)s}
'''

FAILING_PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: %(input)s}
    - type: MapToFields
      config: {language: python, fields: {x: "age // 0"}}
    - type: WriteToJson
      config: {path: %(output)s}
'''

INVALID_PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: NoSuchTransform
'''


def _run_cli(argv):
  """Runs `beam` with argv; returns (exit code, stdout, stderr)."""
  out, err = io.StringIO(), io.StringIO()
  with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
    code = cli.main(argv)
  return code, out.getvalue(), err.getvalue()


def _read_lines(prefix):
  lines = []
  for path in glob.glob(prefix + '*'):
    with open(path) as f:
      lines.extend(f.read().splitlines())
  return sorted(lines)


class ParserTest(unittest.TestCase):
  def test_usage_errors_exit_with_2(self):
    for argv in (
        [],
        ['run'],
        ['pipe'],
        ['pipe', '--from', 'a.csv'],
        ['pipe', '--to', 'b.csv'],
        ['nosuchcommand', 'x'],
    ):
      with self.subTest(argv=argv):
        with contextlib.redirect_stderr(io.StringIO()):
          with self.assertRaises(SystemExit) as e:
            cli.main(argv)
        self.assertEqual(e.exception.code, cli.EXIT_USAGE)

  def test_missing_file(self):
    code, _, err = _run_cli(['run', 'no/such/pipeline.yaml'])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn("no such file: 'no/such/pipeline.yaml'", err)
    self.assertNotIn('pipeline options go after FILE', err)

  def test_missing_file_hints_at_option_order(self):
    # argparse takes "dataflow" for FILE when an option uses a space.
    code, _, err = _run_cli(['run', '--runner', 'dataflow', 'p.yaml'])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn("no such file: 'dataflow'", err)
    self.assertIn('pipeline options go after FILE', err)


@unittest.skipIf(
    yaml_main is None or jsonschema is None, 'requires the [yaml] extra')
class RunTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.input = os.path.join(self.tmp, 'users.csv')
    with open(self.input, 'w') as f:
      f.write('name,age\nAda,36\nLinus,17\nGrace,45\n')

  def _pipeline_file(self, template, output='users.json', input_path=None):
    path = os.path.join(self.tmp, 'pipeline.yaml')
    with open(path, 'w') as f:
      f.write(
          template % {
              'input': json.dumps(input_path or self.input),
              'output': json.dumps(os.path.join(self.tmp, output)),
          })
    return path

  def test_runs_like_yaml_main(self):
    # The CLI and `python -m apache_beam.yaml.main` write the same output for
    # the same pipeline file and options.
    path = self._pipeline_file(PIPELINE, 'cli_users.json')
    code, out, _ = _run_cli(['run', path, '--runner=DirectRunner'])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertRegex(out, r'Done\. Ran locally on (Prism|the Python direct)')

    reference = self._pipeline_file(PIPELINE, 'reference_users.json')
    yaml_main.run(
        ['--yaml_pipeline_file=' + reference, '--runner=DirectRunner'])
    cli_rows = _read_lines(os.path.join(self.tmp, 'cli_users.json'))
    self.assertEqual(len(cli_rows), 2)
    self.assertEqual(
        cli_rows, _read_lines(os.path.join(self.tmp, 'reference_users.json')))

  def test_backslash_sequences_in_paths(self):
    # Regression test for Windows temp paths (`C:\Users\...\tmp...`), where
    # unescaped `\U` and `\t` inside double-quoted YAML strings fail parsing.
    tricky_input = os.path.join(self.tmp, r'C_\Users\tmp_users.csv')
    os.makedirs(os.path.dirname(tricky_input), exist_ok=True)
    shutil.copyfile(self.input, tricky_input)
    path = self._pipeline_file(
        PIPELINE, 'backslash_users.json', input_path=tricky_input)
    code, _, _ = _run_cli(['run', path, '--runner=DirectRunner'])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertEqual(
        len(_read_lines(os.path.join(self.tmp, 'backslash_users.json'))), 2)

  def test_pipeline_failure_exits_with_1(self):
    path = self._pipeline_file(FAILING_PIPELINE)
    code, _, err = _run_cli(['run', path, '--runner=DirectRunner'])
    self.assertEqual(code, cli.EXIT_PIPELINE_FAILED)
    # Only the command's own message is checked: with an in-process worker,
    # Beam's logging also prints the worker's traceback, as it does for
    # `python -m apache_beam.yaml.main`.
    start = err.index('error: pipeline failed:')
    end = err.index('rerun with --verbose', start)
    message = err[start:end]
    self.assertIn('ZeroDivisionError', message)
    self.assertNotIn('Traceback', message)

    # Also check when `wait_until_finish` returns a non-DONE state directly.
    result = mock.Mock()
    result.wait_until_finish.return_value = cli.PipelineState.FAILED
    with mock.patch.object(beam.Pipeline, 'run', return_value=result):
      code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_PIPELINE_FAILED)
    self.assertIn('pipeline finished in state FAILED', err)

  def test_invalid_pipeline_exits_with_2(self):
    path = self._pipeline_file(INVALID_PIPELINE)
    code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn('could not build the pipeline:', err)
    self.assertIn('NoSuchTransform', err)
    self.assertNotIn('Traceback', err)

  def test_verbose_shows_traceback(self):
    path = self._pipeline_file(INVALID_PIPELINE)
    code, _, err = _run_cli(['run', path, '--verbose'])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn('Traceback', err)

  def test_other_flags_are_passed_through_in_order(self):
    path = self._pipeline_file(PIPELINE)
    with mock.patch.object(yaml_main,
                           'build_pipeline_components_from_argv',
                           side_effect=ValueError('stop')) as build:
      _run_cli([
          'run',
          path,
          '--runner=DirectRunner',
          '--jinja_variables={"a": 1}',
          '--verbose',
          '--project',
          'p'
      ])
    self.assertEqual(
        build.call_args.args[0],
        [
            '--yaml_pipeline_file=' + path,
            '--runner=DirectRunner',
            '--jinja_variables={"a": 1}',
            '--project',
            'p'
        ])

  def test_missing_package_exits_with_4(self):
    path = self._pipeline_file(PIPELINE)
    with mock.patch.dict(sys.modules, {'apache_beam.yaml': None}):
      code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('apache-beam[yaml]', err)

    with mock.patch.object(yaml_main,
                           'build_pipeline_components_from_argv',
                           side_effect=ImportError('No module named x')):
      code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('No module named x', err)
    self.assertIn('pip install', err)

  def test_pipeline_context_active_across_build_and_run(self):
    # Java expansion services (Sql, Kafka, JDBC, Iceberg, ...) are started
    # during `constructor(pipeline)` and queried again for artifacts during
    # `pipeline.run()`, so `Pipeline.__enter__`'s subprocess cache and
    # `--beam_services` overrides must stay active across both steps and be
    # cleaned up afterwards.
    path = self._pipeline_file(PIPELINE)
    real_build = yaml_main.build_pipeline_components_from_argv
    snapshots = {}

    def capture(stage):
      snapshots[stage] = (
          dict(subprocess_server.JavaJarServer._BEAM_SERVICES.replacements),
          set(subprocess_server.SubprocessServer._cache._live_owners),
      )

    def wrapped_build(argv):
      options, constructor, display_data = real_build(argv)

      def wrapped_constructor(pipeline):
        capture('build')
        constructor(pipeline)

      return options, wrapped_constructor, display_data

    result = mock.Mock()
    result.wait_until_finish.return_value = cli.PipelineState.DONE

    def fake_run(_self):
      capture('run')
      return result

    owners_before = set(subprocess_server.SubprocessServer._cache._live_owners)
    services_before = dict(
        subprocess_server.JavaJarServer._BEAM_SERVICES.replacements)

    with (
        mock.patch.object(yaml_main,
                          'build_pipeline_components_from_argv',
                          side_effect=wrapped_build),
        mock.patch.object(beam.Pipeline, 'run', fake_run),
    ):
      code, _, _ = _run_cli(
          ['run', path, '--beam_services={"target": "override"}'])

    self.assertEqual(code, cli.EXIT_OK)
    build_services, build_owners = snapshots['build']
    run_services, run_owners = snapshots['run']
    self.assertTrue(build_owners - owners_before)
    self.assertEqual(build_owners, run_owners)
    self.assertEqual(build_services.get('target'), 'override')
    self.assertEqual(run_services.get('target'), 'override')
    self.assertEqual(
        set(subprocess_server.SubprocessServer._cache._live_owners),
        owners_before)
    self.assertEqual(
        dict(subprocess_server.JavaJarServer._BEAM_SERVICES.replacements),
        services_before)

    # Also verify cleanup when `constructor(pipeline)` raises during build.
    def failing_build(argv):
      options, _, display_data = real_build(argv)

      def failing_constructor(_pipeline):
        self.assertGreater(
            len(subprocess_server.SubprocessServer._cache._live_owners),
            len(owners_before))
        raise RuntimeError('expansion failed')

      return options, failing_constructor, display_data

    with mock.patch.object(yaml_main,
                           'build_pipeline_components_from_argv',
                           side_effect=failing_build):
      code, _, _ = _run_cli(
          ['run', path, '--beam_services={"target": "override"}'])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertEqual(
        set(subprocess_server.SubprocessServer._cache._live_owners),
        owners_before)
    self.assertEqual(
        dict(subprocess_server.JavaJarServer._BEAM_SERVICES.replacements),
        services_before)

  def test_no_wait_until_finish(self):
    path = self._pipeline_file(PIPELINE)
    result = mock.Mock()
    result.state = cli.PipelineState.RUNNING
    with mock.patch.object(beam.Pipeline, 'run',
                           return_value=result) as run_mock:
      code, out, _ = _run_cli(['run', path, '--no_wait_until_finish'])
    self.assertEqual(code, cli.EXIT_OK)
    run_mock.assert_called_once()
    result.wait_until_finish.assert_not_called()
    self.assertIn('not waiting for completion', out)

  def test_ctrl_c_cancels_and_exits_with_130(self):
    path = self._pipeline_file(PIPELINE)
    result = mock.Mock()
    result.wait_until_finish.side_effect = KeyboardInterrupt
    with mock.patch.object(beam.Pipeline, 'run', return_value=result):
      code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_INTERRUPTED)
    result.cancel.assert_called_once()
    self.assertIn('Cancelled the pipeline.', err)


@unittest.skipIf(
    yaml_main is None or jsonschema is None, 'requires the [yaml] extra')
class PipeTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
    self.input = os.path.join(self.tmp, 'users.csv')
    with open(self.input, 'w') as f:
      f.write('name,age\nAda,36\nLinus,17\n')
    self.output = os.path.join(self.tmp, 'out', 'users.json')

  def test_pipes_csv_to_json_and_lists_the_files(self):
    code, out, _ = _run_cli([
        'pipe',
        '--from',
        self.input,
        '--to',
        self.output,
        '--runner=DirectRunner'
    ])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertIn('Wrote 1 file:', out)
    self.assertEqual([json.loads(line) for line in _read_lines(self.output)],
                     [{
                         'name': 'Ada', 'age': 36
                     }, {
                         'name': 'Linus', 'age': 17
                     }])

    # Also check bare filenames in the current working directory.
    cwd = os.getcwd()
    self.addCleanup(os.chdir, cwd)
    os.chdir(self.tmp)
    code, out, _ = _run_cli([
        'pipe',
        '--from',
        'users.csv',
        '--to',
        'relative_users.json',
        '--runner=DirectRunner'
    ])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertIn('Wrote 1 file:\n  relative_users.json', out)
    code, _, err = _run_cli(
        ['pipe', '--from', 'users.csv', '--to', 'relative_users.json'])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn('beam pipe never overwrites data', err)

  def test_backslash_sequences_in_paths(self):
    tricky_input = os.path.join(self.tmp, r'C_\Users\tmp_users.csv')
    tricky_output = os.path.join(self.tmp, r'C_\Users\out\users.json')
    os.makedirs(os.path.dirname(tricky_input), exist_ok=True)
    os.makedirs(os.path.dirname(tricky_output), exist_ok=True)
    shutil.copyfile(self.input, tricky_input)
    code, out, _ = _run_cli([
        'pipe',
        '--from',
        tricky_input,
        '--to',
        tricky_output,
        '--runner=DirectRunner'
    ])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertIn('Wrote 1 file:', out)
    self.assertEqual(len(_read_lines(tricky_output)), 2)

  def test_refuses_existing_output(self):
    os.makedirs(os.path.dirname(self.output), exist_ok=True)
    for i in range(7):
      with open(f'{self.output}-0000{i}-of-00007', 'w') as f:
        f.write('keep me\n')
    code, _, err = _run_cli(['pipe', '--from', self.input, '--to', self.output])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn('beam pipe never overwrites data', err)
    self.assertIn('... and 2 more', err)
    with open(f'{self.output}-00000-of-00007') as f:
      self.assertEqual(f.read(), 'keep me\n')

  def test_pipes_csv_to_parquet_and_back_to_json(self):
    parquet_prefix = os.path.join(self.tmp, 'out', 'users.parquet')
    json_prefix = os.path.join(self.tmp, 'out', 'roundtrip.json')
    code, out, _ = _run_cli([
        'pipe',
        '--from',
        self.input,
        '--to',
        parquet_prefix,
        '--runner=DirectRunner'
    ])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertIn('Wrote 1 file:', out)

    code, out, _ = _run_cli([
        'pipe',
        '--from',
        parquet_prefix + '*',
        '--to',
        json_prefix,
        '--runner=DirectRunner'
    ])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertEqual([json.loads(line) for line in _read_lines(json_prefix)],
                     [{
                         'name': 'Ada', 'age': 36
                     }, {
                         'name': 'Linus', 'age': 17
                     }])

  def test_filesystem_options_and_errors(self):
    # Fresh CLI invocations start with FileSystems._pipeline_options == None;
    # _refuse_existing_output must run after Pipeline(options=...) sets it.
    seen_projects = []

    def check_options_on_match(_patterns):
      opts = FileSystems._pipeline_options
      seen_projects.append(
          opts.view_as(GoogleCloudOptions).project if opts else None)
      raise BeamIOError(
          'Match operation failed',
          {'gs://b/u*': RuntimeError('GCP dependencies are not installed')})

    FileSystems._pipeline_options = None
    with mock.patch.object(FileSystems,
                           'match',
                           side_effect=check_options_on_match):
      code, _, err = _run_cli([
          'pipe',
          '--from',
          self.input,
          '--to',
          'gs://b/users.json',
          '--project=my-custom-project'
      ])
    self.assertEqual(seen_projects, ['my-custom-project'])
    self.assertEqual(code, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('GCP dependencies are not installed', err)

    # Missing extras raised as ValueError or wrapped in BeamIOError exit with 4.
    for exc in [
        ValueError('Unable to get filesystem from specified path'),
        BeamIOError('Unable to get the Filesystem',
                    {'hdfs://b/u*': ImportError('Failed to import hdfs')}),
    ]:
      with self.subTest(exc=exc):
        with mock.patch.object(FileSystems, 'match', side_effect=exc):
          code, _, err = _run_cli(
              ['pipe', '--from', self.input, '--to', 's3://bucket/users.json'])
        self.assertEqual(code, cli.EXIT_MISSING_PACKAGE)

    # Remote access / configuration errors exit with 2 before running (and show
    # tracebacks only with --verbose), and 1 if post-run listing fails.
    io_err = BeamIOError(
        'Match operation failed',
        {'s3://bucket/users.json*': RuntimeError('403 Forbidden')})
    with mock.patch.object(FileSystems, 'match', side_effect=io_err):
      code, _, err = _run_cli(
          ['pipe', '--from', self.input, '--to', 's3://bucket/users.json'])
      self.assertEqual(code, cli.EXIT_USAGE)
      self.assertIn('403 Forbidden', err)
      self.assertNotIn('Traceback', err)

      code, _, err = _run_cli([
          'pipe',
          '--from',
          self.input,
          '--to',
          's3://bucket/users.json',
          '--verbose'
      ])
      self.assertEqual(code, cli.EXIT_USAGE)
      self.assertIn('Traceback', err)

    real_match = FileSystems.match
    output_checks = 0

    def fail_post_run_match(patterns, *args, **kwargs):
      nonlocal output_checks
      if patterns == [self.output + '*']:
        output_checks += 1
        if output_checks == 2:
          raise io_err
      return real_match(patterns, *args, **kwargs)

    result = mock.Mock()
    result.wait_until_finish.return_value = cli.PipelineState.DONE
    with (
        mock.patch.object(beam.Pipeline, 'run', return_value=result),
        mock.patch.object(FileSystems, 'match',
                          side_effect=fail_post_run_match),
    ):
      code, _, err = _run_cli(
          ['pipe', '--from', self.input, '--to', self.output])
    self.assertEqual(code, cli.EXIT_PIPELINE_FAILED)
    self.assertIn('403 Forbidden', err)

  def test_no_wait_and_failure_and_ctrl_c_exit_codes(self):
    result = mock.Mock()
    result.state = cli.PipelineState.RUNNING
    with mock.patch.object(beam.Pipeline, 'run', return_value=result):
      code, out, _ = _run_cli([
          'pipe',
          '--from',
          self.input,
          '--to',
          self.output,
          '--no_wait_until_finish'
      ])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertIn('not waiting for completion', out)
    self.assertNotIn('Wrote', out)

    result.wait_until_finish.side_effect = RuntimeError('worker crashed')
    with mock.patch.object(beam.Pipeline, 'run', return_value=result):
      code, _, err = _run_cli(
          ['pipe', '--from', self.input, '--to', self.output])
    self.assertEqual(code, cli.EXIT_PIPELINE_FAILED)
    self.assertIn('worker crashed', err)

    result.wait_until_finish.side_effect = KeyboardInterrupt
    with mock.patch.object(beam.Pipeline, 'run', return_value=result):
      code, _, err = _run_cli(
          ['pipe', '--from', self.input, '--to', self.output])
    self.assertEqual(code, cli.EXIT_INTERRUPTED)
    self.assertIn('Cancelled the pipeline.', err)

  def test_dry_run_prints_a_pipeline_that_beam_run_runs(self):
    for flag in ('--dry-run', '--dry_run'):
      with self.subTest(flag=flag):
        code, out, err = _run_cli(
            ['pipe', '--from', self.input, '--to', self.output, flag])
        self.assertEqual(code, cli.EXIT_OK)
        self.assertTrue(out.startswith('# Generated by: beam pipe --from'))
        self.assertIn('Dry run:', err)
        self.assertEqual(glob.glob(self.output + '*'), [])

    saved = os.path.join(self.tmp, 'pipeline.yaml')
    with open(saved, 'w') as f:
      f.write(out)
    code, _, _ = _run_cli(['run', saved, '--runner=DirectRunner'])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertEqual(len(_read_lines(self.output)), 2)

  def test_flag_errors_exit_with_2(self):
    code, _, err = _run_cli(['pipe', '--from', self.input, '--to', 'out/x'])
    self.assertEqual(code, cli.EXIT_USAGE)
    self.assertIn('add --to-format', err)


class HelpersTest(unittest.TestCase):
  def test_local_paths_warning_only_for_remote_runners(self):
    for argv, warned in [([], False), (['--runner=PrismRunner'], False),
                         (['--runner=dataflow'], True)]:
      with self.subTest(argv=argv):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
          cli._warn_if_local_paths(
              ['users.csv', 'file:///tmp/a.csv', 'gs://b/out'],
              PipelineOptions(argv))
        self.assertEqual('users.csv' in err.getvalue(), warned)
        self.assertEqual('file:///tmp/a.csv' in err.getvalue(), warned)
        self.assertNotIn('gs://b/out', err.getvalue())

  def test_cancel_failure_is_reported(self):
    result = mock.Mock()
    result.cancel.side_effect = RuntimeError('job not found')
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
      cli._cancel(result)
    self.assertIn(
        'Could not cancel the pipeline (job not found)', err.getvalue())

  def test_describe_runner(self):
    prism_result = type('PipelineResult', (), {})()
    type(prism_result).__module__ = (
        'apache_beam.runners.portability.portable_runner')
    direct_result = object()
    cases = [
        ([], prism_result, 'locally on Prism'),
        ([], direct_result, 'locally on the Python direct runner'),
        (['--runner=DirectRunner'], prism_result, 'locally on Prism'),
        (['--runner=dataflow'], direct_result, 'on dataflow'),
    ]
    for argv, result, expected in cases:
      with self.subTest(argv=argv, expected=expected):
        self.assertEqual(
            cli._describe_runner(PipelineOptions(argv), result), expected)

  def test_summarize_keeps_the_first_lines(self):
    error = RuntimeError(
        'Job failed.\n\n' + '\n'.join(f'line {i}' for i in range(20)))
    summary = cli._summarize(error)
    self.assertTrue(summary.startswith('Job failed.\n  line 0'))
    self.assertIn('line 8', summary)
    self.assertNotIn('line 9', summary)
    self.assertTrue(summary.endswith('...'))

  def test_summarize_embedded_python_traceback(self):
    # The shape of a Prism failure: context, then the worker's traceback.
    error = RuntimeError(
        'Pipeline job-001 failed in state FAILED: bundle b failed:'
        'Traceback (most recent call last):\n'
        '  File "common.py", line 1, in process\n'
        '    return fn(x)\n'
        'ZeroDivisionError: division by zero\n\n'
        'During handling of the above exception, another exception '
        'occurred:\n\n'
        'Traceback (most recent call last):\n'
        '  File "common.py", line 2, in process\n'
        "ZeroDivisionError: division by zero [while running 'Map']\n")
    self.assertEqual(
        cli._summarize(error),
        'Pipeline job-001 failed in state FAILED: bundle b failed:\n'
        "  ZeroDivisionError: division by zero [while running 'Map']")

  def test_summarize_names_the_type_of_direct_errors(self):
    self.assertEqual(
        cli._summarize(ZeroDivisionError('division by zero')),
        'ZeroDivisionError: division by zero')
    self.assertEqual(cli._summarize(RuntimeError('boom')), 'boom')
    self.assertEqual(cli._summarize(RuntimeError('')), "RuntimeError('')")


if __name__ == '__main__':
  unittest.main()
