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
import os
import shutil
import tempfile
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.cli import main as cli
from apache_beam.options.pipeline_options import PipelineOptions

try:
  from apache_beam.yaml import main as yaml_main
except ImportError:
  yaml_main = None

PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: "%(input)s"}
    - type: Filter
      config: {language: python, keep: "age > 20"}
    - type: WriteToJson
      config: {path: "%(output)s"}
'''

FAILING_PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromCsv
      config: {path: "%(input)s"}
    - type: MapToFields
      config: {language: python, fields: {x: "age // 0"}}
    - type: WriteToJson
      config: {path: "%(output)s"}
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
    for argv in ([], ['run'], ['nosuchcommand', 'x']):
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


@unittest.skipIf(yaml_main is None, 'requires the [yaml] extra')
class RunTest(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.mkdtemp()
    self.addCleanup(shutil.rmtree, self.tmp)
    self.input = os.path.join(self.tmp, 'users.csv')
    with open(self.input, 'w') as f:
      f.write('name,age\nAda,36\nLinus,17\nGrace,45\n')

  def _pipeline_file(self, template, output='out/users'):
    path = os.path.join(self.tmp, 'pipeline.yaml')
    with open(path, 'w') as f:
      f.write(
          template % {
              'input': self.input, 'output': os.path.join(self.tmp, output)
          })
    return path

  def test_runs_like_yaml_main(self):
    # The CLI and `python -m apache_beam.yaml.main` write the same output for
    # the same pipeline file and options.
    path = self._pipeline_file(PIPELINE, 'cli/users')
    code, out, _ = _run_cli(['run', path, '--runner=DirectRunner'])
    self.assertEqual(code, cli.EXIT_OK)
    self.assertRegex(out, r'Done\. Ran locally on (Prism|the Python direct)')

    reference = self._pipeline_file(PIPELINE, 'reference/users')
    yaml_main.run(
        ['--yaml_pipeline_file=' + reference, '--runner=DirectRunner'])
    cli_rows = _read_lines(os.path.join(self.tmp, 'cli/users'))
    self.assertEqual(len(cli_rows), 2)
    self.assertEqual(
        cli_rows, _read_lines(os.path.join(self.tmp, 'reference/users')))

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
    with mock.patch.object(yaml_main,
                           'build_pipeline_components_from_argv',
                           side_effect=ImportError('No module named x')):
      code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_MISSING_PACKAGE)
    self.assertIn('No module named x', err)
    self.assertIn('pip install', err)

  def test_ctrl_c_cancels_and_exits_with_130(self):
    path = self._pipeline_file(PIPELINE)
    result = mock.Mock()
    result.wait_until_finish.side_effect = KeyboardInterrupt
    with mock.patch.object(beam.Pipeline, 'run', return_value=result):
      code, _, err = _run_cli(['run', path])
    self.assertEqual(code, cli.EXIT_INTERRUPTED)
    result.cancel.assert_called_once()
    self.assertIn('Cancelled the pipeline.', err)


class HelpersTest(unittest.TestCase):
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


if __name__ == '__main__':
  unittest.main()
