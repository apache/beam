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

"""Entry point of the experimental `beam` command.

`beam run FILE` runs a Beam YAML pipeline file the same way as
`python -m apache_beam.yaml.main --yaml_pipeline_file=FILE`. It builds the
pipeline with that module's own functions, so Beam YAML features (Jinja
variables, providers, ...) and runners behave identically, and every flag the
command doesn't define is passed through unchanged as a Beam pipeline option.

On top of that, it gives command-line users and scripts what they rely on:
errors without tracebacks unless `--verbose` is given, documented exit codes,
Ctrl-C cancelling the pipeline, and a last line naming the runner that ran it.
"""

import argparse
import logging
import os
import sys
import traceback

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineState

# Exit codes, also listed in `beam run --help`. Usage errors use 2, the code
# argparse itself exits with.
EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4
EXIT_INTERRUPTED = 130  # 128 + SIGINT, the shell convention for Ctrl-C.

_RUN_EPILOG = """\
examples:
  beam run pipeline.yaml
  beam run pipeline.yaml --runner=dataflow --project=my-project \\
      --region=us-central1 --temp_location=gs://my-bucket/tmp

Every other --flag is passed to the pipeline as a Beam pipeline option.
Write them after FILE, as --flag=value.

exit codes:
  0    the pipeline finished
  1    the pipeline failed
  2    usage error, or the pipeline could not be built
  4    a required package is missing
  130  interrupted (Ctrl-C)
"""


def main(argv=None):
  """Runs the `beam` command and returns its exit code."""
  args, pipeline_args = _build_parser().parse_known_args(argv)
  # The same logging setup as `python -m apache_beam.yaml.main`.
  logging.getLogger().setLevel(logging.INFO)
  try:
    return _run(args.file, pipeline_args, args.verbose)
  except _CliError as e:
    print(f'error: {e}', file=sys.stderr)
    return e.exit_code
  except KeyboardInterrupt:
    print('error: interrupted', file=sys.stderr)
    return EXIT_INTERRUPTED


def _build_parser():
  parser = argparse.ArgumentParser(
      prog='beam',
      description='Apache Beam command-line tool (experimental).',
      allow_abbrev=False)
  commands = parser.add_subparsers(
      dest='command', required=True, metavar='COMMAND')
  # allow_abbrev=False on each parser, so that a pipeline option is never
  # taken for an abbreviation of one of this command's flags.
  run = commands.add_parser(
      'run',
      help='run a Beam YAML pipeline file',
      description=(
          'Run a Beam YAML pipeline file, like '
          'python -m apache_beam.yaml.main --yaml_pipeline_file=FILE.'),
      epilog=_RUN_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  run.add_argument(
      'file',
      metavar='FILE',
      help='the pipeline file: a local path, or a path on any Beam '
      'filesystem such as gs://')
  run.add_argument(
      '--verbose', action='store_true', help='show tracebacks for errors')
  return parser


class _CliError(Exception):
  """An error reported to the user as one message, without a traceback."""
  def __init__(self, message, exit_code):
    super().__init__(message)
    self.exit_code = exit_code


def _run(file, pipeline_args, verbose):
  # Paths with a scheme (gs://, s3://, ...) are opened by Beam's filesystems
  # when the pipeline is built, which reports its own errors for them.
  if '://' not in file and not os.path.isfile(file):
    hint = ''
    if pipeline_args:
      # `beam run --runner dataflow p.yaml` makes argparse take "dataflow"
      # for FILE.
      hint = '\n  pipeline options go after FILE, written as --flag=value'
    raise _CliError(f'no such file: {file!r}{hint}', EXIT_USAGE)

  # Beam YAML is imported here, not at the top of the module, because it needs
  # the [yaml] extra; a missing extra is then reported with exit code 4 and a
  # hint instead of a traceback.
  try:
    from apache_beam.yaml import main as yaml_main
  except ImportError as e:
    _maybe_print_traceback(verbose)
    raise _CliError(
        f'a required package is missing: {e}\n'
        "  Beam YAML needs the [yaml] extra: pip install 'apache-beam[yaml]'",
        EXIT_MISSING_PACKAGE) from None

  # Build and run in two steps (yaml_main.run does both in a `with` block) so
  # that build errors and pipeline failures get different exit codes.
  try:
    options, constructor, display_data = (
        yaml_main.build_pipeline_components_from_argv(
            ['--yaml_pipeline_file=' + file] + pipeline_args))
    pipeline = beam.Pipeline(options=options, display_data=display_data)
    constructor(pipeline)
  except ImportError as e:
    # A transform's extra is missing, for example [gcp] for BigQuery.
    _maybe_print_traceback(verbose)
    raise _CliError(
        f'a required package is missing: {e}\n'
        '  install the extra the pipeline needs, for example: '
        "pip install 'apache-beam[yaml,gcp]'",
        EXIT_MISSING_PACKAGE) from None
  except Exception as e:  # pylint: disable=broad-except
    _maybe_print_traceback(verbose)
    raise _CliError(f'could not build the pipeline: {e}', EXIT_USAGE) from None

  try:
    result = pipeline.run()
    try:
      state = result.wait_until_finish()
    except KeyboardInterrupt:
      # main() reports the interruption after the pipeline is cancelled.
      _cancel(result)
      raise
  except Exception as e:  # pylint: disable=broad-except
    _maybe_print_traceback(verbose)
    more = '' if verbose else '\n  rerun with --verbose for the traceback'
    raise _CliError(
        f'pipeline failed: {_summarize(e)}{more}',
        EXIT_PIPELINE_FAILED) from None
  if state != PipelineState.DONE:
    raise _CliError(f'pipeline finished in state {state}', EXIT_PIPELINE_FAILED)

  print(f'Done. Ran {_describe_runner(options, result)}.')
  return EXIT_OK


def _maybe_print_traceback(verbose):
  if verbose:
    traceback.print_exc()


_PYTHON_TRACEBACK = 'Traceback (most recent call last):'


def _summarize(error, max_lines=10):
  """Returns the part of a pipeline error that says what went wrong.

  Runner errors often embed whole stack traces. Prism and the default runner
  put a line of context ("Pipeline job-001 failed in state FAILED: bundle ...
  failed:") before the worker's Python traceback, whose last line names the
  exception and the failing step; Java runners add `\\tat ...` frames. Stack
  frames are indented, so only unindented lines are kept, and for a Python
  traceback only the context and the last exception line. `--verbose` still
  prints everything.
  """
  message = str(error)
  if type(error) is not RuntimeError and not message.startswith(
      type(error).__name__):
    # Errors raised directly (for example a ZeroDivisionError from the
    # in-process runner) don't name their type in the message.
    message = f'{type(error).__name__}: {message}'
  lines = [
      line.rstrip() for line in message.splitlines()
      if line.strip() and not line[0].isspace()
  ]
  if any(_PYTHON_TRACEBACK in line for line in lines):
    context = lines[0].split(_PYTHON_TRACEBACK)[0].rstrip()
    causes = [
        line for line in lines if _PYTHON_TRACEBACK not in line and
        not line.startswith('During handling of the above exception')
    ]
    lines = [line for line in (context, causes[-1] if causes else '') if line]
  if not lines:
    return repr(error)
  shown = '\n  '.join(lines[:max_lines])
  return shown + ('\n  ...' if len(lines) > max_lines else '')


def _cancel(result):
  """Cancels the pipeline after Ctrl-C, saying whether that worked.

  A local runner usually receives the same Ctrl-C and stops by itself, in
  which case cancelling fails harmlessly.
  """
  try:
    result.cancel()
    print('Cancelled the pipeline.', file=sys.stderr)
  except Exception as e:  # pylint: disable=broad-except
    print(
        f'Could not cancel the pipeline ({e}). If it is still running, '
        "stop it with the runner's tools.",
        file=sys.stderr)


def _describe_runner(options, result):
  """Says which runner ran the pipeline, for the last line of output.

  Without --runner (or with --runner=DirectRunner), the Python SDK runs the
  pipeline on Prism and falls back to the Python direct runner when Prism
  can't run it. The type of the result tells the two apart.
  """
  runner = options.view_as(StandardOptions).runner
  name = str(runner or 'DirectRunner').rsplit('.', 1)[-1].lower()
  if name not in ('direct', 'directrunner'):
    return f'on {runner}'
  if 'portable_runner' in type(result).__module__:
    return 'locally on Prism'
  return 'locally on the Python direct runner'


if __name__ == '__main__':
  sys.exit(main())
