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

`beam pipe --from SOURCE --to SINK` reads files in one format and writes them
in another, without writing a pipeline first. It builds a two-step Beam YAML
pipeline (see `pipe_spec`) and runs it like `beam run`. `--dry-run` prints
that pipeline instead, ready to save, edit and `beam run`.

On top of that, both give command-line users and scripts what they rely on:
errors without tracebacks unless `--verbose` is given, documented exit codes,
Ctrl-C cancelling the pipeline, and a last line naming the runner that ran it.
"""

import argparse
import logging
import os
import sys
import traceback

import yaml

import apache_beam as beam
from apache_beam.cli import pipe_spec
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineState

# Exit codes, also listed in `--help`. Usage errors use 2, the code argparse
# itself exits with.
EXIT_OK = 0
EXIT_PIPELINE_FAILED = 1
EXIT_USAGE = 2
EXIT_MISSING_PACKAGE = 4
EXIT_INTERRUPTED = 130  # 128 + SIGINT, the shell convention for Ctrl-C.

_EXIT_CODES = """
exit codes:
  0    the pipeline finished (or --dry-run checked it, or --no_wait_until_finish submitted it)
  1    the pipeline failed
  2    usage error, or the pipeline could not be built
  4    a required package is missing
  130  interrupted (Ctrl-C)
"""

_RUN_EPILOG = """\
examples:
  beam run pipeline.yaml
  beam run pipeline.yaml --runner=dataflow --project=my-project \\
      --region=us-central1 --temp_location=gs://my-bucket/tmp

Every other --flag is passed to the pipeline as a Beam pipeline option.
Write them after FILE, as --flag=value.
""" + _EXIT_CODES

_PIPE_EPILOG = """\
examples:
  beam pipe --from users.csv --to out/users.parquet
  beam pipe --from 'data/*.parquet' --to out/data --to-format json
  beam pipe --from users.csv --to out/users.parquet --dry-run > pipeline.yaml

Formats: csv, json, parquet, avro (from the file extension unless overridden).
Paths can be local or on any Beam filesystem (gs://, s3://, ...). The
output path is a prefix: Beam writes sharded files that start with it, and
existing output is never overwritten.
Every other --flag is passed to the pipeline as a Beam pipeline option.
""" + _EXIT_CODES


def main(argv=None):
  """Runs the `beam` command and returns its exit code."""
  argv = sys.argv[1:] if argv is None else argv
  args, pipeline_args = _build_parser().parse_known_args(argv)
  # The same logging setup as `python -m apache_beam.yaml.main`.
  logging.getLogger().setLevel(logging.INFO)
  try:
    if args.command == 'run':
      return _run(args.file, pipeline_args, args.verbose)
    return _pipe(args, pipeline_args, argv[argv.index('pipe') + 1:])
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

  pipe = commands.add_parser(
      'pipe',
      help='read files in one format and write them in another',
      description=(
          'Read files in one format and write them in another, by building '
          'and running a two-step Beam YAML pipeline.'),
      epilog=_PIPE_EPILOG,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      allow_abbrev=False)
  pipe.add_argument(
      '--from',
      dest='source',
      required=True,
      metavar='SOURCE',
      help='files to read: a path or glob')
  pipe.add_argument(
      '--to',
      dest='sink',
      required=True,
      metavar='SINK',
      help='where to write: a path prefix')
  pipe.add_argument(
      '--format',
      metavar='FMT',
      help='format of both sides; overridden by --from-format / --to-format')
  pipe.add_argument(
      '--from-format',
      metavar='FMT',
      help='format of the source (default: from the file extension)')
  pipe.add_argument(
      '--to-format',
      metavar='FMT',
      help='format of the sink (default: from the file extension)')
  # `--dry_run` is accepted and consumed too: Beam has a test-only pipeline
  # option of that name, which some runners honor and others ignore. Passed
  # through, it could run the pipeline the user asked not to run.
  pipe.add_argument(
      '--dry-run',
      '--dry_run',
      action='store_true',
      help='check the pipeline and print it as YAML instead of running it')
  pipe.add_argument(
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

  return _run_yaml(['--yaml_pipeline_file=' + file] + pipeline_args, verbose)


def _pipe(args, pipeline_args, pipe_argv):
  """Runs `beam pipe`; `pipe_argv` is its command line, for --dry-run."""
  try:
    pipe = pipe_spec.build_pipe_spec(
        args.source,
        args.sink,
        args.from_format or args.format,
        args.to_format or args.format)
  except pipe_spec.PipeSpecError as e:
    raise _CliError(str(e), EXIT_USAGE) from None

  # A dry run makes every check a real run makes, including checking for
  # existing output and building the pipeline, so that it predicts whether
  # `beam run` on its output will work.
  return _run_yaml(
      ['--yaml_pipeline=' + yaml.safe_dump(pipe.spec, sort_keys=False)] +
      pipeline_args,
      args.verbose,
      local_paths=[args.source, args.sink],
      dry_run_yaml=pipe_spec.to_yaml(pipe.spec, pipe_argv)
      if args.dry_run else None,
      output_prefix=pipe.output_prefix)


def _run_yaml(
    yaml_main_argv,
    verbose,
    local_paths=(),
    dry_run_yaml=None,
    output_prefix=None):
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
  # that build errors and pipeline failures get different exit codes, and so
  # that `pipe --dry-run` can stop after building. Enter `Pipeline.__enter__`
  # across both steps so Java expansion service subprocesses stay cached
  # between `constructor(pipeline)` and `pipeline.run()` (when artifacts are
  # staged), and `--beam_services` overrides apply to both.
  pipeline = None
  try:
    try:
      options, constructor, display_data = (
          yaml_main.build_pipeline_components_from_argv(yaml_main_argv))
      pipeline = beam.Pipeline(options=options, display_data=display_data)
      pipeline.__enter__()
      if output_prefix is not None:
        # Checked after Pipeline(options=...) calls FileSystems.set_options, so
        # remote filesystems receive flags like --project, --s3_*, or --hdfs_*.
        _refuse_existing_output(output_prefix, verbose)
      constructor(pipeline)
    except _CliError:
      raise
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
      raise _CliError(
          f'could not build the pipeline: {e}', EXIT_USAGE) from None

    if local_paths:
      _warn_if_local_paths(local_paths, options)
    if dry_run_yaml is not None:
      # The YAML goes to stdout and every message to stderr, so that
      # `> pipeline.yaml` captures exactly the pipeline.
      print(dry_run_yaml, end='')
      print(
          'Dry run: the pipeline was built and checked but not run. To run '
          'it, save the YAML (for example with > pipeline.yaml) and use: '
          'beam run pipeline.yaml',
          file=sys.stderr)
      return EXIT_OK

    no_wait = options.view_as(StandardOptions).no_wait_until_finish
    state = None
    try:
      result = pipeline.run()
      if not no_wait:
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
  finally:
    if pipeline is not None and hasattr(pipeline, '_extra_context'):
      pipeline._extra_context.close()
    # PortableRunner's atexit handler retains `result` and its
    # `_runtime_exception.__traceback__`, which references this frame.
    pipeline = None

  if no_wait:
    print(
        f'Submitted {_describe_runner(options, result)} '
        '(not waiting for completion).')
    return EXIT_OK

  if state != PipelineState.DONE:
    raise _CliError(f'pipeline finished in state {state}', EXIT_PIPELINE_FAILED)

  print(f'Done. Ran {_describe_runner(options, result)}.')
  if output_prefix is not None:
    written = _matching_files(
        output_prefix, verbose, error_exit_code=EXIT_PIPELINE_FAILED)
    print(f'Wrote {len(written)} file{"" if len(written) == 1 else "s"}:')
    for path in written:
      print('  ' + path)
  return EXIT_OK


def _refuse_existing_output(prefix, verbose):
  """Fails if any file starts with the output prefix: pipe never overwrites."""
  existing = _matching_files(prefix, verbose, error_exit_code=EXIT_USAGE)
  if existing:
    shown = '\n'.join('  ' + path for path in existing[:5])
    more = f'\n  ... and {len(existing) - 5} more' if len(existing) > 5 else ''
    raise _CliError(
        f'output already exists for {prefix!r}; beam pipe never overwrites '
        f'data:\n{shown}{more}\n'
        '  remove these files or choose another --to path',
        EXIT_USAGE)


def _matching_files(prefix, verbose, error_exit_code):
  # LocalFileSystem.match calls os.path.dirname(prefix), which is '' for a
  # bare filename in the current directory and causes _list('') to return no
  # files. Prepending './' (or '.\' on Windows) gives dirname '.' so files in
  # the current directory are found.
  dot = (
      '.' +
      os.sep if '://' not in prefix and not os.path.dirname(prefix) else '')
  try:
    match = FileSystems.match([dot + prefix + '*'])[0]
  except (BeamIOError, ValueError) as e:
    _maybe_print_traceback(verbose)
    cause = (
        next(iter(e.exception_details.values()))
        if isinstance(e, BeamIOError) and e.exception_details else e)
    if (isinstance(cause, ImportError) or
        'dependencies are not installed' in str(cause) or
        'Unable to get filesystem from specified path' in str(cause)):
      # Missing filesystem extra, for example [gcp], [aws], [azure], [hadoop].
      raise _CliError(
          f'a required package is missing: {cause}',
          EXIT_MISSING_PACKAGE) from None
    raise _CliError(
        f'could not access {prefix!r}: {cause}', error_exit_code) from None
  return sorted(
      m.path[len(dot):] if dot and m.path.startswith(dot) else m.path
      for m in match.metadata_list)


def _runner_name(runner):
  """'DirectRunner', 'direct', None -> 'direct'; 'DataflowRunner' -> ...

  None means the SDK's default runner, DirectRunner.
  """
  name = str(runner or 'DirectRunner').rsplit('.', 1)[-1].lower()
  return name[:-len('runner')] if name.endswith('runner') else name


# Runners that execute on this machine, by _runner_name.
_LOCAL_RUNNERS = frozenset(
    ['direct', 'switchingdirect', 'bundlebaseddirect', 'prism', 'fnapi'])


def _warn_if_local_paths(paths, options):
  """Warns when a runner that isn't local would open local files.

  On a cluster the runner's workers open the files, so a local or relative
  path refers to each worker's own disk or container. The job can then
  succeed while reading nothing, or write where the user never looks.
  """
  runner = options.view_as(StandardOptions).runner
  local = [
      path for path in paths
      if '://' not in path or path.lower().startswith('file://')
  ]
  if local and _runner_name(runner) not in _LOCAL_RUNNERS:
    print(
        f'warning: {", ".join(local)}: local paths are opened by the '
        f"{runner} runner's workers, not by this machine; use storage every "
        'worker can reach (for example gs://, s3:// or hdfs://, or a shared '
        'mount at the same absolute path)',
        file=sys.stderr)


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
  if _runner_name(runner) != 'direct':
    return f'on {runner}'
  if 'portable_runner' in type(result).__module__:
    return 'locally on Prism'
  return 'locally on the Python direct runner'


if __name__ == '__main__':
  sys.exit(main())
