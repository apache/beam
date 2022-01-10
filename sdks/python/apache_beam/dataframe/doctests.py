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

"""A module that allows running existing pandas doctests with Beam dataframes.

This module hooks into the doctesting framework by providing a custom
runner and, in particular, an OutputChecker, as well as providing a fake
object for mocking out the pandas module.

The (novel) sequence of events when running a doctest is as follows.

  1. The test invokes `pd.DataFrame(...)` (or similar) and an actual dataframe
     is computed and stashed but a Beam deferred dataframe is returned
     in its place.
  2. Computations are done on these "dataframes," resulting in new objects,
     but as these are actually deferred, only expression trees are built.
     In the background, a mapping of id -> deferred dataframe is stored for
     each newly created dataframe.
  3. When any dataframe is printed out, the repr has been overwritten to
     print `Dataframe[id]`. The aforementened mapping is used to map this back
     to the actual dataframe object, which is then computed via Beam, and its
     the (stringified) result plugged into the actual output for comparison.
  4. The comparison is then done on the sorted lines of the expected and actual
     values.
"""

import collections
import contextlib
import doctest
import re
import sys
import traceback
from io import StringIO
from typing import Any
from typing import Dict
from typing import List

import numpy as np
import pandas as pd

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frames  # pylint: disable=unused-import
from apache_beam.dataframe import pandas_top_level_functions
from apache_beam.dataframe import transforms
from apache_beam.dataframe.frame_base import DeferredBase


class FakePandasObject(object):
  """A stand-in for the wrapped pandas objects.
  """
  def __init__(self, pandas_obj, test_env):
    self._pandas_obj = pandas_obj
    self._test_env = test_env

  def __call__(self, *args, **kwargs):
    result = self._pandas_obj(*args, **kwargs)
    if type(result) in DeferredBase._pandas_type_map:
      placeholder = expressions.PlaceholderExpression(result.iloc[0:0])
      self._test_env._inputs[placeholder] = result
      return DeferredBase.wrap(placeholder)
    else:
      return result

  def __getattr__(self, name):
    attr = getattr(self._pandas_obj, name)
    if callable(attr):
      result = FakePandasObject(attr, self._test_env)
    else:
      result = attr
    # Cache this so two lookups return the same object.
    setattr(self, name, result)
    return result

  def __reduce__(self):
    return lambda: pd, ()


class TestEnvironment(object):
  """A class managing the patching (of methods, inputs, and outputs) needed
  to run and validate tests.

  These classes are patched to be able to recognize and retrieve inputs
  and results, stored in `self._inputs` and `self._all_frames` respectively.
  """
  def __init__(self):
    self._inputs = {}
    self._all_frames = {}

  def fake_pandas_module(self):
    return FakePandasObject(pandas_top_level_functions.pd_wrapper, self)

  @contextlib.contextmanager
  def _monkey_patch_type(self, deferred_type):
    """Monkey-patch __init__ to record a pointer to all created frames, and
    __repr__ to be able to recognize them in the doctest output.
    """
    try:
      old_init, old_repr = deferred_type.__init__, deferred_type.__repr__

      def new_init(df, *args, **kwargs):
        old_init(df, *args, **kwargs)
        self._all_frames[id(df)] = df

      deferred_type.__init__ = new_init
      deferred_type.__repr__ = lambda self: 'DeferredBase[%s]' % id(self)
      self._recorded_results = collections.defaultdict(list)
      yield
    finally:
      deferred_type.__init__, deferred_type.__repr__ = old_init, old_repr

  @contextlib.contextmanager
  def context(self):
    """Creates a context within which DeferredBase types are monkey patched
    to record ids."""
    with contextlib.ExitStack() as stack:
      for deferred_type in DeferredBase._pandas_type_map.values():
        stack.enter_context(self._monkey_patch_type(deferred_type))
      yield


class _InMemoryResultRecorder(object):
  """Helper for extracting computed results from a Beam pipeline.

  Used as follows::

  with _InMemoryResultRecorder() as recorder:
    with beam.Pipeline() as p:
      ...
      pcoll | beam.Map(recorder.record_fn(name))

    seen = recorder.get_recorded(name)
  """

  # Class-level value to survive pickling.
  _ALL_RESULTS = {}  # type: Dict[str, List[Any]]

  def __init__(self):
    self._id = id(self)

  def __enter__(self):
    self._ALL_RESULTS[self._id] = collections.defaultdict(list)
    return self

  def __exit__(self, *unused_args):
    del self._ALL_RESULTS[self._id]

  def record_fn(self, name):
    def record(value):
      self._ALL_RESULTS[self._id][name].append(value)

    return record

  def get_recorded(self, name):
    return self._ALL_RESULTS[self._id][name]


WONT_IMPLEMENT = 'apache_beam.dataframe.frame_base.WontImplementError'
NOT_IMPLEMENTED = 'NotImplementedError'


class _DeferrredDataframeOutputChecker(doctest.OutputChecker):
  """Validates output by replacing DeferredBase[...] with computed values.
  """
  def __init__(self, env, use_beam):
    self._env = env
    if use_beam:
      self.compute = self.compute_using_beam
    else:
      self.compute = self.compute_using_session
    self.reset()

  def reset(self):
    self._last_error = None

  def compute_using_session(self, to_compute):
    session = expressions.PartitioningSession(self._env._inputs)
    return {
        name: session.evaluate(frame._expr)
        for name,
        frame in to_compute.items()
    }

  def compute_using_beam(self, to_compute):
    with _InMemoryResultRecorder() as recorder:
      with beam.Pipeline() as p:
        input_pcolls = {
            placeholder: p
            | 'Create%s' % placeholder >> beam.Create([input[::2], input[1::2]])
            for placeholder,
            input in self._env._inputs.items()
        }
        output_pcolls = (
            input_pcolls | transforms._DataframeExpressionsTransform(
                {name: frame._expr
                 for name, frame in to_compute.items()}))
        for name, output_pcoll in output_pcolls.items():
          _ = output_pcoll | 'Record%s' % name >> beam.FlatMap(
              recorder.record_fn(name))
      # pipeline runs, side effects recorded

      def concat(values):
        if len(values) > 1:
          return pd.concat(values)
        else:
          return values[0]

      return {
          name: concat(recorder.get_recorded(name))
          for name in to_compute.keys()
      }

  def fix(self, want, got):
    if 'DeferredBase' in got:
      try:
        to_compute = {
            m.group(0): self._env._all_frames[int(m.group(1))]
            for m in re.finditer(r'DeferredBase\[(\d+)\]', got)
        }
        computed = self.compute(to_compute)
        for name, frame in computed.items():
          got = got.replace(name, repr(frame))

        # If a multiindex is used, compensate for it
        if any(isinstance(frame, pd.core.generic.NDFrame) and
               frame.index.nlevels > 1 for frame in computed.values()):

          def fill_multiindex(text):
            """An awful hack to work around the fact that pandas omits repeated
            elements in a multi-index.
            For example:

              Series name  Row ID
              s1           0         a
                           1         b
              s2           0         c
                           1         d
              dtype: object

            The s1 and s2 are implied for the 2nd and 4th rows. However if we
            re-order this Series it might be printed this way:

              Series name  Row ID
              s1           0         a
              s2           1         d
              s2           0         c
              s1           1         b
              dtype: object

            In our model these are equivalent, but when we sort the lines and
            check equality they are not. This method fills in any omitted
            multiindex values, so that we can successfully sort and compare."""
            lines = [list(line) for line in text.split('\n')]
            for prev, line in zip(lines[:-1], lines[1:]):
              if all(l == ' ' for l in line):
                continue

              for i, l in enumerate(line):
                if l != ' ':
                  break
                line[i] = prev[i]

            return '\n'.join(''.join(line) for line in lines)

          got = fill_multiindex(got)
          want = fill_multiindex(want)

        def sort_and_normalize(text):
          return '\n'.join(
              sorted(
                  [line.rstrip() for line in text.split('\n') if line.strip()],
                  key=str.strip)) + '\n'

        got = sort_and_normalize(got)
        want = sort_and_normalize(want)
      except Exception:
        got = traceback.format_exc()
    return want, got

  @property
  def _seen_error(self):
    return self._last_error is not None

  def check_output(self, want, got, optionflags):
    # When an error occurs check_output is called with want=example.exc_msg,
    # and got=exc_msg

    # First check if `want` is a special string indicating wont_implement_ok
    # and/or not_implemented_ok
    allowed_exceptions = want.split('|')
    if all(exc in (WONT_IMPLEMENT, NOT_IMPLEMENTED)
           for exc in allowed_exceptions):
      # If it is, check for WontImplementError and NotImplementedError
      if WONT_IMPLEMENT in allowed_exceptions and got.startswith(
          WONT_IMPLEMENT):
        self._last_error = WONT_IMPLEMENT
        return True

      elif NOT_IMPLEMENTED in allowed_exceptions and got.startswith(
          NOT_IMPLEMENTED):
        self._last_error = NOT_IMPLEMENTED
        return True

      elif got.startswith('NameError') and self._seen_error:
        # This allows us to gracefully skip tests like
        #    >>> res = df.unsupported_operation()
        #    >>> check(res)
        return True

    self.reset()
    want, got = self.fix(want, got)
    return super().check_output(want, got, optionflags)

  def output_difference(self, example, got, optionflags):
    want, got = self.fix(example.want, got)
    if want != example.want:
      example = doctest.Example(
          example.source,
          want,
          example.exc_msg,
          example.lineno,
          example.indent,
          example.options)
    return super().output_difference(example, got, optionflags)


class BeamDataframeDoctestRunner(doctest.DocTestRunner):
  """A Doctest runner suitable for replacing the `pd` module with one backed
  by beam.
  """
  def __init__(
      self,
      env,
      use_beam=True,
      wont_implement_ok=None,
      not_implemented_ok=None,
      skip=None,
      **kwargs):
    self._test_env = env

    def to_callable(cond):
      if cond == '*':
        return lambda example: True
      else:
        return lambda example: example.source.strip() == cond

    self._wont_implement_ok = {
        test: [to_callable(cond) for cond in examples]
        for test,
        examples in (wont_implement_ok or {}).items()
    }
    self._not_implemented_ok = {
        test: [to_callable(cond) for cond in examples]
        for test,
        examples in (not_implemented_ok or {}).items()
    }
    self._skip = {
        test: [to_callable(cond) for cond in examples]
        for test,
        examples in (skip or {}).items()
    }
    super().__init__(
        checker=_DeferrredDataframeOutputChecker(self._test_env, use_beam),
        **kwargs)
    self.success = 0
    self.skipped = 0
    self._reasons = collections.defaultdict(list)
    self._skipped_set = set()

  def _is_wont_implement_ok(self, example, test):
    return any(
        wont_implement(example)
        for wont_implement in self._wont_implement_ok.get(test.name, []))

  def _is_not_implemented_ok(self, example, test):
    return any(
        not_implemented(example)
        for not_implemented in self._not_implemented_ok.get(test.name, []))

  def run(self, test, **kwargs):
    self._checker.reset()
    for example in test.examples:
      if any(should_skip(example)
             for should_skip in self._skip.get(test.name, [])):
        self._skipped_set.add(example)
        example.source = 'pass'
        example.want = ''
        self.skipped += 1
      elif example.exc_msg is None:
        allowed_exceptions = []
        if self._is_not_implemented_ok(example, test):
          allowed_exceptions.append(NOT_IMPLEMENTED)
        if self._is_wont_implement_ok(example, test):
          allowed_exceptions.append(WONT_IMPLEMENT)

        if len(allowed_exceptions):
          # Don't fail doctests that raise this error.
          example.exc_msg = '|'.join(allowed_exceptions)
    with self._test_env.context():
      result = super().run(test, **kwargs)
      # Can't add attributes to builtin result.
      result = AugmentedTestResults(result.failed, result.attempted)
      result.summary = self.summary()
      return result

  def report_success(self, out, test, example, got):
    def extract_concise_reason(got, expected_exc):
      m = re.search(r"Implement(?:ed)?Error:\s+(.*)\n$", got)
      if m:
        return m.group(1)
      elif "NameError" in got:
        return "NameError following %s" % expected_exc
      elif re.match(r"DeferredBase\[\d+\]\n", got):
        return "DeferredBase[*]"
      else:
        return got.replace("\n", "\\n")

    if self._checker._last_error is not None:
      self._reasons[self._checker._last_error].append(
          extract_concise_reason(got, self._checker._last_error))

    if self._checker._seen_error:
      m = re.search('^([a-zA-Z0-9_, ]+)=', example.source)
      if m:
        for var in m.group(1).split(','):
          var = var.strip()
          if var in test.globs:
            # More informative to get a NameError than
            # use the wrong previous value.
            del test.globs[var]

    return super().report_success(out, test, example, got)

  def fake_pandas_module(self):
    return self._test_env.fake_pandas_module()

  def summarize(self):
    super().summarize()
    self.summary().summarize()

  def summary(self):
    return Summary(self.failures, self.tries, self.skipped, self._reasons)


class AugmentedTestResults(doctest.TestResults):
  pass


class Summary(object):
  def __init__(self, failures=0, tries=0, skipped=0, error_reasons=None):
    self.failures = failures
    self.tries = tries
    self.skipped = skipped
    self.error_reasons = error_reasons or collections.defaultdict(list)

  def result(self):
    res = AugmentedTestResults(self.failures, self.tries)
    res.summary = self
    return res

  def __add__(self, other):
    merged_reasons = {
        key: self.error_reasons.get(key, []) + other.error_reasons.get(key, [])
        for key in set(self.error_reasons.keys()).union(
            other.error_reasons.keys())
    }
    return Summary(
        self.failures + other.failures,
        self.tries + other.tries,
        self.skipped + other.skipped,
        merged_reasons)

  def summarize(self):
    def print_partition(indent, desc, n, total):
      print("%s%d %s (%.1f%%)" % ("  " * indent, n, desc, n / total * 100))

    print()
    print("%d total test cases:" % self.tries)

    if not self.tries:
      return

    print_partition(1, "skipped", self.skipped, self.tries)
    for error, reasons in self.error_reasons.items():
      print_partition(1, error, len(reasons), self.tries)
      reason_counts = sorted(
          collections.Counter(reasons).items(),
          key=lambda x: x[1],
          reverse=True)
      for desc, count in reason_counts:
        print_partition(2, desc, count, len(reasons))
    print_partition(1, "failed", self.failures, self.tries)
    print_partition(
        1,
        "passed",
        self.tries - self.skipped -
        sum(len(reasons)
            for reasons in self.error_reasons.values()) - self.failures,
        self.tries)
    print()


def parse_rst_ipython_tests(rst, name, extraglobs=None, optionflags=None):
  """Extracts examples from an rst file and produce a test suite by running
  them through pandas to get the expected outputs.
  """

  # Optional dependency.
  import IPython
  from traitlets.config import Config

  def get_indent(line):
    return len(line) - len(line.lstrip())

  def is_example_line(line):
    line = line.strip()
    return line and not line.startswith('#') and not line[0] == line[-1] == ':'

  IMPORT_PANDAS = 'import pandas as pd'

  example_srcs = []
  lines = iter([(lineno, line.rstrip()) for lineno,
                line in enumerate(rst.split('\n')) if is_example_line(line)] +
               [(None, 'END')])

  # https://ipython.readthedocs.io/en/stable/sphinxext.html
  lineno, line = next(lines)
  while True:
    if line == 'END':
      break
    if line.startswith('.. ipython::'):
      lineno, line = next(lines)
      indent = get_indent(line)
      example = []
      example_srcs.append((lineno, example))
      while get_indent(line) >= indent:
        if '@verbatim' in line or ':verbatim:' in line or '@savefig' in line:
          example_srcs.pop()
          break
        line = re.sub(r'In \[\d+\]: ', '', line)
        line = re.sub(r'\.\.\.+:', '', line)
        example.append(line[indent:])
        lineno, line = next(lines)
        if get_indent(line) == indent and line[indent] not in ')]}':
          example = []
          example_srcs.append((lineno, example))
    else:
      lineno, line = next(lines)

  # TODO(robertwb): Would it be better to try and detect/compare the actual
  # objects in two parallel sessions than make (stringified) doctests?
  examples = []

  config = Config()
  config.HistoryManager.hist_file = ':memory:'
  config.InteractiveShell.autocall = False
  config.InteractiveShell.autoindent = False
  config.InteractiveShell.colors = 'NoColor'

  set_pandas_options()
  IP = IPython.InteractiveShell.instance(config=config)
  IP.run_cell(IMPORT_PANDAS + '\n')
  IP.run_cell('import numpy as np\n')
  try:
    stdout = sys.stdout
    for lineno, src in example_srcs:
      sys.stdout = cout = StringIO()
      src = '\n'.join(src)
      if src == IMPORT_PANDAS:
        continue
      IP.run_cell(src + '\n')
      output = cout.getvalue()
      if output:
        # Strip the prompt.
        # TODO(robertwb): Figure out how to suppress this.
        output = re.sub(r'^Out\[\d+\]:[ \t]*\n?', '', output)
      examples.append(doctest.Example(src, output, lineno=lineno))

  finally:
    sys.stdout = stdout

  return doctest.DocTest(
      examples, dict(extraglobs or {}, np=np), name, name, None, None)


def test_rst_ipython(
    rst,
    name,
    report=False,
    wont_implement_ok=(),
    not_implemented_ok=(),
    skip=(),
    **kwargs):
  """Extracts examples from an rst file and run them through pandas to get the
  expected output, and then compare them against our dataframe implementation.
  """
  def run_tests(extraglobs, optionflags, **kwargs):
    # The patched one.
    tests = parse_rst_ipython_tests(rst, name, extraglobs, optionflags)
    runner = doctest.DocTestRunner(optionflags=optionflags)
    set_pandas_options()
    result = runner.run(tests, **kwargs)
    if report:
      runner.summarize()
    return result

  result = _run_patched(
      run_tests,
      wont_implement_ok={name: wont_implement_ok},
      not_implemented_ok={name: not_implemented_ok},
      skip={name: skip},
      **kwargs)
  return result


def teststring(text, wont_implement_ok=None, not_implemented_ok=None, **kwargs):
  return teststrings(
      {'<string>': text},
      wont_implement_ok={'<string>': ['*']} if wont_implement_ok else None,
      not_implemented_ok={'<string>': ['*']} if not_implemented_ok else None,
      **kwargs)


def teststrings(texts, report=False, **runner_kwargs):
  optionflags = runner_kwargs.pop('optionflags', 0)
  optionflags |= (
      doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)

  parser = doctest.DocTestParser()
  runner = BeamDataframeDoctestRunner(
      TestEnvironment(), optionflags=optionflags, **runner_kwargs)
  globs = {
      'pd': runner.fake_pandas_module(),
      'np': np,
      'option_context': pd.option_context,
  }
  with expressions.allow_non_parallel_operations():
    for name, text in texts.items():
      test = parser.get_doctest(text, globs, name, name, 0)
      runner.run(test)
  if report:
    runner.summarize()
  return runner.summary().result()


def set_pandas_options():
  # See
  # https://github.com/pandas-dev/pandas/blob/a00202d12d399662b8045a8dd3fdac04f18e1e55/doc/source/conf.py#L319
  np.random.seed(123456)
  np.set_printoptions(precision=4, suppress=True)
  pd.options.display.max_rows = 15


def _run_patched(func, *args, **kwargs):
  set_pandas_options()

  # https://github.com/pandas-dev/pandas/blob/1.0.x/setup.cfg#L63
  optionflags = kwargs.pop('optionflags', 0)
  optionflags |= (
      doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)

  env = TestEnvironment()
  use_beam = kwargs.pop('use_beam', True)
  skip = kwargs.pop('skip', {})
  wont_implement_ok = kwargs.pop('wont_implement_ok', {})
  not_implemented_ok = kwargs.pop('not_implemented_ok', {})
  extraglobs = dict(kwargs.pop('extraglobs', {}))
  extraglobs['pd'] = env.fake_pandas_module()

  try:
    # Unfortunately the runner is not injectable.
    original_doc_test_runner = doctest.DocTestRunner
    doctest.DocTestRunner = lambda **kwargs: BeamDataframeDoctestRunner(
        env,
        use_beam=use_beam,
        wont_implement_ok=wont_implement_ok,
        not_implemented_ok=not_implemented_ok,
        skip=skip,
        **kwargs)
    with expressions.allow_non_parallel_operations():
      return func(
          *args, extraglobs=extraglobs, optionflags=optionflags, **kwargs)
  finally:
    doctest.DocTestRunner = original_doc_test_runner


def with_run_patched_docstring(target=None):
  assert target is not None

  def wrapper(fn):
    fn.__doc__ = f"""Run all pandas doctests in the specified {target}.

    Arguments `skip`, `wont_implement_ok`, `not_implemented_ok` are all in the
    format::

      {{
         "module.Class.method": ['*'],
         "module.Class.other_method": [
           'instance.other_method(bad_input)',
           'observe_result_of_bad_input()',
         ],
      }}

    `'*'` indicates all examples should be matched, otherwise the list is a list
    of specific input strings that should be matched.

    All arguments are kwargs.

    Args:
      optionflags (int): Passed through to doctests.
      extraglobs (Dict[str,Any]): Passed through to doctests.
      use_beam (bool): If true, run a Beam pipeline with partitioned input to
        verify the examples, else use PartitioningSession to simulate
        distributed execution.
      skip (Dict[str,str]): A set of examples to skip entirely.
      wont_implement_ok (Dict[str,str]): A set of examples that are allowed to
        raise WontImplementError.
      not_implemented_ok (Dict[str,str]): A set of examples that are allowed to
        raise NotImplementedError.

    Returns:
      ~doctest.TestResults: A doctest result describing the passed/failed tests.
    """
    return fn

  return wrapper


@with_run_patched_docstring(target="file")
def testfile(*args, **kwargs):
  return _run_patched(doctest.testfile, *args, **kwargs)


@with_run_patched_docstring(target="module")
def testmod(*args, **kwargs):
  return _run_patched(doctest.testmod, *args, **kwargs)
