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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import contextlib
import doctest
import re
import traceback
from typing import Any
from typing import Dict
from typing import List

import numpy as np
import pandas as pd

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frames  # pylint: disable=unused-import
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
    if type(result) in DeferredBase._pandas_type_map.keys():
      placeholder = expressions.PlaceholderExpression(result[0:0])
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
    return FakePandasObject(pd, self)

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
  _ALL_RESULTS: Dict[str, List[Any]] = {}

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


class _DeferrredDataframeOutputChecker(doctest.OutputChecker):
  """Validates output by replacing DeferredBase[...] with computed values.
  """
  def __init__(self, env, use_beam):
    self._env = env
    if use_beam:
      self.compute = self.compute_using_beam
    else:
      self.compute = self.compute_using_session
    self._seen_wont_implement = False

  def reset(self):
    self._seen_wont_implement = False

  def compute_using_session(self, to_compute):
    session = expressions.PartitioningSession(self._env._inputs)
    return {
        name: frame._expr.evaluate_at(session)
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

  def check_output(self, want, got, optionflags):
    if got.startswith('apache_beam.dataframe.frame_base.WontImplementError'):
      self._seen_wont_implement = True
      return True
    elif got.startswith('NameError') and self._seen_wont_implement:
      # After raising WontImplementError, ignore NameErrors.
      # This allows us to gracefully skip tests like
      #    >>> res = df.unsupported_operation()
      #    >>> check(res)
      return True
    else:
      # Reset.
      self._seen_wont_implement = False
    want, got = self.fix(want, got)
    return super(_DeferrredDataframeOutputChecker,
                 self).check_output(want, got, optionflags)

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
    return super(_DeferrredDataframeOutputChecker,
                 self).output_difference(example, got, optionflags)


class BeamDataframeDoctestRunner(doctest.DocTestRunner):
  """A Doctest runner suitable for replacing the `pd` module with one backed
  by beam.
  """
  def __init__(
      self, env, use_beam=True, wont_implement_ok=None, skip=None, **kwargs):
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
    self._skip = {
        test: [to_callable(cond) for cond in examples]
        for test,
        examples in (skip or {}).items()
    }
    super(BeamDataframeDoctestRunner, self).__init__(
        checker=_DeferrredDataframeOutputChecker(self._test_env, use_beam),
        **kwargs)
    self.success = 0
    self.skipped = 0
    self.wont_implement = 0
    self._wont_implement_reasons = []
    self._skipped_set = set()

  def run(self, test, **kwargs):
    self._checker.reset()
    for example in test.examples:
      if any(should_skip(example)
             for should_skip in self._skip.get(test.name, [])):
        self._skipped_set.add(example)
        example.source = 'pass'
        example.want = ''
        self.skipped += 1
      elif example.exc_msg is None and any(
          wont_implement(example)
          for wont_implement in self._wont_implement_ok.get(test.name, [])):
        # Don't fail doctests that raise this error.
        example.exc_msg = (
            'apache_beam.dataframe.frame_base.WontImplementError: ...')
    with self._test_env.context():
      result = super(BeamDataframeDoctestRunner, self).run(test, **kwargs)
      return result

  def report_success(self, out, test, example, got):
    def extract_concise_reason(got):
      m = re.search(r"WontImplementError:\s+(.*)\n$", got)
      if m:
        return m.group(1)
      elif "NameError" in got:
        return "NameError following WontImplementError"
      elif re.match(r"DeferredBase\[\d+\]\n", got):
        return "DeferredBase[*]"
      else:
        return got.replace("\n", "\\n")

    if self._checker._seen_wont_implement:
      self.wont_implement += 1
      self._wont_implement_reasons.append(extract_concise_reason(got))

    return super(BeamDataframeDoctestRunner,
                 self).report_success(out, test, example, got)

  def fake_pandas_module(self):
    return self._test_env.fake_pandas_module()

  def summarize(self):
    super(BeamDataframeDoctestRunner, self).summarize()

    def print_partition(indent, desc, n, total):
      print("%s%d %s (%.1f%%)" % ("  " * indent, n, desc, n / total * 100))

    print()
    print("%d total test cases:" % self.tries)
    print_partition(1, "skipped", self.skipped, self.tries)
    print_partition(1, "won't implement", self.wont_implement, self.tries)
    reason_counts = sorted(
        collections.Counter(self._wont_implement_reasons).items(),
        key=lambda x: x[1],
        reverse=True)
    for desc, count in reason_counts:
      print_partition(2, desc, count, self.wont_implement)
    print_partition(
        1,
        "passed",
        self.tries - self.skipped - self.wont_implement - self.failures,
        self.tries)
    print()


def teststring(text, report=True, **runner_kwargs):
  optionflags = runner_kwargs.pop('optionflags', 0)
  optionflags |= (
      doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)

  wont_implement_ok = runner_kwargs.pop('wont_implement_ok', False)

  parser = doctest.DocTestParser()
  runner = BeamDataframeDoctestRunner(
      TestEnvironment(),
      optionflags=optionflags,
      wont_implement_ok={'<string>': ['*']} if wont_implement_ok else None,
      **runner_kwargs)
  test = parser.get_doctest(
      text, {
          'pd': runner.fake_pandas_module(), 'np': np
      },
      '<string>',
      '<string>',
      0)
  with expressions.allow_non_parallel_operations():
    result = runner.run(test)
  if report:
    runner.summarize()
  return result


def testfile(*args, **kwargs):
  return _run_patched(doctest.testfile, *args, **kwargs)


def testmod(*args, **kwargs):
  return _run_patched(doctest.testmod, *args, **kwargs)


def _run_patched(func, *args, **kwargs):
  try:
    # See
    # https://github.com/pandas-dev/pandas/blob/a00202d12d399662b8045a8dd3fdac04f18e1e55/doc/source/conf.py#L319
    np.random.seed(123456)
    np.set_printoptions(precision=4, suppress=True)
    pd.options.display.max_rows = 15

    # https://github.com/pandas-dev/pandas/blob/1.0.x/setup.cfg#L63
    optionflags = kwargs.pop('optionflags', 0)
    optionflags |= (
        doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)

    env = TestEnvironment()
    use_beam = kwargs.pop('use_beam', True)
    skip = kwargs.pop('skip', {})
    wont_implement_ok = kwargs.pop('wont_implement_ok', {})
    extraglobs = dict(kwargs.pop('extraglobs', {}))
    extraglobs['pd'] = env.fake_pandas_module()
    # Unfortunately the runner is not injectable.
    original_doc_test_runner = doctest.DocTestRunner
    doctest.DocTestRunner = lambda **kwargs: BeamDataframeDoctestRunner(
        env,
        use_beam=use_beam,
        wont_implement_ok=wont_implement_ok,
        skip=skip,
        **kwargs)
    with expressions.allow_non_parallel_operations():
      return func(
          *args, extraglobs=extraglobs, optionflags=optionflags, **kwargs)
  finally:
    doctest.DocTestRunner = original_doc_test_runner
