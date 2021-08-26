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

# pytype: skip-file

import ast
import shlex
import subprocess as sp

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def assert_matches_stdout(
    actual, expected_stdout, normalize_fn=lambda elem: elem, label=''):
  """Asserts a PCollection of strings matches the expected stdout elements.

  Args:
    actual (beam.PCollection): A PCollection.
    expected (List[str]): A list of stdout elements, one line per element.
    normalize_fn (Function[any]): A function to normalize elements before
        comparing them. Can be used to sort lists before comparing.
    label (str): [optional] Label to make transform names unique.
  """
  def stdout_to_python_object(elem_str):
    try:
      elem = ast.literal_eval(elem_str)
    except (SyntaxError, ValueError):
      elem = elem_str
    return normalize_fn(elem)

  actual = actual | label >> beam.Map(stdout_to_python_object)
  expected = list(map(stdout_to_python_object, expected_stdout))
  assert_that(actual, equal_to(expected), 'assert ' + label)


def run_shell_commands(commands, **kwargs):
  """Runs a list of Notebook-like shell commands.

  Lines starting with `#` are ignored as comments.
  Lines starting with `!` are run as commands.
  Variables like `{variable}` are substituted with **kwargs.
  """
  for cmd in commands:
    cmd = cmd.strip().lstrip('!').format(**kwargs)
    sp_cmd = shlex.split(cmd, comments=True, posix=True)
    if sp_cmd:
      sp.call(sp_cmd)
      yield sp_cmd
