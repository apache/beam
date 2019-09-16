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

from __future__ import absolute_import

import argparse
import shlex
import subprocess as sp


def parse_example(argv=None):
  """Parse the command line arguments and return it as a string function call.

  Examples:
    python path/to/snippets.py function_name
    python path/to/snippets.py function_name arg1
    python path/to/snippets.py function_name arg1 arg2 ... argN
  """
  parser = argparse.ArgumentParser()
  parser.add_argument('example', help='Name of the example to run.')
  parser.add_argument('args', nargs=argparse.REMAINDER,
                      help='Arguments for example.')
  args = parser.parse_args(argv)

  # Return the example as a string representing the Python function call.
  example_args = ', '.join([repr(arg) for arg in args.args])
  return '{}({})'.format(args.example, example_args)


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
