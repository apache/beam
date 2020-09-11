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

"""A module for running the pandas docs (such as the users guide) against our
dataframe implementation.

Run as python -m apache_beam.dataframe.pandas_docs_test [getting_started ...]
"""

from __future__ import absolute_import

import doctest
import itertools
import os
import re
import sys
import unittest
import urllib.request
import zipfile
from io import StringIO

import IPython
import numpy as np
import pandas as pd
from traitlets.config import Config

from apache_beam.dataframe import doctests

PANDAS_VERSION = '1.1.1'
PANDAS_DIR = os.path.expanduser("~/.apache_beam/cache/pandas-" + PANDAS_VERSION)
PANDAS_DOCS_SOURCE = os.path.join(PANDAS_DIR, 'doc', 'source')


def main():
  if not os.path.exists(PANDAS_DIR):
    # Download the pandas source.
    os.makedirs(os.path.dirname(PANDAS_DIR), exist_ok=True)
    zip = os.path.join(PANDAS_DIR + '.zip')
    if not os.path.exists(zip):
      url = (
          'https://github.com/pandas-dev/pandas/archive/v%s.zip' %
          PANDAS_VERSION)
      print('Downloading', url)
      with urllib.request.urlopen(url) as fin:
        with open(zip, 'wb') as fout:
          fout.write(fin.read())

    print('Extrating', zip)
    with zipfile.ZipFile(zip, 'r') as handle:
      handle.extractall(os.path.dirname(PANDAS_DIR))

  tests = sys.argv[1:] or ['getting_started', 'user_guide']
  paths = []
  filters = []

  # Explicit paths.
  for test in tests:
    if os.path.exists(test):
      paths.append(test)
    else:
      filters.append(test)

  # Names of pandas source files.
  for root, dirs, files in os.walk(PANDAS_DOCS_SOURCE):
    for name in files:
      base, ext = os.path.splitext(name)
      if ext == '.rst':
        path = os.path.join(root, name)
        if any(filter in path for filter in filters):
          paths.append(path)

  # Now run all the tests.
  running_summary = doctests.Summary()
  for path in paths:
    with open(path) as f:
      rst = f.read()
    running_summary += doctests.test_rst_ipython(
        rst, path, report=True, wont_implement_ok=['*'], use_beam=False).summary

  print('*' * 70)
  print("Final summary:")
  running_summary.summarize()


if __name__ == '__main__':
  main()
