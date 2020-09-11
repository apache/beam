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

# This is here to populate PANDAS_DIR early when called manually.

if __name__ == '__main__':
  if not os.path.exists(PANDAS_DIR):
    os.makedirs(os.path.dirname(PANDAS_DIR), exist_ok=True)
    zip = os.path.join(PANDAS_DIR + '.zip')
    if not os.path.exists(zip):
      url = 'https://github.com/pandas-dev/pandas/archive/v%s.zip' % PANDAS_VERSION
      print('Downloading', url)
      with urllib.request.urlopen(url) as fin:
        with open(zip, 'wb') as fout:
          fout.write(fin.read())

    print('Extrating', zip)
    with zipfile.ZipFile(zip, 'r') as handle:
      handle.extractall(os.path.dirname(PANDAS_DIR))


@unittest.skipIf(sys.version_info <= (3, ), 'Requires contextlib.ExitStack.')
@unittest.skipIf(
    not os.path.exists(PANDAS_DIR),
    'Run python -m apache_beam.dataframe.pandas_docs_test to download pandas tests.'
)
class PandasDocsTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    unittest.TestCase.setUpClass()
    cls._running_summary = doctests.Summary()

  @classmethod
  def tearDownClass(cls):
    print("Final summary:")
    cls._running_summary.summarize()
    unittest.TestCase.tearDownClass()

  def _run_rst_tests(
      self, path, report=True, wont_implement_ok=['*'], skip=[], **kwargs):

    with open(os.path.join(PANDAS_DIR, 'doc', 'source', *path.split('/'))) as f:
      rst = f.read()

    result = doctests.test_rst_ipython(
        rst, path, report=report, wont_implement_ok=wont_implement_ok, **kwargs)
    type(self)._running_summary += result.summary
    self.assertEqual(result.failed, 0)

  # TODO(robertwb): Auto-generate?
  def test_10min(self):
    self._run_rst_tests('user_guide/10min.rst', use_beam=False)

  def test_basics(self):
    self._run_rst_tests('user_guide/basics.rst', use_beam=False)

  def test_groupby(self):
    self._run_rst_tests('user_guide/groupby.rst', use_beam=False)


if __name__ == '__main__':
  unittest.main()
