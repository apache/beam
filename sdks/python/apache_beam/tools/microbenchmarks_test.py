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

"""Unit tests for microbenchmarks code."""

# pytype: skip-file

import unittest
from importlib.metadata import PackageNotFoundError
from importlib.metadata import distribution

from apache_beam.tools import coders_microbenchmark
from apache_beam.tools import utils


class MicrobenchmarksTest(unittest.TestCase):
  def test_coders_microbenchmark(self):
    # Right now, we don't evaluate performance impact, only check that
    # microbenchmark code can successfully run.
    coders_microbenchmark.run_coder_benchmarks(
        num_runs=1, input_size=10, seed=1, verbose=False)

  def is_cython_installed(self):
    try:
      distribution('cython')
      return True
    except PackageNotFoundError:
      return False

  def test_check_compiled(self):
    if self.is_cython_installed():
      utils.check_compiled('apache_beam.runners.worker.opcounters')
    # Unfortunately, if cython is not installed, that doesn't mean we
    # can rule out compiled modules (e.g. if Beam was installed from a wheel).
    # Technically the other way around could be true as well, e.g. if
    # Cython was installed after Beam, but this is rarer and more easily
    # remedied.


if __name__ == '__main__':
  unittest.main()
