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

import os
import tempfile
import unittest

from parameterized import param
from parameterized import parameterized

from apache_beam.utils.profiler import Profile


class ProfilerTest(unittest.TestCase):
  @parameterized.expand([
      param(enable_cpu_memory=(True, True)),
      param(enable_cpu_memory=(True, False)),
      param(enable_cpu_memory=(False, True)),
      param(enable_cpu_memory=(False, False)),
  ])
  def test_profiler(self, enable_cpu_memory):
    try:
      from guppy import hpy  # pylint: disable=unused-import
      guppy_imported = True
    except ImportError:
      guppy_imported = False

    enable_cpu, enable_memory = enable_cpu_memory

    with tempfile.TemporaryDirectory() as tmp:
      with Profile('id',
                   profile_location=tmp,
                   enable_cpu_profiling=enable_cpu,
                   enable_memory_profiling=enable_memory,
                   log_results=True):
        hash('test')
      files = os.listdir(tmp)
      if enable_cpu:
        self.assertIn('cpu_profile', files)
      else:
        self.assertNotIn('cpu_profile', files)

      if enable_memory and guppy_imported:
        self.assertIn('memory_profile', files)
      else:
        self.assertNotIn('memory_profile', files)


if __name__ == '__main__':
  unittest.main()
