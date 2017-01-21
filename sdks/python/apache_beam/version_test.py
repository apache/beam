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
"""Unit tests for the version module."""

import unittest
from apache_beam import version as beam_version


class Version(unittest.TestCase):

  def test_version(self):
    # Test that version is processed from the external file and cached.
    self.assertIsNone(beam_version.__version__)
    self.assertIsNotNone(beam_version.get_version())
    self.assertEqual(beam_version.get_version(), beam_version.__version__)


if __name__ == '__main__':
  unittest.main()
