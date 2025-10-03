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

import unittest

from apache_beam.typehints import decorators_test
from apache_beam.typehints import typehints_test
from apache_beam.typehints.testing import typehints_test_generator


class TestTypehintTestsInSync(unittest.TestCase):
  def test_generated_file_is_up_to_date(self):
    typehints_test_generator.check_generated_file(typehints_test.__file__)
    typehints_test_generator.check_generated_file(decorators_test.__file__)


if __name__ == '__main__':
  unittest.main()
