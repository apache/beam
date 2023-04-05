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

"""Test runner objects that's only for end-to-end tests.

This package defines runners, which are used to execute test pipeline and
verify results.
"""

# Protect against environments where dataflow runner is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.test_dataflow_runner import TestDataflowRunner
  from apache_beam.runners.direct.test_direct_runner import TestDirectRunner
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position
