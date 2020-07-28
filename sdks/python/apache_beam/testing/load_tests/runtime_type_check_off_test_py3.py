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

"""
For more information, see documentation in the parent class: BaseRunTimeTypeCheckTest.

Example test run:

python -m apache_beam.testing.load_tests.runtime_type_check_off_test_py3 \
    --test-pipeline-options="
    --project=apache-beam-testing
    --region=us-centrall
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=gbk
    --nested_typehint=0
    --fanout=200
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"
"""

# pytype: skip-file

import logging

from apache_beam.testing.load_tests.base_runtime_type_check_test_py3 import BaseRunTimeTypeCheckTest


class RunTimeTypeCheckOffTest(BaseRunTimeTypeCheckTest):
  def __init__(self):
    self.runtime_type_check = False
    super(RunTimeTypeCheckOffTest, self).__init__()


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  RunTimeTypeCheckOffTest().run()
