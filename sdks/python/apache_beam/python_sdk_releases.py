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

"""Descriptions of the versions of the SDK.

This manages the features and tests supported by different versions of the
Dataflow SDK for Python.

To add feature 'foo' to a particular release, add a 'properties' value with
'feature_foo': True. To remove feature 'foo' from a particular release, add a
'properties' value with 'feature_foo': False. Features are cumulative and can
be added and removed multiple times.

By default, all tests are enabled. To remove test 'bar' from a particular
release, add a 'properties' value with 'test_bar': False. To add it back to a
subsequent release, add a 'properties' value with 'test_bar': True. Tests are
cumulative and can be removed and added multiple times.

See go/dataflow-testing for more information.
"""

OLDEST_SUPPORTED_PYTHON_SDK = 'python-0.1.4'

RELEASES = [
    {'name': 'python-0.2.7',},
    {'name': 'python-0.2.6',},
    {'name': 'python-0.2.5',},
    {'name': 'python-0.2.4',},
    {'name': 'python-0.2.3',},
    {'name': 'python-0.2.2',},
    {'name': 'python-0.2.1',},
    {'name': 'python-0.2.0',},
    {'name': 'python-0.1.5',},
    {'name': 'python-0.1.4',},
    {'name': 'python-0.1.3',},
    {'name': 'python-0.1.2',},
    {'name': 'python-0.1.1',
     'properties': {
         'feature_python': True,
     }
    },
]
