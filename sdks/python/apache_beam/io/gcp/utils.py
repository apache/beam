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

# define shared variables needed for multiple tests.
# test modules are not importable by each other. So we need to define the
# constants here instead of importing the same constants from
# different test files.
# https://docs.pytest.org/en/7.1.x/explanation/pythonpath.html

_DESTINATION_ELEMENT_PAIRS = [
    # DESTINATION 1
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'py'
    }),
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'java'
    }),
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'go'
    }),
    ('project1:dataset1.table1', {
        'name': 'flink', 'language': 'java'
    }),
    ('project1:dataset1.table1', {
        'name': 'flink', 'language': 'scala'
    }),

    # DESTINATION 3
    ('project1:dataset1.table3', {
        'name': 'spark', 'language': 'scala'
    }),

    # DESTINATION 1
    ('project1:dataset1.table1', {
        'name': 'spark', 'language': 'py'
    }),
    ('project1:dataset1.table1', {
        'name': 'spark', 'language': 'scala'
    }),

    # DESTINATION 2
    ('project1:dataset1.table2', {
        'name': 'beam', 'foundation': 'apache'
    }),
    ('project1:dataset1.table2', {
        'name': 'flink', 'foundation': 'apache'
    }),
    ('project1:dataset1.table2', {
        'name': 'spark', 'foundation': 'apache'
    }),
]

_ELEMENTS = [elm[1] for elm in _DESTINATION_ELEMENT_PAIRS]
