#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from test_helper import failed, passed, get_file_output, test_is_not_empty


def test_output():
    output = get_file_output()

    answers = [
        'apple:2',
        'banana:4',
        'grape:1',
        'orange:2',
        'papaya:1'
    ]

    if all(wordCount in output for wordCount in answers):
        passed()
    else:
        failed('Incorrect output. Count number of each word in "word:count" format.')


if __name__ == '__main__':
    test_is_not_empty()
    test_output()
