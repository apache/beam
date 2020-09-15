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

    numbers_greater_than_100 = ['110', '150', '250']
    remaining_numbers = ['1', '2', '3', '4', '5', '100']

    answers = []

    for num in numbers_greater_than_100:
        answers.append('Number > 100: ' + num)

    for num in remaining_numbers:
        answers.append('Number <= 100: ' + num)

    if all(num in output for num in answers):
        passed()
    else:
        failed("Incorrect output. Partition the numbers accordingly.")


if __name__ == '__main__':
    test_is_not_empty()
    test_output()
