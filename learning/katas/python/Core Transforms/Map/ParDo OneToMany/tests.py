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

from test_helper import failed, passed, \
    get_answer_placeholders, get_file_output, test_is_not_empty, \
    test_answer_placeholders_text_deleted


def test_dofn_process_method():
    placeholders = get_answer_placeholders()
    placeholder = placeholders[0]

    if 'def process(self, element' in placeholder:
        passed()
    else:
        failed('Override "process" method')


def test_pardo():
    placeholders = get_answer_placeholders()
    placeholder = placeholders[1]

    if 'beam.ParDo(BreakIntoWordsDoFn())' in placeholder:
        passed()
    else:
        failed('Use beam.ParDo')


def test_output():
    output = get_file_output()

    answers = ['Hello', 'Beam', 'It', 'is', 'awesome']

    if all(num in output for num in answers):
        passed()
    else:
        failed("Incorrect output. Break each sentence into words.")


if __name__ == '__main__':
    test_is_not_empty()
    test_answer_placeholders_text_deleted()
    test_dofn_process_method()
    test_pardo()
    test_output()
