#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from test_helper import failed, passed, get_file_output, test_is_not_empty


def test_output():
    output = get_file_output()

    answers = [
        'Person[Henry,Singapore,Singapore]',
        'Person[Jane,San Francisco,United States]',
        'Person[Lee,Beijing,China]',
        'Person[John,Sydney,Australia]',
        'Person[Alfred,London,United Kingdom]'
    ]

    if all(person in output for person in answers):
        passed()
    else:
        failed("Incorrect output. Enrich the Person's country by the city.")


if __name__ == '__main__':
    test_is_not_empty()
    test_output()
