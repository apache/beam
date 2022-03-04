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
from datetime import datetime

from test_helper import failed, passed, get_file_output, test_is_not_empty


def test_output():
  answers = ["1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "1, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)",
             "0, window(start=2021-03-01T00:00:00Z, end=2021-03-02T00:00:00Z)"]

  output = get_file_output()

  if all(elem in output for elem in answers) and all(elem in answers for elem in output):
    passed()
  else:
    failed("Try using an early trigger with the AfterWatermark trigger.")


if __name__ == '__main__':
  test_is_not_empty()
  test_output()
