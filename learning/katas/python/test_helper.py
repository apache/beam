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

import sys
import subprocess


def get_file_text(path):
    """Returns file text by path."""
    file_io = open(path, "r")
    text = file_io.read()
    file_io.close()
    return text


def get_file_output(path=sys.argv[-1], arg_string="", encoding="utf-8", ):
    """
    Returns answer file output.

    :param path: path of file to execute
    :param arg_string: arguments to be passed to the script
    :param encoding: to decode output in python3
    :return: list of strings
    """
    proc = subprocess.Popen([sys.executable, path], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    if arg_string:
        for arg in arg_string.split("\n"):
            proc.stdin.write(bytearray(str(arg) + "\n", encoding))
            proc.stdin.flush()

    return list(map(lambda x: str(x.decode(encoding)), proc.communicate()[0].splitlines()))


def test_is_not_empty() -> bool:
    """Checks that file is not empty."""
    path = sys.argv[-1]
    file_text = get_file_text(path)

    if len(file_text) > 0:
        return True
    else:
        return False
