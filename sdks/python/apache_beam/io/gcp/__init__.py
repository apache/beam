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
from __future__ import absolute_import

import sys

# Important: the MIME library in the Python 3.x standard library used by
# apitools causes uploads containing '\r\n' to be corrupted, unless we
# patch the BytesGenerator class to write contents verbatim.
if sys.version_info[0] == 3:
  try:
    # pylint: disable=wrong-import-order, wrong-import-position
    # pylint: disable=ungrouped-imports
    import apitools.base.py.transfer as transfer
    import email.generator as email_generator

    class _WrapperNamespace(object):
      class BytesGenerator(email_generator.BytesGenerator):
        def _write_lines(self, lines):
          self.write(lines)

    transfer.email_generator = _WrapperNamespace
  except ImportError:
    # We may not have the GCP dependencies installed, so we pass in this case.
    pass
