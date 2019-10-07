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

from apache_beam.utils.timestamp import Timestamp


class WatermarkReporter(object):
  """An abstraction that should implement the DoFn.WatermarkReporterParam.

  The WatermarkReporter is an abstraction that will replace the
  ``DoFn.WatermarkReporterParam``. When called, the implementation should use
  the value to give the runner a (best-effort) lower bound about the timestamps
  of future output associated with the current element processing by the
  ``DoFn``.
  """

  def __call__(self, value):
    """Sets the best-effort lower bound watermark.

    If there are multiple outputs, the watermark applies to all of them.

    Args:
        value: the watermark represented as an int or float in seconds, or an
               ``apache_beam.utils.timestamp.Timestamp``.
    """
    pass
