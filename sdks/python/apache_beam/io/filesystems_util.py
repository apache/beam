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
"""Utility functions for getting the correct file systems for a file name"""

from apache_beam.io.localfilesystem import LocalFileSystem


# TODO(BEAM-1585): Add a mechanism to add user implemented file systems
def get_filesystem(path):
  """Function that returns the FileSystem class to use based on the path
  provided in the input.
  """
  if path.startswith('gs://'):
    try:
      from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
    except ImportError:
      raise ImportError(
          'Google Cloud Platform IO not available, '
          'please install apache_beam[gcp]')
    return GCSFileSystem()
  else:
    return LocalFileSystem()
