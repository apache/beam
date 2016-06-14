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
"""Utilities for dealing with file paths."""

import os


def join(path, *paths):
  """Joins given path pieces with the appropriate separator.

  This function is useful for joining parts of a path that could at times refer
  to either a GCS path or a local path.  In particular, this is useful for
  ensuring Windows compatibility as on Windows, the GCS path separator is
  different from the separator for local paths.

  Use os.path.join instead if a path always refers to a local path.

  Args:
    path: First part of path to join.  If this part starts with 'gs:/', the GCS
      separator will be used in joining this path.
    *paths: Remaining part(s) of path to join.

  Returns:
    Pieces joined by the appropriate path separator.
  """
  if path.startswith('gs:/'):
    # Note that we explicitly choose not to use posixpath.join() here, since
    # that function has the undesirable behavior of having, for example,
    # posixpath.join('gs://bucket/path', '/to/file') return '/to/file' instead
    # of the slightly less surprising result 'gs://bucket/path//to/file'.
    return '/'.join((path,) + paths)
  else:
    return os.path.join(path, *paths)
