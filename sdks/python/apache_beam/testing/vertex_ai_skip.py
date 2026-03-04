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

"""Centralized skip for Vertex AI integration tests when dependencies are missing.

Test modules use skip_if_vertex_ai_disabled on classes that require the Vertex AI
Python SDK to be installed.
"""

import pytest


def _is_vertex_ai_available() -> bool:
  """Return True if Vertex AI client dependencies are importable."""
  try:
    import vertexai  # type: ignore[import-not-found]  # pylint: disable=unused-import
  except ImportError:
    return False
  return True


skip_if_vertex_ai_disabled = pytest.mark.skipif(
    not _is_vertex_ai_available(),
    reason='Vertex AI dependencies not available.')
