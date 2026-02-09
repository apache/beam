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

"""Context-scoped access to pipeline options during graph construction and
translation.

This module provides thread-safe and async-safe access to PipelineOptions
using contextvars.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING
from typing import Optional

if TYPE_CHECKING:
  from apache_beam.options.pipeline_options import PipelineOptions

# The contextvar holding the current pipeline's options.
# Each thread and each asyncio task gets its own isolated copy.
_pipeline_options: ContextVar[Optional['PipelineOptions']] = ContextVar(
    'pipeline_options', default=None)


def get_pipeline_options() -> Optional['PipelineOptions']:
  """Get the current pipeline's options from the context.

  Returns:
    The PipelineOptions for the currently executing pipeline operation,
    or None if called outside of a pipeline context.
  """
  return _pipeline_options.get()


@contextmanager
def scoped_pipeline_options(options: Optional['PipelineOptions']):
  """Context manager that sets pipeline options for the duration of a block.

  Args:
    options: The PipelineOptions to make available during this scope.
  """
  token = _pipeline_options.set(options)
  try:
    yield
  finally:
    _pipeline_options.reset(token)
