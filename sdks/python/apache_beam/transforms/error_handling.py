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

"""Utilities for gracefully handling errors and excluding bad elements."""

import traceback

from apache_beam import transforms


class ErrorHandler:
  """ErrorHandlers are used to skip and otherwise process bad records.

  Error handlers allow one to implement the "dead letter queue" pattern in
  a fluent manner, disaggregating the error processing specification from
  the main processing chain.

  This is typically used as follows::

    with error_handling.ErrorHandler(WriteToSomewhere(...)) as error_handler:
      result = pcoll | SomeTransform().with_error_handler(error_handler)

  in which case errors encountered by `SomeTransform()`` in processing pcoll
  will be written by the PTransform `WriteToSomewhere(...)` and excluded from
  `result` rather than failing the pipeline.

  To implement `with_error_handling` on a PTransform, one caches the provided
  error handler for use in `expand`.  During `expand()` one can invoke
  `error_handler.add_error_pcollection(...)` any number of times with
  PCollections containing error records to be processed by the given error
  handler, or (if applicable) simply invoke `with_error_handling(...)` on any
  subtransforms.

  The `with_error_handling` should accept `None` to indicate that error handling
  is not enabled (and make implementation-by-forwarding-error-handlers easier).
  In this case, any non-recoverable errors should fail the pipeline (e.g.
  propagate exceptions in `process` methods) rather than silently ignore errors.
  """
  def __init__(self, consumer):
    self._consumer = consumer
    self._creation_traceback = traceback.format_stack()[-2]
    self._error_pcolls = []
    self._closed = False

  def __enter__(self):
    self._error_pcolls = []
    self._closed = False
    return self

  def __exit__(self, *exec_info):
    if exec_info[0] is None:
      self.close()

  def close(self):
    """Indicates all error-producing operations have reported any errors.

    Invokes the provided error consuming PTransform on any provided error
    PCollections.
    """
    self._output = (
        tuple(self._error_pcolls) | transforms.Flatten() | self._consumer)
    self._closed = True

  def output(self):
    """Returns result of applying the error consumer to the error pcollections.
    """
    if not self._closed:
      raise RuntimeError(
          "Cannot access the output of an error handler "
          "until it has been closed.")
    return self._output

  def add_error_pcollection(self, pcoll):
    """Called by a class implementing error handling on the error records.
    """
    pcoll.pipeline._register_error_handler(self)
    self._error_pcolls.append(pcoll)

  def verify_closed(self):
    """Called at end of pipeline construction to ensure errors are not ignored.
    """
    if not self._closed:
      raise RuntimeError(
          "Unclosed error handler initialized at %s" % self._creation_traceback)


class _IdentityPTransform(transforms.PTransform):
  def expand(self, pcoll):
    return pcoll


class CollectingErrorHandler(ErrorHandler):
  """An ErrorHandler that simply collects all errors for further processing.

  This ErrorHandler requires the set of errors be retrieved via `output()`
  and consumed (or explicitly discarded).
  """
  def __init__(self):
    super().__init__(_IdentityPTransform())
    self._creation_traceback = traceback.format_stack()[-2]
    self._output_accessed = False

  def output(self):
    self._output_accessed = True
    return super().output()

  def verify_closed(self):
    if not self._output_accessed:
      raise RuntimeError(
          "CollectingErrorHandler requires the output to be retrieved. "
          "Initialized at %s" % self._creation_traceback)
    return super().verify_closed()
