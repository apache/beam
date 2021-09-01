#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Utilities to be used in  Interactive Beam.
"""

import functools
import hashlib
import json
import logging

import pandas as pd

from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.frame_base import DeferredBase
from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload
from apache_beam.runners.interactive.caching.expression_cache import ExpressionCache
from apache_beam.testing.test_stream import WindowedValueHolder
from apache_beam.typehints.schemas import named_fields_from_element_type

_LOGGER = logging.getLogger(__name__)

# Add line breaks to the IPythonLogHandler's HTML output.
_INTERACTIVE_LOG_STYLE = """
  <style>
    div.alert {
      white-space: pre-line;
    }
  </style>
"""


def to_element_list(
    reader,  # type: Generator[Union[TestStreamPayload.Event, WindowedValueHolder]]
    coder,  # type: Coder
    include_window_info,  # type: bool
    n=None,  # type: int
    include_time_events=False, # type: bool
):
  # type: (...) -> List[WindowedValue]

  """Returns an iterator that properly decodes the elements from the reader.
  """

  # Defining a generator like this makes it easier to limit the count of
  # elements read. Otherwise, the count limit would need to be duplicated.
  def elements():
    for e in reader:
      if isinstance(e, TestStreamPayload.Event):
        if (e.HasField('watermark_event') or
            e.HasField('processing_time_event')):
          if include_time_events:
            yield e
        else:
          for tv in e.element_event.elements:
            decoded = coder.decode(tv.encoded_element)
            yield (
                decoded.windowed_value
                if include_window_info else decoded.windowed_value.value)
      elif isinstance(e, WindowedValueHolder):
        yield (
            e.windowed_value if include_window_info else e.windowed_value.value)
      else:
        yield e

  # Because we can yield multiple elements from a single TestStreamFileRecord,
  # we have to limit the count here to ensure that `n` is fulfilled.
  count = 0
  for e in elements():
    if n and count >= n:
      break

    yield e

    if not isinstance(e, TestStreamPayload.Event):
      count += 1


def elements_to_df(elements, include_window_info=False, element_type=None):
  # type: (List[WindowedValue], bool, Any) -> DataFrame

  """Parses the given elements into a Dataframe.

  If the elements are a list of WindowedValues, then it will break out the
  elements into their own DataFrame and return it. If include_window_info is
  True, then it will concatenate the windowing information onto the elements
  DataFrame.
  """
  try:
    columns_names = [
        name for name, _ in named_fields_from_element_type(element_type)
    ]
  except TypeError:
    columns_names = None

  rows = []
  windowed_info = []
  for e in elements:
    rows.append(e.value)
    if include_window_info:
      windowed_info.append([e.timestamp.micros, e.windows, e.pane_info])

  using_dataframes = isinstance(element_type, pd.DataFrame)
  using_series = isinstance(element_type, pd.Series)
  if using_dataframes or using_series:
    rows_df = pd.concat(rows)
  else:
    rows_df = pd.DataFrame(rows, columns=columns_names)

  if include_window_info and not using_series:
    windowed_info_df = pd.DataFrame(
        windowed_info, columns=['event_time', 'windows', 'pane_info'])
    final_df = pd.concat([rows_df, windowed_info_df], axis=1)
  else:
    final_df = rows_df

  return final_df


def register_ipython_log_handler():
  # type: () -> None

  """Adds the IPython handler to a dummy parent logger (named
  'apache_beam.runners.interactive') of all interactive modules' loggers so that
  if is_in_notebook, logging displays the logs as HTML in frontends.
  """

  # apache_beam.runners.interactive is not a module, thus this "root" logger is
  # a dummy one created to hold the IPython log handler. When children loggers
  # have propagate as True (by default) and logging level as NOTSET (by default,
  # so the "root" logger's logging level takes effect), the IPython log handler
  # will be triggered at the "root"'s own logging level. And if a child logger
  # sets its logging level, it can take control back.
  interactive_root_logger = logging.getLogger('apache_beam.runners.interactive')
  if any([isinstance(h, IPythonLogHandler)
          for h in interactive_root_logger.handlers]):
    return
  interactive_root_logger.setLevel(logging.INFO)
  interactive_root_logger.addHandler(IPythonLogHandler())
  # Disable the propagation so that logs emitted from interactive modules should
  # only be handled by loggers and handlers defined within interactive packages.
  interactive_root_logger.propagate = False


class IPythonLogHandler(logging.Handler):
  """A logging handler to display logs as HTML in IPython backed frontends."""
  # TODO(BEAM-7923): Switch to Google hosted CDN once
  # https://code.google.com/archive/p/google-ajax-apis/issues/637 is resolved.
  log_template = """
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">
            <div class="alert alert-{level}">{msg}</div>"""

  logging_to_alert_level_map = {
      logging.CRITICAL: 'danger',
      logging.ERROR: 'danger',
      logging.WARNING: 'warning',
      logging.INFO: 'info',
      logging.DEBUG: 'dark',
      logging.NOTSET: 'light'
  }

  def emit(self, record):
    try:
      from html import escape
      from IPython.core.display import HTML
      from IPython.core.display import display
      display(HTML(_INTERACTIVE_LOG_STYLE))
      display(
          HTML(
              self.log_template.format(
                  level=self.logging_to_alert_level_map[record.levelno],
                  msg=escape(record.msg % record.args))))
    except ImportError:
      pass  # NOOP when dependencies are not available.


def obfuscate(*inputs):
  # type: (*Any) -> str

  """Obfuscates any inputs into a hexadecimal string."""
  str_inputs = [str(input) for input in inputs]
  merged_inputs = '_'.join(str_inputs)
  return hashlib.md5(merged_inputs.encode('utf-8')).hexdigest()


class ProgressIndicator(object):
  """An indicator visualizing code execution in progress."""
  # TODO(BEAM-7923): Switch to Google hosted CDN once
  # https://code.google.com/archive/p/google-ajax-apis/issues/637 is resolved.
  spinner_template = """
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css" integrity="sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh" crossorigin="anonymous">
            <div id="{id}" class="spinner-border text-info" role="status">
            </div>"""
  spinner_removal_template = """
            $("#{id}").remove();"""

  def __init__(self, enter_text, exit_text):
    # type: (str, str) -> None

    self._id = 'progress_indicator_{}'.format(obfuscate(id(self)))
    self._enter_text = enter_text
    self._exit_text = exit_text

  def __enter__(self):
    try:
      from IPython.core.display import HTML
      from IPython.core.display import display
      from apache_beam.runners.interactive import interactive_environment as ie
      if ie.current_env().is_in_notebook:
        display(HTML(self.spinner_template.format(id=self._id)))
      else:
        display(self._enter_text)
    except ImportError as e:
      _LOGGER.error(
          'Please use interactive Beam features in an IPython'
          'or notebook environment: %s' % e)

  def __exit__(self, exc_type, exc_value, traceback):
    try:
      from IPython.core.display import Javascript
      from IPython.core.display import display
      from IPython.core.display import display_javascript
      from apache_beam.runners.interactive import interactive_environment as ie
      if ie.current_env().is_in_notebook:
        script = self.spinner_removal_template.format(id=self._id)
        display_javascript(
            Javascript(
                ie._JQUERY_WITH_DATATABLE_TEMPLATE.format(
                    customized_script=script)))
      else:
        display(self._exit_text)
    except ImportError as e:
      _LOGGER.error(
          'Please use interactive Beam features in an IPython'
          'or notebook environment: %s' % e)


def progress_indicated(func):
  # type: (Callable[..., Any]) -> Callable[..., Any]

  """A decorator using a unique progress indicator as a context manager to
  execute the given function within."""
  @functools.wraps(func)
  def run_within_progress_indicator(*args, **kwargs):
    with ProgressIndicator('Processing...', 'Done.'):
      return func(*args, **kwargs)

  return run_within_progress_indicator


def as_json(func):
  # type: (Callable[..., Any]) -> Callable[..., str]

  """A decorator convert python objects returned by callables to json
  string.

  The decorated function should always return an object parsable by json.dumps.
  If the object is not parsable, the str() of original object is returned
  instead.
  """
  def return_as_json(*args, **kwargs):
    try:
      return_value = func(*args, **kwargs)
      return json.dumps(return_value)
    except TypeError:
      return str(return_value)

  return return_as_json


def deferred_df_to_pcollection(df):
  assert isinstance(df, DeferredBase), '{} is not a DeferredBase'.format(df)

  # The proxy is used to output a DataFrame with the correct columns.
  #
  # TODO(BEAM-11064): Once type hints are implemented for pandas, use those
  # instead of the proxy.
  cache = ExpressionCache()
  cache.replace_with_cached(df._expr)

  proxy = df._expr.proxy()
  return to_pcollection(df, yield_elements='pandas', label=str(df._expr)), proxy
