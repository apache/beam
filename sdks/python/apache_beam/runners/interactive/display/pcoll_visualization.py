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

"""Module visualizes PCollection data.

For internal use only; no backwards-compatibility guarantees.
Only works with Python 3.5+.
"""
# pytype: skip-file

import base64
import datetime
import html
import logging
import warnings
from datetime import timedelta
from typing import Optional

from dateutil import tz

import apache_beam as beam
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.utils import elements_to_df
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow

try:
  from IPython import get_ipython  # pylint: disable=import-error
  from IPython.display import HTML  # pylint: disable=import-error
  from IPython.display import Javascript  # pylint: disable=import-error
  from IPython.display import display  # pylint: disable=import-error
  from IPython.display import display_javascript  # pylint: disable=import-error
  from facets_overview.generic_feature_statistics_generator import GenericFeatureStatisticsGenerator  # pylint: disable=import-error
  from timeloop import Timeloop  # pylint: disable=import-error

  if get_ipython():
    _pcoll_visualization_ready = True
  else:
    _pcoll_visualization_ready = False
except ImportError:
  _pcoll_visualization_ready = False

_LOGGER = logging.getLogger(__name__)

_CSS = """
            <style>
            .p-Widget.jp-OutputPrompt.jp-OutputArea-prompt:empty {{
              padding: 0;
              border: 0;
            }}
            .p-Widget.jp-RenderedJavaScript.jp-mod-trusted.jp-OutputArea-output:empty {{
              padding: 0;
              border: 0;
            }}
            </style>"""
_DIVE_SCRIPT_TEMPLATE = """
            try {{
              document
                .getElementById("{display_id}")
                .contentDocument
                .getElementById("{display_id}")
                .data = {jsonstr};
            }} catch (e) {{
              // NOOP when the user has cleared the output from the notebook.
            }}"""
_DIVE_HTML_TEMPLATE = _CSS + """
            <iframe id={display_id} style="border:none" width="100%" height="600px"
              srcdoc='
                <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
                <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html">
                <facets-dive sprite-image-width="{sprite_size}" sprite-image-height="{sprite_size}" id="{display_id}" height="600"></facets-dive>
                <script>
                  document.getElementById("{display_id}").data = {jsonstr};
                </script>
              '>
            </iframe>"""
_OVERVIEW_SCRIPT_TEMPLATE = """
              try {{
                document
                  .getElementById("{display_id}")
                  .contentDocument
                  .getElementById("{display_id}")
                  .protoInput = "{protostr}";
              }} catch (e) {{
                // NOOP when the user has cleared the output from the notebook.
              }}"""
_OVERVIEW_HTML_TEMPLATE = _CSS + """
            <iframe id={display_id} style="border:none" width="100%" height="600px"
              srcdoc='
                <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
                <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html">
                <facets-overview id="{display_id}"></facets-overview>
                <script>
                  document.getElementById("{display_id}").protoInput = "{protostr}";
                </script>
              '>
            </iframe>"""
_DATATABLE_INITIALIZATION_CONFIG = """
            bAutoWidth: false,
            columns: {columns},
            destroy: true,
            responsive: true,
            columnDefs: [
              {{
                targets: "_all",
                className: "dt-left"
              }},
              {{
                "targets": 0,
                "width": "10px",
                "title": ""
              }}
            ]"""
_DATAFRAME_SCRIPT_TEMPLATE = """
            var dt;
            if ($.fn.dataTable.isDataTable("#{table_id}")) {{
              dt = $("#{table_id}").dataTable();
            }} else if ($("#{table_id}_wrapper").length == 0) {{
              dt = $("#{table_id}").dataTable({{
                """ + _DATATABLE_INITIALIZATION_CONFIG + """
              }});
            }} else {{
              return;
            }}
            dt.api()
              .clear()
              .rows.add({data_as_rows})
              .draw('full-hold');"""
_DATAFRAME_PAGINATION_TEMPLATE = _CSS + """
            <link rel="stylesheet" href="https://cdn.datatables.net/1.10.20/css/jquery.dataTables.min.css">
            <table id="{table_id}" class="display" style="display:block"></table>
            <script>
              {script_in_jquery_with_datatable}
            </script>"""
_NO_DATA_TEMPLATE = _CSS + """
            <div id="no_data_{id}">No data to display.</div>"""
_NO_DATA_REMOVAL_SCRIPT = """
            $("#no_data_{id}").remove();"""


def visualize(
    stream,
    dynamic_plotting_interval=None,
    include_window_info=False,
    display_facets=False,
    element_type=None):
  """Visualizes the data of a given PCollection. Optionally enables dynamic
  plotting with interval in seconds if the PCollection is being produced by a
  running pipeline or the pipeline is streaming indefinitely. The function
  always returns immediately and is asynchronous when dynamic plotting is on.

  If dynamic plotting enabled, the visualization is updated continuously until
  the pipeline producing the PCollection is in an end state. The visualization
  would be anchored to the notebook cell output area. The function
  asynchronously returns a handle to the visualization job immediately. The user
  could manually do::

    # In one notebook cell, enable dynamic plotting every 1 second:
    handle = visualize(pcoll, dynamic_plotting_interval=1)
    # Visualization anchored to the cell's output area.
    # In a different cell:
    handle.stop()
    # Will stop the dynamic plotting of the above visualization manually.
    # Otherwise, dynamic plotting ends when pipeline is not running anymore.

  If dynamic_plotting is not enabled (by default), None is returned.

  If include_window_info is True, the data will include window information,
  which consists of the event timestamps, windows, and pane info.

  If display_facets is True, the facets widgets will be rendered. Otherwise, the
  facets widgets will not be rendered.

  The function is experimental. For internal use only; no
  backwards-compatibility guarantees.
  """
  if not _pcoll_visualization_ready:
    return None
  pv = PCollectionVisualization(
      stream,
      include_window_info=include_window_info,
      display_facets=display_facets,
      element_type=element_type)
  if ie.current_env().is_in_notebook:
    pv.display()
  else:
    pv.display_plain_text()
    # We don't want to do dynamic plotting if there is no notebook frontend.
    return None

  if dynamic_plotting_interval:
    # Disables the verbose logging from timeloop.
    logging.getLogger('timeloop').disabled = True
    tl = Timeloop()

    def dynamic_plotting(stream, pv, tl, include_window_info, display_facets):
      @tl.job(interval=timedelta(seconds=dynamic_plotting_interval))
      def continuous_update_display():  # pylint: disable=unused-variable
        # Always creates a new PCollVisualization instance when the
        # PCollection materialization is being updated and dynamic
        # plotting is in-process.
        # PCollectionVisualization created at this level doesn't need dynamic
        # plotting interval information when instantiated because it's already
        # in dynamic plotting logic.
        updated_pv = PCollectionVisualization(
            stream,
            include_window_info=include_window_info,
            display_facets=display_facets,
            element_type=element_type)
        updated_pv.display(updating_pv=pv)

        # Stop updating the visualizations as soon as the stream will not yield
        # new elements.
        if stream.is_done():
          try:
            tl.stop()
          except RuntimeError:
            # The job can only be stopped once. Ignore excessive stops.
            pass

      tl.start()
      return tl

    return dynamic_plotting(stream, pv, tl, include_window_info, display_facets)
  return None


def visualize_computed_pcoll(
    pcoll_name: str,
    pcoll: beam.pvalue.PCollection,
    max_n: int,
    max_duration_secs: float,
    dynamic_plotting_interval: Optional[int] = None,
    include_window_info: bool = False,
    display_facets: bool = False) -> None:
  """A simple visualize alternative.

  When the pcoll_name and pcoll pair identifies a watched and computed
  PCollection in the current interactive environment without ambiguity, an
  ElementStream can be built directly from cache. Returns immediately, the
  visualization is asynchronous, but guaranteed to end in the near future.

  Args:
    pcoll_name: the variable name of the PCollection.
    pcoll: the PCollection to be visualized.
    max_n: the maximum number of elements to visualize.
    max_duration_secs: max duration of elements to read in seconds.
    dynamic_plotting_interval: the interval in seconds between visualization
      updates if provided; otherwise, no dynamic plotting.
    include_window_info: whether to include windowing info in the elements.
    display_facets: whether to display the facets widgets.
  """
  pipeline = ie.current_env().user_pipeline(pcoll.pipeline)
  rm = ie.current_env().get_recording_manager(pipeline, create_if_absent=True)

  stream = rm.read(
      pcoll_name, pcoll, max_n=max_n, max_duration_secs=max_duration_secs)
  if stream:
    visualize(
        stream,
        dynamic_plotting_interval=dynamic_plotting_interval,
        include_window_info=include_window_info,
        display_facets=display_facets,
        element_type=pcoll.element_type)


class PCollectionVisualization(object):
  """A visualization of a PCollection.

  The class relies on creating a PipelineInstrument w/o actual instrument to
  access current interactive environment for materialized PCollection data at
  the moment of self instantiation through cache.
  """
  def __init__(
      self,
      stream,
      include_window_info=False,
      display_facets=False,
      element_type=None):
    assert _pcoll_visualization_ready, (
        'Dependencies for PCollection visualization are not available. Please '
        'use `pip install apache-beam[interactive]` to install necessary '
        'dependencies and make sure that you are executing code in an '
        'interactive environment such as a Jupyter notebook.')
    self._stream = stream
    # Variable name as the title for element value in the rendered data table.
    self._pcoll_var = stream.var
    if not self._pcoll_var:
      self._pcoll_var = 'Value'
    obfuscated_id = stream.display_id(id(self))
    self._dive_display_id = 'facets_dive_{}'.format(obfuscated_id)
    self._overview_display_id = 'facets_overview_{}'.format(obfuscated_id)
    self._df_display_id = 'df_{}'.format(obfuscated_id)
    self._include_window_info = include_window_info
    self._display_facets = display_facets
    self._is_datatable_empty = True
    self._element_type = element_type

  def display_plain_text(self):
    """Displays a head sample of the normalized PCollection data.

    This function is used when the ipython kernel is not connected to a
    notebook frontend such as when running ipython in terminal or in unit tests.
    It's a visualization in terminal-like UI, not a function to retrieve data
    for programmatically usages.
    """
    # Double check if the dependency is ready in case someone mistakenly uses
    # the function.
    if _pcoll_visualization_ready:
      data = self._to_dataframe()
      # Displays a data-table with at most 25 entries from the head.
      data_sample = data.head(25)
      display(data_sample)

  def display(self, updating_pv=None):
    """Displays the visualization through IPython.

    Args:
      updating_pv: A PCollectionVisualization object. When provided, the
        display_id of each visualization part will inherit from the initial
        display of updating_pv and only update that visualization web element
        instead of creating new ones.

    The visualization has 3 parts: facets-dive, facets-overview and paginated
    data table. Each part is assigned an auto-generated unique display id
    (the uniqueness is guaranteed throughout the lifespan of the PCollection
    variable).
    """
    # Ensures that dive, overview and table render the same data because the
    # materialized PCollection data might being updated continuously.
    data = self._to_dataframe()
    # Give the numbered column names when visualizing.
    data.columns = [
        self._pcoll_var + '.' +
        str(column) if isinstance(column, int) else column
        for column in data.columns
    ]
    # String-ify the dictionaries for display because elements of type dict
    # cannot be ordered.
    with warnings.catch_warnings():
      # TODO(yathu) switch to use DataFrame.map when dropped pandas<2.1 support
      warnings.filterwarnings(
          "ignore", message="DataFrame.applymap has been deprecated")
      data = data.applymap(lambda x: str(x) if isinstance(x, dict) else x)

    if updating_pv:
      # Only updates when data is not empty. Otherwise, consider it a bad
      # iteration and noop since there is nothing to be updated.
      if data.empty:
        _LOGGER.debug('Skip a visualization update due to empty data.')
      else:
        self._display_dataframe(data.copy(deep=True), updating_pv)
        if self._display_facets:
          self._display_dive(data.copy(deep=True), updating_pv)
          self._display_overview(data.copy(deep=True), updating_pv)
    else:
      self._display_dataframe(data.copy(deep=True))
      if self._display_facets:
        self._display_dive(data.copy(deep=True))
        self._display_overview(data.copy(deep=True))

  def _display_dive(self, data, update=None):
    sprite_size = 32 if len(data.index) > 50000 else 64
    format_window_info_in_dataframe(data)
    jsonstr = data.to_json(orient='records', default_handler=str)
    if update:
      script = _DIVE_SCRIPT_TEMPLATE.format(
          display_id=update._dive_display_id, jsonstr=jsonstr)
      display_javascript(Javascript(script))
    else:
      html_str = _DIVE_HTML_TEMPLATE.format(
          display_id=self._dive_display_id,
          jsonstr=html.escape(jsonstr),
          sprite_size=sprite_size)
      display(HTML(html_str))

  def _display_overview(self, data, update=None):
    if (not data.empty and self._include_window_info and
        all(column in data.columns
            for column in ('event_time', 'windows', 'pane_info'))):
      data = data.drop(['event_time', 'windows', 'pane_info'], axis=1)

    # GFSG expects all column names to be strings.
    data.columns = data.columns.astype(str)

    gfsg = GenericFeatureStatisticsGenerator()
    proto = gfsg.ProtoFromDataFrames([{'name': 'data', 'table': data}])
    protostr = base64.b64encode(proto.SerializeToString()).decode('utf-8')
    if update:
      script = _OVERVIEW_SCRIPT_TEMPLATE.format(
          display_id=update._overview_display_id, protostr=protostr)
      display_javascript(Javascript(script))
    else:
      html_str = _OVERVIEW_HTML_TEMPLATE.format(
          display_id=self._overview_display_id, protostr=protostr)
      display(HTML(html_str))

  def _display_dataframe(self, data, update=None):
    table_id = 'table_{}'.format(
        update._df_display_id if update else self._df_display_id)
    columns = [{
        'title': ''
    }] + [{
        'title': str(column)
    } for column in data.columns]
    format_window_info_in_dataframe(data)
    # Convert the dataframe into rows, each row looks like
    # [column_1_val, column_2_val, ...].
    with warnings.catch_warnings():
      warnings.filterwarnings(
          "ignore", message=".*DataFrame.applymap has been deprecated.*")
      rows = data.applymap(lambda x: str(x)).to_dict('split')['data']
    # Convert each row into dict where keys are column index in the datatable
    # to be rendered and values are data from the dataframe. Column index 0 is
    # left out to hold the int index (not part of the data) from dataframe.
    # Each row becomes: {1: column_1_val, 2: column_2_val, ...}.
    rows = [{k + 1: v for k, v in enumerate(row)} for row in rows]
    # Add the dataframe int index (used as default ordering column) to datatable
    # column index 0 (will be rendered as the first column).
    # Each row becomes:
    # {1: column_1_val, 2: column_2_val, ..., 0: int_index_in_dataframe}.
    for k, row in enumerate(rows):
      row[0] = k
    script = _DATAFRAME_SCRIPT_TEMPLATE.format(
        table_id=table_id, columns=columns, data_as_rows=rows)
    script_in_jquery_with_datatable = ie._JQUERY_WITH_DATATABLE_TEMPLATE.format(
        customized_script=script)
    # Dynamically load data into the existing datatable if not empty.
    if update and not update._is_datatable_empty:
      display_javascript(Javascript(script_in_jquery_with_datatable))
    else:
      if data.empty:
        html_str = _NO_DATA_TEMPLATE.format(id=table_id)
      else:
        html_str = _DATAFRAME_PAGINATION_TEMPLATE.format(
            table_id=table_id,
            script_in_jquery_with_datatable=script_in_jquery_with_datatable)
      if update:
        if not data.empty:
          # Initialize a datatable to replace the existing no data div.
          display(
              Javascript(
                  ie._JQUERY_WITH_DATATABLE_TEMPLATE.format(
                      customized_script=_NO_DATA_REMOVAL_SCRIPT.format(
                          id=table_id))))
          display(HTML(html_str), display_id=update._df_display_id)
          update._is_datatable_empty = False
      else:
        display(HTML(html_str), display_id=self._df_display_id)
        if not data.empty:
          self._is_datatable_empty = False

  def _to_dataframe(self):
    results = list(self._stream.read(tail=False))
    return elements_to_df(
        results, self._include_window_info, element_type=self._element_type)


def format_window_info_in_dataframe(data):
  if 'event_time' in data.columns:
    data['event_time'] = data['event_time'].apply(event_time_formatter)
  if 'windows' in data.columns:
    data['windows'] = data['windows'].apply(windows_formatter)
  if 'pane_info' in data.columns:
    data['pane_info'] = data['pane_info'].apply(pane_info_formatter)


def event_time_formatter(event_time_us):
  options = ie.current_env().options
  to_tz = options.display_timezone
  try:
    return (
        datetime.datetime.utcfromtimestamp(event_time_us / 1000000).replace(
            tzinfo=tz.tzutc()).astimezone(to_tz).strftime(
                options.display_timestamp_format))
  except ValueError:
    if event_time_us < 0:
      return 'Min Timestamp'
    return 'Max Timestamp'


def windows_formatter(windows):
  result = []
  for w in windows:
    if isinstance(w, GlobalWindow):
      result.append(str(w))
    elif isinstance(w, IntervalWindow):
      # First get the duration in terms of hours, minutes, seconds, and
      # micros.
      duration = w.end.micros - w.start.micros
      duration_secs = duration // 1000000
      hours, remainder = divmod(duration_secs, 3600)
      minutes, seconds = divmod(remainder, 60)
      micros = (duration - duration_secs * 1000000) % 1000000

      # Construct the duration string. Try and write the string in such a
      # way that minimizes the amount of characters written.
      duration = ''
      if hours:
        duration += '{}h '.format(hours)

      if minutes or (hours and seconds):
        duration += '{}m '.format(minutes)

      if seconds:
        if micros:
          duration += '{}.{:06}s'.format(seconds, micros)
        else:
          duration += '{}s'.format(seconds)

      start = event_time_formatter(w.start.micros)

      result.append('{} ({})'.format(start, duration))

  return ','.join(result)


def pane_info_formatter(pane_info):
  from apache_beam.utils.windowed_value import PaneInfo
  from apache_beam.utils.windowed_value import PaneInfoTiming
  assert isinstance(pane_info, PaneInfo)

  result = 'Pane {}'.format(pane_info.index)
  timing_info = '{}{}'.format(
      'Final ' if pane_info.is_last else '',
      PaneInfoTiming.to_string(pane_info.timing).lower().capitalize() if
      pane_info.timing in (PaneInfoTiming.EARLY, PaneInfoTiming.LATE) else '')

  if timing_info:
    result += ': ' + timing_info

  return result
