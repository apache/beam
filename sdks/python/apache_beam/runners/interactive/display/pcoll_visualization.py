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

from __future__ import absolute_import

import base64
import logging
from datetime import timedelta

from pandas.io.json import json_normalize

from apache_beam import pvalue
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as instr

try:
  import jsons  # pylint: disable=import-error
  from IPython import get_ipython  # pylint: disable=import-error
  from IPython.core.display import HTML  # pylint: disable=import-error
  from IPython.core.display import Javascript  # pylint: disable=import-error
  from IPython.core.display import display  # pylint: disable=import-error
  from IPython.core.display import display_javascript  # pylint: disable=import-error
  from IPython.core.display import update_display  # pylint: disable=import-error
  from facets_overview.generic_feature_statistics_generator import GenericFeatureStatisticsGenerator  # pylint: disable=import-error
  from timeloop import Timeloop  # pylint: disable=import-error

  if get_ipython():
    _pcoll_visualization_ready = True
  else:
    _pcoll_visualization_ready = False
except ImportError:
  _pcoll_visualization_ready = False

# 1-d types that need additional normalization to be compatible with DataFrame.
_one_dimension_types = (int, float, str, bool, list, tuple)

_DIVE_SCRIPT_TEMPLATE = """
            document.querySelector("#{display_id}").data = {jsonstr};"""
_DIVE_HTML_TEMPLATE = """
            <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
            <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html">
            <facets-dive sprite-image-width="{sprite_size}" sprite-image-height="{sprite_size}" id="{display_id}" height="600"></facets-dive>
            <script>
              document.querySelector("#{display_id}").data = {jsonstr};
            </script>"""
_OVERVIEW_SCRIPT_TEMPLATE = """
              document.querySelector("#{display_id}").protoInput = "{protostr}";
              """
_OVERVIEW_HTML_TEMPLATE = """
            <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
            <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist/facets-jupyter.html">
            <facets-overview id="{display_id}"></facets-overview>
            <script>
              document.querySelector("#{display_id}").protoInput = "{protostr}";
            </script>"""
_DATAFRAME_PAGINATION_TEMPLATE = """
            <script src="https://ajax.googleapis.com/ajax/libs/jquery/2.2.2/jquery.min.js"></script>
            <script src="https://cdn.datatables.net/1.10.20/js/jquery.dataTables.min.js"></script>
            <link rel="stylesheet" href="https://cdn.datatables.net/1.10.20/css/jquery.dataTables.min.css">
            {dataframe_html}
            <script>
              $(document).ready(
                function() {{
                  $("#{table_id}").DataTable();
                }});
            </script>"""


def visualize(pcoll, dynamic_plotting_interval=None):
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

  The function is experimental. For internal use only; no
  backwards-compatibility guarantees.
  """
  if not _pcoll_visualization_ready:
    return None
  pv = PCollectionVisualization(pcoll)
  if ie.current_env().is_in_notebook:
    pv.display_facets()
  else:
    pv.display_plain_text()
    # We don't want to do dynamic plotting if there is no notebook frontend.
    return None

  if dynamic_plotting_interval:
    # Disables the verbose logging from timeloop.
    logging.getLogger('timeloop').disabled = True
    tl = Timeloop()

    def dynamic_plotting(pcoll, pv, tl):
      @tl.job(interval=timedelta(seconds=dynamic_plotting_interval))
      def continuous_update_display():  # pylint: disable=unused-variable
        # Always creates a new PCollVisualization instance when the
        # PCollection materialization is being updated and dynamic
        # plotting is in-process.
        updated_pv = PCollectionVisualization(pcoll)
        updated_pv.display_facets(updating_pv=pv)
        if ie.current_env().is_terminated(pcoll.pipeline):
          try:
            tl.stop()
          except RuntimeError:
            # The job can only be stopped once. Ignore excessive stops.
            pass

      tl.start()
      return tl

    return dynamic_plotting(pcoll, pv, tl)
  return None


class PCollectionVisualization(object):
  """A visualization of a PCollection.

  The class relies on creating a PipelineInstrument w/o actual instrument to
  access current interactive environment for materialized PCollection data at
  the moment of self instantiation through cache.
  """
  def __init__(self, pcoll):
    assert _pcoll_visualization_ready, (
        'Dependencies for PCollection visualization are not available. Please '
        'use `pip install apache-beam[interactive]` to install necessary '
        'dependencies and make sure that you are executing code in an '
        'interactive environment such as a Jupyter notebook.')
    assert isinstance(
        pcoll,
        pvalue.PCollection), ('pcoll should be apache_beam.pvalue.PCollection')
    self._pcoll = pcoll
    # This allows us to access cache key and other meta data about the pipeline
    # whether it's the pipeline defined in user code or a copy of that pipeline.
    # Thus, this module doesn't need any other user input but the PCollection
    # variable to be visualized. It then automatically figures out the pipeline
    # definition, materialized data and the pipeline result for the execution
    # even if the user never assigned or waited the result explicitly.
    # With only the constructor of PipelineInstrument, any interactivity related
    # pre-process or instrument is not triggered for performance concerns.
    self._pin = instr.PipelineInstrument(pcoll.pipeline)
    self._cache_key = self._pin.cache_key(self._pcoll)
    self._dive_display_id = 'facets_dive_{}_{}'.format(
        self._cache_key, id(self))
    self._overview_display_id = 'facets_overview_{}_{}'.format(
        self._cache_key, id(self))
    self._df_display_id = 'df_{}_{}'.format(self._cache_key, id(self))

  def display_plain_text(self):
    """Displays a random sample of the normalized PCollection data.

    This function is used when the ipython kernel is not connected to a
    notebook frontend such as when running ipython in terminal or in unit tests.
    """
    # Double check if the dependency is ready in case someone mistakenly uses
    # the function.
    if _pcoll_visualization_ready:
      data = self._to_dataframe()
      data_sample = data.sample(n=25 if len(data) > 25 else len(data))
      display(data_sample)

  def display_facets(self, updating_pv=None):
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
    if updating_pv:
      self._display_dive(data, updating_pv._dive_display_id)
      self._display_overview(data, updating_pv._overview_display_id)
      self._display_dataframe(data, updating_pv._df_display_id)
    else:
      self._display_dive(data)
      self._display_overview(data)
      self._display_dataframe(data)

  def _display_dive(self, data, update=None):
    sprite_size = 32 if len(data.index) > 50000 else 64
    jsonstr = data.to_json(orient='records')
    if update:
      script = _DIVE_SCRIPT_TEMPLATE.format(display_id=update, jsonstr=jsonstr)
      display_javascript(Javascript(script))
    else:
      html = _DIVE_HTML_TEMPLATE.format(
          display_id=self._dive_display_id,
          jsonstr=jsonstr,
          sprite_size=sprite_size)
      display(HTML(html))

  def _display_overview(self, data, update=None):
    gfsg = GenericFeatureStatisticsGenerator()
    proto = gfsg.ProtoFromDataFrames([{'name': 'data', 'table': data}])
    protostr = base64.b64encode(proto.SerializeToString()).decode('utf-8')
    if update:
      script = _OVERVIEW_SCRIPT_TEMPLATE.format(
          display_id=update, protostr=protostr)
      display_javascript(Javascript(script))
    else:
      html = _OVERVIEW_HTML_TEMPLATE.format(
          display_id=self._overview_display_id, protostr=protostr)
      display(HTML(html))

  def _display_dataframe(self, data, update=None):
    if update:
      table_id = 'table_{}'.format(update)
      html = _DATAFRAME_PAGINATION_TEMPLATE.format(
          dataframe_html=data.to_html(notebook=True, table_id=table_id),
          table_id=table_id)
      update_display(HTML(html), display_id=update)
    else:
      table_id = 'table_{}'.format(self._df_display_id)
      html = _DATAFRAME_PAGINATION_TEMPLATE.format(
          dataframe_html=data.to_html(notebook=True, table_id=table_id),
          table_id=table_id)
      display(HTML(html), display_id=self._df_display_id)

  def _to_element_list(self):
    pcoll_list = []
    if ie.current_env().cache_manager().exists('full', self._cache_key):
      pcoll_list, _ = ie.current_env().cache_manager().read('full',
                                                            self._cache_key)
    return pcoll_list

  def _to_dataframe(self):
    normalized_list = []
    # Column name for _one_dimension_types if presents.
    normalized_column = str(self._pcoll)
    # Normalization needs to be done for each element because they might be of
    # different types. The check is only done on the root level, pandas json
    # normalization I/O would take care of the nested levels.
    for el in self._to_element_list():
      if self._is_one_dimension_type(el):
        # Makes such data structured.
        normalized_list.append({normalized_column: el})
      else:
        normalized_list.append(jsons.load(jsons.dump(el)))
    # Creates a dataframe that str() 1-d iterable elements after
    # normalization so that facets_overview can treat such data as categorical.
    return json_normalize(normalized_list).applymap(
        lambda x: str(x) if type(x) in (list, tuple) else x)

  def _is_one_dimension_type(self, val):
    return type(val) in _one_dimension_types
