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

"""For rendering pipeline graph in HTML-compatible format.

This module is experimental. No backwards-compatibility guarantees.
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import os
import subprocess
from typing import TYPE_CHECKING
from typing import Optional
from typing import Type

from future.utils import with_metaclass

from apache_beam.utils.plugin import BeamPlugin

if TYPE_CHECKING:
  from apache_beam.runners.interactive.display.pipeline_graph import PipelineGraph


class PipelineGraphRenderer(with_metaclass(abc.ABCMeta, BeamPlugin)):  # type: ignore[misc]
  """Abstract class for renderers, who decide how pipeline graphs are rendered.
  """
  @classmethod
  @abc.abstractmethod
  def option(cls):
    # type: () -> str

    """The corresponding rendering option for the renderer.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def render_pipeline_graph(self, pipeline_graph):
    # type: (PipelineGraph) -> str

    """Renders the pipeline graph in HTML-compatible format.

    Args:
      pipeline_graph: (pipeline_graph.PipelineGraph) the graph to be rendererd.

    Returns:
      unicode, str or bytes that can be expressed as HTML.
    """
    raise NotImplementedError


class MuteRenderer(PipelineGraphRenderer):
  """Use this renderer to mute the pipeline display.
  """
  @classmethod
  def option(cls):
    # type: () -> str
    return 'mute'

  def render_pipeline_graph(self, pipeline_graph):
    # type: (PipelineGraph) -> str
    return ''


class TextRenderer(PipelineGraphRenderer):
  """This renderer simply returns the dot representation in text format.
  """
  @classmethod
  def option(cls):
    # type: () -> str
    return 'text'

  def render_pipeline_graph(self, pipeline_graph):
    # type: (PipelineGraph) -> str
    return pipeline_graph.get_dot()


class PydotRenderer(PipelineGraphRenderer):
  """This renderer renders the graph using pydot.

  It depends on
    1. The software Graphviz: https://www.graphviz.org/
    2. The python module pydot: https://pypi.org/project/pydot/
  """
  @classmethod
  def option(cls):
    # type: () -> str
    return 'graph'

  def render_pipeline_graph(self, pipeline_graph):
    # type: (PipelineGraph) -> str
    return pipeline_graph._get_graph().create_svg().decode("utf-8")  # pylint: disable=protected-access


def get_renderer(option=None):
  # type: (Optional[str]) -> Type[PipelineGraphRenderer]

  """Get an instance of PipelineGraphRenderer given rendering option.

  Args:
    option: (str) the rendering option.

  Returns:
    (PipelineGraphRenderer)
  """
  if option is None:
    if os.name == 'nt':
      exists = subprocess.call(['where', 'dot.exe']) == 0
    else:
      exists = subprocess.call(['which', 'dot']) == 0

    if exists:
      option = 'graph'
    else:
      option = 'text'

  renderer = [
      r for r in PipelineGraphRenderer.get_all_subclasses()
      if option == r.option()
  ]
  if len(renderer) == 0:
    raise ValueError()
  elif len(renderer) == 1:
    return renderer[0]()
  else:
    raise ValueError('Found more than one renderer for option: %s', option)
