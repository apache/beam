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

"""Set of utilities to visualize a pipeline to be executed by FnApiRunner."""
from typing import Set
from typing import Tuple

from apache_beam.runners.portability.fn_api_runner.translations import Stage
from apache_beam.runners.portability.fn_api_runner.watermark_manager import WatermarkManager
from apache_beam.utils import timestamp


def show_stage(stage: Stage):
  try:
    import graphviz
  except ImportError:
    import warnings
    warnings.warn('Unable to draw pipeline. graphviz library missing.')
    return

  g = graphviz.Digraph()

  seen_pcollections = set()
  for t in stage.transforms:
    g.node(t.unique_name, shape='box')

    for i in t.inputs.values():
      assert isinstance(i, str)
      if i not in seen_pcollections:
        g.node(i)
        seen_pcollections.add(i)

      g.edge(i, t.unique_name)

    for o in t.outputs.values():
      assert isinstance(o, str)
      if o not in seen_pcollections:
        g.node(o)
        seen_pcollections.add(o)

      g.edge(t.unique_name, o)

  g.render('stage_graph', format='png')


def show_watermark_manager(watermark_manager: WatermarkManager, filename=None):
  try:
    import graphviz
  except ImportError:
    import warnings
    warnings.warn('Unable to draw pipeline. graphviz library missing.')
    return

  g = graphviz.Digraph()

  def pcoll_node_name(pcoll_node: WatermarkManager.PCollectionNode):
    if isinstance(pcoll_node.name, tuple):
      return 'PCOLL_%s_%s' % pcoll_node.name
    else:
      return 'PCOLL_%s' % pcoll_node.name

  def add_node(name, shape=None, color=None, label=None):
    if name not in seen_nodes:
      seen_nodes.add(name)
      g.node(
          name,
          shape=shape,
          fillcolor=color,
          style='filled',
          label=name + (label or ''))

  def add_links(link_from=None, link_to=None, edge_style="solid"):
    if link_from and link_to:
      if (link_to, link_from, edge_style) not in seen_links:
        g.edge(link_from, link_to, style=edge_style)
        seen_links.add((link_to, link_from, edge_style))

  seen_nodes: Set[str] = set()
  seen_links: Set[Tuple[str, str]] = set()
  for node in watermark_manager._stages_by_name.values():
    name = 'STAGE_%s...%s' % (node.name[:30], node.name[-30:])
    add_node(name, 'box')

  for pcnode in watermark_manager._pcollections_by_name.values():
    assert isinstance(pcnode, WatermarkManager.PCollectionNode)
    name = pcoll_node_name(pcnode)
    if pcnode.watermark() == timestamp.MIN_TIMESTAMP:
      color = 'aquamarine'
    elif pcnode.watermark() == timestamp.MAX_TIMESTAMP:
      color = 'aquamarine4'
    else:
      color = 'aquamarine2'
    add_node(
        name,
        color=color,
        label='\n%s\nprod: %s' %
        (pcnode.watermark(), pcnode._produced_watermark))

  for node in watermark_manager._stages_by_name.values():
    stage = 'STAGE_%s...%s' % (node.name[:30], node.name[-30:])
    for pcoll in node.inputs:
      input_name = pcoll_node_name(pcoll)
      # Main inputs have a BOLD edge.
      add_links(link_from=input_name, link_to=stage, edge_style="bold")
    for pcoll in node.side_inputs:
      # Side inputs have a dashed edge.
      input_name = pcoll_node_name(pcoll)
      add_links(link_from=input_name, link_to=stage, edge_style="dashed")

  for pcnode in watermark_manager._pcollections_by_name.values():
    assert isinstance(pcnode, WatermarkManager.PCollectionNode)
    pcoll_name = pcoll_node_name(pcnode)
    for producer in pcnode.producers:
      prod_name = 'STAGE_%s...%s' % (producer.name[:30], producer.name[-30:])
      add_links(link_from=prod_name, link_to=pcoll_name)

  g.render(filename or 'pipeline_graph', format='png')
