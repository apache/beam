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

"""Utilities for managing watermarks for a pipeline execution by FnApiRunner."""

from __future__ import absolute_import

from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.runners.portability.fn_api_runner.translations import split_buffer_id
from apache_beam.runners.worker import bundle_processor
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp


class WatermarkManager(object):
  """Manages the watermarks of a pipeline's stages.
    It works by constructing an internal graph representation of the pipeline,
    and keeping track of dependencies."""
  class WatermarkNode(object):
    def __init__(self, name):
      self.name = name

  class PCollectionNode(WatermarkNode):
    def __init__(self, name):
      super(WatermarkManager.PCollectionNode, self).__init__(name)
      self._watermark = timestamp.MIN_TIMESTAMP
      self.producers = set()

    def __str__(self):
      return 'PCollectionNode<producers=[%s]' % ([i for i in self.producers])

    def set_watermark(self, wm):
      self._watermark = min(self.upstream_watermark(), wm)

    def upstream_watermark(self):
      if self.producers:
        return min(p.output_watermark() for p in self.producers)
      else:
        return timestamp.MAX_TIMESTAMP

    def watermark(self):
      if self._watermark:
        return self._watermark
      else:
        return self.upstream_watermark()

  class StageNode(WatermarkNode):
    def __init__(self, name):
      super(WatermarkManager.StageNode, self).__init__(name)
      # We keep separate inputs and side inputs because side inputs
      # should hold back a stage's input watermark, to hold back execution
      # for that stage; but they should not be considered when calculating
      # the output watermark of the stage, because only the main input
      # can actually advance that watermark.
      self.inputs = set()
      self.side_inputs = set()

    def __str__(self):
      return 'StageNode<inputs=[%s],side_inputs=[%s]' % (
          [i.name for i in self.inputs], [i.name for i in self.side_inputs])

    def set_watermark(self, wm):
      raise NotImplementedError('Stages do not have a watermark')

    def output_watermark(self):
      w = min(i.watermark() for i in self.inputs)
      return w

    def input_watermark(self):
      w = min(i.upstream_watermark() for i in self.inputs)

      if self.side_inputs:
        w = min(w, min(i.upstream_watermark() for i in self.side_inputs))
      return w

  def __init__(self, stages):
    # type: (List[translations.Stage]) -> None
    self._watermarks_by_name = {}
    for s in stages:
      stage_name = s.name
      stage_node = WatermarkManager.StageNode(stage_name)
      self._watermarks_by_name[stage_name] = stage_node

      # 1. Get stage inputs, create nodes for them, add to _watermarks_by_name,
      #    and add as inputs to stage node.
      for transform in s.transforms:
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          buffer_id = transform.spec.payload
          if buffer_id == translations.IMPULSE_BUFFER:
            pcoll_name = transform.unique_name
          else:
            _, pcoll_name = split_buffer_id(buffer_id)
          if pcoll_name not in self._watermarks_by_name:
            self._watermarks_by_name[
                pcoll_name] = WatermarkManager.PCollectionNode(pcoll_name)
          stage_node.inputs.add(self._watermarks_by_name[pcoll_name])

      # 2. Get stage timers, and add them as inputs to the stage.
      for transform in s.transforms:
        if transform.spec.urn in translations.PAR_DO_URNS:
          payload = proto_utils.parse_Bytes(
              transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
          for timer_family_id in payload.timer_family_specs.keys():
            timer_pcoll_name = (transform.unique_name, timer_family_id)
            self._watermarks_by_name[
                timer_pcoll_name] = WatermarkManager.PCollectionNode(
                    timer_pcoll_name)
            stage_node.inputs.add(self._watermarks_by_name[timer_pcoll_name])

      # 3. Get stage outputs, create nodes for them, add to _watermarks_by_name,
      #    and add stage as their producer
      for transform in s.transforms:
        if transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          buffer_id = transform.spec.payload
          _, pcoll_name = split_buffer_id(buffer_id)
          if pcoll_name not in self._watermarks_by_name:
            self._watermarks_by_name[
                pcoll_name] = WatermarkManager.PCollectionNode(pcoll_name)
          self._watermarks_by_name[pcoll_name].producers.add(stage_node)

      # 4. Get stage side inputs, create nodes for them, add to
      #    _watermarks_by_name, and add them as side inputs of the stage.
      for pcoll_name in s.side_inputs():
        if pcoll_name not in self._watermarks_by_name:
          self._watermarks_by_name[
              pcoll_name] = WatermarkManager.PCollectionNode(pcoll_name)
        stage_node.side_inputs.add(self._watermarks_by_name[pcoll_name])

  def get_node(self, name):
    # type: (str) -> WatermarkNode
    return self._watermarks_by_name[name]

  def get_watermark(self, name):
    element = self._watermarks_by_name[name]
    return element.watermark()

  def set_watermark(self, name, watermark):
    element = self._watermarks_by_name[name]
    element.set_watermark(watermark)

  def show(self):
    try:
      import graphviz
    except ImportError:
      import warnings
      warnings.warn('Unable to draw pipeline. graphviz library missing.')
      return

    g = graphviz.Digraph()

    def add_node(name, shape=None):
      if name not in seen_nodes:
        seen_nodes.add(name)
        g.node(name, shape=shape)

    def add_links(link_from=None, link_to=None):
      if link_from and link_to:
        if (link_to, link_from) not in seen_links:
          g.edge(link_from, link_to)
          seen_links.add((link_to, link_from))

    seen_nodes = set()
    seen_links = set()
    for node in self._watermarks_by_name.values():
      if isinstance(node, WatermarkManager.StageNode):
        name = 'STAGE_%s...%s' % (node.name[:30], node.name[-30:])
        add_node(name, 'box')
      else:
        assert isinstance(node, WatermarkManager.PCollectionNode)
        name = 'PCOLL_%s' % node.name
        add_node(name)

    for node in self._watermarks_by_name.values():
      if isinstance(node, WatermarkManager.StageNode):
        stage = 'STAGE_%s...%s' % (node.name[:30], node.name[-30:])
        for pcoll in node.inputs:
          input_name = 'PCOLL_%s' % pcoll.name
          add_links(link_from=input_name, link_to=stage)
        for pcoll in node.side_inputs:
          input_name = 'PCOLL_%s' % pcoll.name
          add_links(link_from=input_name, link_to=stage)
      else:
        assert isinstance(node, WatermarkManager.PCollectionNode)
        pcoll_name = 'PCOLL_%s' % node.name
        for producer in node.producers:
          prod_name = 'STAGE_%s...%s' % (
              producer.name[:30], producer.name[-30:])
          add_links(link_from=prod_name, link_to=pcoll_name)

    g.render('graph', format='png')
