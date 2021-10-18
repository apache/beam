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

from typing import Dict
from typing import List
from typing import Set
from typing import Union

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
  class PCollectionNode(object):
    def __init__(self, name):
      self.name = name
      self._watermark = timestamp.MIN_TIMESTAMP
      self.producers: Set[WatermarkManager.StageNode] = set()

    def __str__(self):
      return 'PCollectionNode<producers=%s>' % list(self.producers)

    def set_watermark(self, wm: timestamp.Timestamp):
      self._watermark = min(self.upstream_watermark(), wm)

    def upstream_watermark(self):
      if self.producers:
        return min(p.input_watermark() for p in self.producers)
      else:
        return timestamp.MAX_TIMESTAMP

    def watermark(self):
      return self._watermark

  class StageNode(object):
    def __init__(self, name):
      # We keep separate inputs and side inputs because side inputs
      # should hold back a stage's input watermark, to hold back execution
      # for that stage; but they should not be considered when calculating
      # the output watermark of the stage, because only the main input
      # can actually advance that watermark.
      self.name = name
      self.inputs: Set[WatermarkManager.PCollectionNode] = set()
      self.side_inputs: Set[WatermarkManager.PCollectionNode] = set()
      self.outputs: Set[WatermarkManager.PCollectionNode] = set()

    def __str__(self):
      return 'StageNode<inputs=%s,side_inputs=%s' % (
          [i.name for i in self.inputs], [i.name for i in self.side_inputs])

    def output_watermark(self):
      if not self.outputs:
        return self.input_watermark()
      else:
        return min(o.watermark() for o in self.outputs)

    def input_watermark(self):
      if not self.inputs:
        return timestamp.MAX_TIMESTAMP
      w = min(i.upstream_watermark() for i in self.inputs)

      if self.side_inputs:
        w = min(w, min(i.upstream_watermark() for i in self.side_inputs))
      return w

  def __init__(self, stages):
    # type: (List[translations.Stage]) -> None
    self._pcollections_by_name: Dict[Union[str, translations.TimerFamilyId],
                                     WatermarkManager.PCollectionNode] = {}
    self._stages_by_name: Dict[str, WatermarkManager.StageNode] = {}

    def add_pcollection(
        pcname: str,
        snode: WatermarkManager.StageNode) -> WatermarkManager.PCollectionNode:
      if pcname not in self._pcollections_by_name:
        self._pcollections_by_name[pcname] = WatermarkManager.PCollectionNode(
            pcname)
      pcnode = self._pcollections_by_name[pcname]
      assert isinstance(pcnode, WatermarkManager.PCollectionNode)
      snode.inputs.add(pcnode)
      return pcnode

    for s in stages:
      stage_name = s.name
      stage_node = WatermarkManager.StageNode(stage_name)
      self._stages_by_name[stage_name] = stage_node

      # 1. Get stage inputs, create nodes for them, add to _stages_by_name,
      #    and add as inputs to stage node.
      for transform in s.transforms:
        if transform.spec.urn == bundle_processor.DATA_INPUT_URN:
          buffer_id = transform.spec.payload
          if buffer_id == translations.IMPULSE_BUFFER:
            pcoll_name = transform.unique_name
            add_pcollection(pcoll_name, stage_node)
            continue
          else:
            _, pcoll_name = split_buffer_id(buffer_id)
          add_pcollection(pcoll_name, stage_node)

      # 2. Get stage timers, and add them as inputs to the stage.
      for transform in s.transforms:
        if transform.spec.urn in translations.PAR_DO_URNS:
          payload = proto_utils.parse_Bytes(
              transform.spec.payload, beam_runner_api_pb2.ParDoPayload)
          for timer_family_id in payload.timer_family_specs.keys():
            timer_pcoll_name = (transform.unique_name, timer_family_id)
            self._pcollections_by_name[
                timer_pcoll_name] = WatermarkManager.PCollectionNode(
                    timer_pcoll_name)
            timer_pcoll_node = self._pcollections_by_name[timer_pcoll_name]
            assert isinstance(
                timer_pcoll_node, WatermarkManager.PCollectionNode)
            stage_node.inputs.add(timer_pcoll_node)

      # 3. Get stage outputs, create nodes for them, add to
      # _pcollections_by_name, and add stage as their producer
      for transform in s.transforms:
        if transform.spec.urn == bundle_processor.DATA_OUTPUT_URN:
          buffer_id = transform.spec.payload
          _, pcoll_name = split_buffer_id(buffer_id)
          if pcoll_name not in self._pcollections_by_name:
            self._pcollections_by_name[
                pcoll_name] = WatermarkManager.PCollectionNode(pcoll_name)
          pcoll_node = self._pcollections_by_name[pcoll_name]
          assert isinstance(pcoll_node, WatermarkManager.PCollectionNode)
          pcoll_node.producers.add(stage_node)
          stage_node.outputs.add(pcoll_node)

      # 4. Get stage side inputs, create nodes for them, add to
      #    _pcollections_by_name, and add them as side inputs of the stage.
      for pcoll_name in s.side_inputs():
        if pcoll_name not in self._pcollections_by_name:
          self._pcollections_by_name[
              pcoll_name] = WatermarkManager.PCollectionNode(pcoll_name)
        pcoll_node = self._pcollections_by_name[pcoll_name]
        assert isinstance(pcoll_node, WatermarkManager.PCollectionNode)
        stage_node.side_inputs.add(pcoll_node)

  def get_stage_node(self, name):
    # type: (str) -> StageNode
    return self._stages_by_name[name]

  def set_pcoll_watermark(self, name, watermark):
    element = self._pcollections_by_name[name]
    element.set_watermark(watermark)
