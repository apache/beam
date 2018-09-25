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

"""For generating Beam pipeline graph in DOT representation.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import threading

import pydot

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2


class PipelineGraph(object):
  """Creates a DOT representation of the pipeline. Thread-safe."""

  def __init__(self,
               pipeline,
               default_vertex_attrs=None,
               default_edge_attrs=None):
    """Constructor of PipelineGraph.

    Examples:
      graph = pipeline_graph.PipelineGraph(pipeline_proto)
      graph.display_graph()

      or

      graph = pipeline_graph.PipelineGraph(pipeline)
      graph.display_graph()

    Args:
      pipeline: (Pipeline proto) or (Pipeline) pipeline to be rendered.
      default_vertex_attrs: (Dict[str, str]) a dict of default vertex attributes
      default_edge_attrs: (Dict[str, str]) a dict of default edge attributes
    """
    self._lock = threading.Lock()
    self._graph = None

    if isinstance(pipeline, beam_runner_api_pb2.Pipeline):
      self._pipeline_proto = pipeline
    elif isinstance(pipeline, beam.Pipeline):
      self._pipeline_proto = pipeline.to_runner_api()
    else:
      raise TypeError('pipeline should either be a %s or %s, while %s is given'
                      % (beam_runner_api_pb2.Pipeline, beam.Pipeline,
                         type(pipeline)))

    # A dict from PCollection ID to a list of its consuming Transform IDs
    self._consumers = collections.defaultdict(list)
    # A dict from PCollection ID to its producing Transform ID
    self._producers = {}

    for transform_id, transform_proto in self._top_level_transforms():
      for pcoll_id in transform_proto.inputs.values():
        self._consumers[pcoll_id].append(transform_id)
      for pcoll_id in transform_proto.outputs.values():
        self._producers[pcoll_id] = transform_id

    # Set the default vertex color to blue.
    default_vertex_attrs = default_vertex_attrs or {}
    if 'color' not in default_vertex_attrs:
      default_vertex_attrs['color'] = 'blue'
    if 'fontcolor' not in default_vertex_attrs:
      default_vertex_attrs['fontcolor'] = 'blue'

    vertex_dict, edge_dict = self._generate_graph_dicts()
    self._construct_graph(vertex_dict,
                          edge_dict,
                          default_vertex_attrs,
                          default_edge_attrs)

  def get_dot(self):
    return self._get_graph().to_string()

  def _top_level_transforms(self):
    """Yields all top level PTransforms (subtransforms of the root PTransform).

    Yields: (str, PTransform proto) ID, proto pair of top level PTransforms.
    """
    transforms = self._pipeline_proto.components.transforms
    for root_transform_id in self._pipeline_proto.root_transform_ids:
      root_transform_proto = transforms[root_transform_id]
      for top_level_transform_id in root_transform_proto.subtransforms:
        top_level_transform_proto = transforms[top_level_transform_id]
        yield top_level_transform_id, top_level_transform_proto

  def _generate_graph_dicts(self):
    """From pipeline_proto and other info, generate the graph.

    Returns:
      vertex_dict: (Dict[str, Dict[str, str]]) vertex mapped to attributes.
      edge_dict: (Dict[(str, str), Dict[str, str]]) vertex pair mapped to the
          edge's attribute.
    """
    transforms = self._pipeline_proto.components.transforms

    # A dict from vertex name (i.e. PCollection ID) to its attributes.
    vertex_dict = collections.defaultdict(dict)
    # A dict from vertex name pairs defining the edge (i.e. a pair of PTransform
    # IDs defining the PCollection) to its attributes.
    edge_dict = collections.defaultdict(dict)

    self._edge_to_vertex_pairs = collections.defaultdict(list)

    for _, transform in self._top_level_transforms():
      vertex_dict[transform.unique_name] = {}

      for pcoll_id in transform.outputs.values():
        # For PCollections without consuming PTransforms, we add an invisible
        # PTransform node as the consumer.
        if pcoll_id not in self._consumers:
          invisible_leaf = 'leaf%s' % (hash(pcoll_id) % 10000)
          vertex_dict[invisible_leaf] = {'style': 'invis'}
          self._edge_to_vertex_pairs[pcoll_id].append(
              (transform.unique_name, invisible_leaf))
          edge_dict[(transform.unique_name, invisible_leaf)] = {}
        else:
          for consumer in self._consumers[pcoll_id]:
            producer_name = transform.unique_name
            consumer_name = transforms[consumer].unique_name
            self._edge_to_vertex_pairs[pcoll_id].append(
                (producer_name, consumer_name))
            edge_dict[(producer_name, consumer_name)] = {}

    return vertex_dict, edge_dict

  def _get_graph(self):
    """Returns pydot.Dot object for the pipeline graph.

    The purpose of this method is to avoid accessing the graph while it is
    updated. No one except for this method should be accessing _graph directly.

    Returns:
      (pydot.Dot)
    """
    with self._lock:
      return self._graph

  def _construct_graph(self, vertex_dict, edge_dict,
                       default_vertex_attrs, default_edge_attrs):
    """Constructs the pydot.Dot object for the pipeline graph.

    Args:
      vertex_dict: (Dict[str, Dict[str, str]]) maps vertex names to attributes
      edge_dict: (Dict[(str, str), Dict[str, str]]) maps vertex name pairs to
          attributes
      default_vertex_attrs: (Dict[str, str]) a dict of attributes
      default_edge_attrs: (Dict[str, str]) a dict of attributes
    """
    with self._lock:
      self._graph = pydot.Dot()

      if default_vertex_attrs:
        self._graph.set_node_defaults(**default_vertex_attrs)
      if default_edge_attrs:
        self._graph.set_edge_defaults(**default_edge_attrs)

      self._vertex_refs = {}  # Maps vertex name to pydot.Node
      self._edge_refs = {}  # Maps vertex name pairs to pydot.Edge

      for vertex, vertex_attrs in vertex_dict.items():
        vertex_ref = pydot.Node(vertex, **vertex_attrs)
        self._vertex_refs[vertex] = vertex_ref
        self._graph.add_node(vertex_ref)

      for edge, edge_attrs in edge_dict.items():
        vertex_src = self._vertex_refs[edge[0]]
        vertex_dst = self._vertex_refs[edge[1]]

        edge_ref = pydot.Edge(vertex_src, vertex_dst, **edge_attrs)
        self._edge_refs[edge] = edge_ref
        self._graph.add_edge(edge_ref)

  def _update_graph(self, vertex_dict=None, edge_dict=None):
    """Updates the pydot.Dot object with the given attribute update

    Args:
      vertex_dict: (Dict[str, Dict[str, str]]) maps vertex names to attributes
      edge_dict: This should be
          Either (Dict[str, Dict[str, str]]) which maps edge names to attributes
          Or (Dict[(str, str), Dict[str, str]]) which maps vertex pairs to edge
          attributes
    """
    def set_attrs(ref, attrs):
      for attr_name, attr_val in attrs.items():
        ref.set(attr_name, attr_val)

    with self._lock:
      if vertex_dict:
        for vertex, vertex_attrs in vertex_dict.items():
          set_attrs(self._vertex_refs[vertex], vertex_attrs)
      if edge_dict:
        for edge, edge_attrs in edge_dict.items():
          if isinstance(edge, tuple):
            set_attrs(self._edge_refs[edge], edge_attrs)
          else:
            for vertex_pair in self._edge_to_vertex_pairs[edge]:
              set_attrs(self._edge_refs[vertex_pair], edge_attrs)
