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

"""DirectRunner, executing on the local machine.

The DirectRunner is a runner implementation that executes the entire
graph of transformations belonging to a pipeline on the local machine.
"""

# from __future__ import absolute_import

import cPickle as pickle
import itertools
import json
import logging
import multiprocessing
from Queue import Queue
import random
import socket
import struct
import subprocess
import sys
import threading
import time

# import collections
# import logging

# from google.protobuf import wrappers_pb2

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.coders import typecoders
from apache_beam.internal import pickler
# from apache_beam import typehints
# from apache_beam.metrics.execution import MetricsEnvironment
# from apache_beam.options.pipeline_options import DirectOptions
# from apache_beam.options.pipeline_options import StandardOptions
# from apache_beam.options.value_provider import RuntimeValueProvider
# from apache_beam.pvalue import PCollection
# from apache_beam.runners.direct.bundle_factory import BundleFactory
# from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
# from apache_beam.runners.runner import PipelineState
# from apache_beam.runners.runner import PValueCache
# from apache_beam.transforms.core import _GroupAlsoByWindow
# from apache_beam.transforms.core import _GroupByKeyOnly
# from apache_beam.transforms.ptransform import PTransform

# __all__ = ['LaserRunner']

from apache_beam.runners.laser.channels import ChannelConfig
from apache_beam.runners.laser.channels import LinkMode
from apache_beam.runners.laser.channels import LinkStrategy
from apache_beam.runners.laser.channels import LinkStrategyType
from apache_beam.runners.laser.channels import Interface
from apache_beam.runners.laser.channels import set_channel_config
from apache_beam.runners.laser.channels import get_channel_manager
from apache_beam.runners.laser.channels import remote_method
from apache_beam.runners.laser.compute_node import InProcessComputeNodeManager
from apache_beam.runners.laser.compute_node import MultiProcessComputeNodeManager
from apache_beam.runners.laser.graph import CompositeWatermarkNode
from apache_beam.runners.laser.graph import StepGraph
from apache_beam.runners.laser.graph import ParDoStep
from apache_beam.runners.laser.graph import FlattenStep
from apache_beam.runners.laser.graph import GroupByKeyOnlyStep
from apache_beam.runners.laser.graph import ReadStep
from apache_beam.runners.laser.graph import ParDoFnData
from apache_beam.runners.laser.laser_operations import ShuffleReadOperation
from apache_beam.runners.laser.laser_operations import ShuffleWriteOperation
from apache_beam.runners.laser.laser_operations import LaserSideInputSource
from apache_beam.runners.laser.lexicographic import LexicographicPosition
from apache_beam.runners.laser.lexicographic import LexicographicRange
from apache_beam.runners.laser.lexicographic import lexicographic_cmp
from apache_beam.runners.laser.optimizer import generate_execution_graph
from apache_beam.runners.laser.shuffle import SimpleShuffleWorker
from apache_beam.runners.laser.work_manager import WorkManager
from apache_beam.runners.laser.worker import LaserWorker
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.io import iobase
from apache_beam.io import ReadFromText
from apache_beam import pvalue
from apache_beam.runners.worker import operation_specs
# from apache_beam.runners.worker import operations

class WatermarkManager(object):
  def __init__(self):
    self.tracked_nodes = set()
    self.root_nodes = set()

  def track_nodes(self, watermark_node):
    start_node = watermark_node
    if isinstance(watermark_node, CompositeWatermarkNode):
      print 'track_nodes GOT COMPOSITE NODE', watermark_node
      start_node = watermark_node.input_watermark_node
    if start_node in self.tracked_nodes:
      return
    self.tracked_nodes.add(start_node)
    self.root_nodes.add(start_node)
    queue = Queue()
    queue.put(start_node)
    while not queue.empty():
      current = queue.get()
      print 'TRACK NODE', current, 'PARENTS', current.watermark_parents, 'CHILDREN', current.watermark_children
      for dependent in current.watermark_children:
        if dependent in self.tracked_nodes:
          self.root_nodes.discard(dependent)
        else:
          self.tracked_nodes.add(dependent)
          queue.put(dependent)

  def start(self):
    for node in self.root_nodes:
      node._refresh_input_watermark()







class Executor(object):
  def __init__(self, execution_graph, execution_context):
    self.execution_graph = execution_graph
    self.execution_context = execution_context

  def run(self):
    print 'EXECUTOR RUN'
    print 'execution graph', self.execution_graph
    self.execution_context.work_manager.start()
    for fused_stage in self.execution_graph.stages:
      fused_stage.initialize()

    self.execution_context.watermark_manager.start()



class ExecutionContext(object):
  def __init__(self, work_manager, watermark_manager, shuffle_interface):
    self.work_manager = work_manager
    self.watermark_manager = watermark_manager
    self.shuffle_interface = shuffle_interface





class LaserRunner(PipelineRunner):
  """Executes a pipeline using multiple processes on the local machine."""

  def __init__(self):
    self.step_graph = StepGraph()

  def run_Read(self, transform_node):
    self._run_read_from(transform_node, transform_node.transform.source)

  def _run_read_from(self, transform_node, source_input):
    """Used when this operation is the result of reading source."""
    if not isinstance(source_input, NativeSource):
      source_bundle = iobase.SourceBundle(1.0, source_input, None, None)
    else:
      source_bundle = source_input
    print 'source', source_bundle
    # print 'split off', list(source_bundle.source.split(1))
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)

    # step_info = StepInfo(None, transform_node.outputs)
    step = ReadStep(transform_node.full_label, source_bundle, element_coder)
    # print 'transform_node', transform_node.outputs[None].producer
    self.step_graph.add_step(transform_node, step)
    print 'READ STEP', step

    # read_op = operation_specs.WorkerRead(source, output_coders=[element_coder])
    # print 'READ OP', read_op
    # self.outputs[output] = len(self.map_tasks), 0, 0
    # self.map_tasks.append([(transform_node.full_label, read_op)])
    # return len(self.map_tasks) - 1

  def _get_coder(self, pvalue, windowed=True):
    # TODO(robertwb): This should be an attribute of the pvalue itself.
    return DataflowRunner._get_coder(
        pvalue.element_type or typehints.Any,
        pvalue.windowing.windowfn.get_window_coder() if windowed else None)

  # def apply__GroupByKeyOnly(self, transform, pcoll):
  #   return pvalue.PCollection(pcoll.pipeline)

  def run__GroupByKeyOnly(self, transform_node):
    output = transform_node.outputs[None]
    output_coder = self._get_coder(output)
    input_coder = self._get_coder(transform_node.inputs[0])
    unwindowed_input_coder = input_coder.wrapped_value_coder
    print 'GBK INPUT WINDOWED_CODER', self._get_coder(transform_node.inputs[0])
    shuffle_coder = coders.WindowedValueCoder(
        coders.TupleCoder(
            [unwindowed_input_coder.key_coder(), coders.WindowedValueCoder(unwindowed_input_coder.value_coder(),
                input_coder.window_coder
                                      )]), coders.GlobalWindowCoder())

    step = GroupByKeyOnlyStep(transform_node.full_label, shuffle_coder, output_coder)
    print 'GBKO STEP', step
    self.step_graph.add_step(transform_node, step)

  def run_Flatten(self, transform_node):
    if transform_node.inputs:
      element_coder = self._get_coder(transform_node.inputs[0])
    else:
      element_coder = self._get_coder(transform_node.outputs[None])
    step = FlattenStep(transform_node.full_label, element_coder)
    self.step_graph.add_step(transform_node, step)




    # input_tag = transform_node.inputs[0].tag
    # input_step = self._cache.get_pvalue(transform_node.inputs[0])
    # step = self._add_step(
    #     TransformNames.GROUP, transform_node.full_label, transform_node)
    # step.add_property(
    #     PropertyNames.PARALLEL_INPUT,
    #     {'@type': 'OutputReference',
    #      PropertyNames.STEP_NAME: input_step.proto.name,
    #      PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    # step.encoding = self._get_encoded_output_coder(transform_node)
    # step.add_property(
    #     PropertyNames.OUTPUT_INFO,
    #     [{PropertyNames.USER_NAME: (
    #         '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
    #       PropertyNames.ENCODING: step.encoding,
    #       PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])
    # windowing = transform_node.transform.get_windowing(
    #     transform_node.inputs)
    # step.add_property(
    #     PropertyNames.SERIALIZED_FN,
    #     self.serialize_windowing_strategy(windowing))


  def run_ParDo(self, transform_node):
    transform = transform_node.transform
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)

    # Conform to the _pardo_fn_data behavior.
    side_input_labels = {}
    for i, side_input in enumerate(transform_node.side_inputs):
      side_input_labels[side_input] = str(i)

    pardo_fn_data = ParDoFnData(*DataflowRunner._pardo_fn_data(
            transform_node,
            lambda side_input: side_input_labels[side_input]))  # TODO
    print 'output_tags', transform.output_tags  # TODO once we have multiple outputs or whatever
    print 'pardo_fn_data', pardo_fn_data
    print 'node', transform_node
    print 'input node', transform_node.inputs[0]
    print 'OUTPUT TAGS', transform.output_tags
    step = ParDoStep(transform_node.full_label, pardo_fn_data, element_coder, transform.output_tags)
    print 'PARDO STEP', step

    self.step_graph.add_step(transform_node, step)

  def run(self, pipeline):
    # Visit the pipeline and build up the step graph.
    super(LaserRunner, self).run(pipeline)

    print 'COMPLETEd step graph', self.step_graph
    for step in self.step_graph.steps:
      print step.inputs, step.outputs

    shuffle_worker = SimpleShuffleWorker()
    set_channel_config(ChannelConfig(
      node_addresses=['master'],
      link_strategies=[]))
    channel_manager = get_channel_manager()
    channel_manager.register_interface('master/shuffle', shuffle_worker)

    node_manager = InProcessComputeNodeManager()
    # node_manager = MultiProcessComputeNodeManager()
    node_manager.start()
    work_manager = WorkManager(node_manager)
    watermark_manager = WatermarkManager()
    execution_context = ExecutionContext(work_manager, watermark_manager, shuffle_worker)
    print 'GENERATING EXECUTION GRAPH'
    execution_graph = generate_execution_graph(self.step_graph, execution_context)

    print 'EXECUTION GRAPH', execution_graph
    print execution_graph.stages
    for stage in execution_graph.stages:
      execution_context.watermark_manager.track_nodes(stage)
    print 'ROOT NODES', execution_context.watermark_manager.root_nodes
    executor = Executor(execution_graph, execution_context)
    executor.run()




# class CoordinatorInterface(Interface):

#   @remote_method(int, str)
#   def register_worker(host_id, worker_id):
#     raise NotImplementedError()

#   @remote_method(str, returns=str)
#   def ping(self, body):
#     raise NotImplementedError()

# class LaserCoordinator(threading.Thread, CoordinatorInterface):

#   def __init__(self):
#     self.channel_manager = get_channel_manager()
#     self.worker_channels = {}
#     super(LaserCoordinator, self).__init__()

#   def run(self):
#     print 'RUN'
#     while True:
#       for worker_id in self.worker_channels:
#         start_time = time.time()
#         for i in range(100):
#           self.worker_channels[worker_id].ping()
#         end_time = time.time()
#         print 'WORKER PING', worker_id, (end_time - start_time) / 100
#       time.sleep(1)


#   def register_worker(self, host_id, worker_id):
#     print 'REGISTERED', host_id, worker_id
#     # self.worker_channels[worker_id] = self.channel_manager.get_interface(HostDescriptor(host_id), 'worker', WorkerInterface)


#   def ping(self, body):
#     # print 'LaserCoordinator PINGED', body
#     return 'PINGED_%r' % body











def run(argv):
  if '--worker' in argv:
    worker = LaserWorker(json.loads(argv[-1]))
    worker.run()
    sys.exit(0)

  COORDINATOR_PORT = random.randint(20000,30000)
  # set_channel_config(ChannelConfig(
  #   node_addresses=['master'],
  #   link_strategies=[
  #     LinkStrategy(
  #       LinkStrategyType.LISTEN,
  #       mode=LinkMode.TCP,
  #       address='localhost',
  #       port=COORDINATOR_PORT,
  #       # mode=LinkMode.UNIX,
  #       # address='./uds_socket3-%s' % COORDINATOR_PORT,
  #       )]))
  manager = get_channel_manager()

  coordinator = LaserCoordinator()
  manager.register_interface('master/coordinator', coordinator)
  coordinator.start()

  print 'spawning worker'
  spawn_worker({
    'id': 'worker1',
    'channel_config': ChannelConfig(
    node_addresses=['worker1'],
    anycast_aliases = {'worker[any]': 'worker1'},
    link_strategies=[
      LinkStrategy(
        LinkStrategyType.CONNECT,
        mode=LinkMode.TCP,
        address='localhost',
        port=COORDINATOR_PORT,
        )]).to_dict(),
    })
  print 'DONE'
  # spawn_worker({
  #   'id': 102,
  #   'coordinator_host_id': manager.host_descriptor.host_id,
  #   'channel_config': {
  #     'host_id': 102,  # TODO: redundant
  #     'connect': True,
  #     'connect_mode': LinkMode.UNIX,
  #     'connect_address': './uds_socket3-%s' % COORDINATOR_PORT,
  #   }})
  # print 'DONE'
  time.sleep(10)

if __name__ == '__main__':
  if '--worker' in sys.argv:
    config_dict = json.loads(sys.argv[-1])
    set_channel_config(ChannelConfig.from_dict(config_dict['channel_config']))
    worker = LaserWorker(config_dict)
    worker.run()
    sys.exit(0)
  logging.getLogger().setLevel(logging.DEBUG)
  set_channel_config(ChannelConfig(
    node_addresses=['master']))
  # InProcessComputeNodeManager().start()
  # raise ''
  # run(sys.argv)
  from apache_beam import Pipeline
  from apache_beam import Create
  from apache_beam import Flatten
  from apache_beam import DoFn
  from apache_beam.io import WriteToText
  from apache_beam.testing.util import assert_that, equal_to

  #### WORDCOUNT SECTION
  p = Pipeline(runner=LaserRunner())
  def _print(x):
    import time
    # time.sleep(4)
    print 'PRRRINT:', x
  # lines = p | ReadFromText('gs://dataflow-samples/shakespeare/*.txt')
  # lines = p | ReadFromText('shakespeare/*.txt')
  # lines = p | ReadFromText('data/*.txt')
  lines1 = p | "lines1" >> Create(['a', 'a', 'b' ,'a c'])
  lines2 = p | "lines2" >> Create(['D', 'eee', 'FFFFFfff' ,'gggGGGGGGGGGGGG'])
  # lines1 | WriteToText('mytmp')
  # assert_that(lines1, equal_to([1, 2]))
  lines = (lines1, lines2) | Flatten()
  # # WORDCAP:
  lines | beam.Map(lambda x: x.upper()) | beam.Map(_print)

  # # WORDCOUNT:
  # paired = (lines
  #           | 'split' >> beam.FlatMap(lambda text_line: __import__('re').findall(r'[A-Za-z\']+', text_line))
  #                         .with_output_types(unicode)
  #           | 'pair_with_one' >> beam.Map(lambda x: (x, 1)))
  #           # | 'group' >> beam.GroupByKey()
  #           # | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
  # paired | beam.Map(_print)

  # #### BIGSHUFFLE SECTION
  # p = Pipeline(runner=LaserRunner())
  # # lines = p | ReadFromText('gs://sort_g/input/ascii_sort_1GB_input.0000999', coder=beam.coders.BytesCoder())
  # lines = p | ReadFromText('./ascii_sort_1GB_input.0000999', coder=beam.coders.BytesCoder())
  # output = (lines
  #           | 'split' >> beam.Map(
  #               lambda x: (x[:10], x[10:99]))
  #           .with_output_types(beam.typehints.KV[str, str])
  #           | 'group' >> beam.GroupByKey()
  #           | 'format' >> beam.FlatMap(
  #               lambda (key, vals): ['%s%s' % (key, val) for val in vals]))

  p.run()#.wait_until_finish()



