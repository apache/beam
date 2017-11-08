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
import json
import logging
import multiprocessing
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
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.io import iobase
from apache_beam import pvalue
from apache_beam.pvalue import PBegin
from apache_beam.runners.worker import operation_specs
# from apache_beam.runners.worker import operations

class StepGraph(object):
  def __init__(self):
    self.steps = []  # set?

    # The below are used for conevenience in construction of the StepGraph, but
    # not by any subsequent logic.
    self.transform_node_to_step = {}
    self.pcollection_to_node = {}

  # def register_origin_transform_node()
  def add_step(self, transform_node, step):
    self.steps.append(step)
    self.transform_node_to_step[transform_node] = step
    print 'TRANSFORM_NODE', transform_node
    print 'inputs', transform_node.inputs
    inputs = []
    for input_pcollection in transform_node.inputs:
      if isinstance(input_pcollection, PBegin):
        continue
      assert input_pcollection in self.pcollection_to_node
      pcollection_node = self.pcollection_to_node[input_pcollection]
      pcollection_node.add_consumer(step)
      inputs.append(pcollection_node)
    step.inputs = inputs
    print 'YO inputs', inputs

    # TODO: side inputs in this graph.

    outputs = {}
    for tag, output_pcollection in transform_node.outputs.iteritems():
      print 'TAG', tag, output_pcollection
      # TODO: do we want to associate system names here or somewhere?  might be useful for association with monitoring and such.
      pcollection_node = PCollectionNode(step, tag)
      self.pcollection_to_node[output_pcollection] = pcollection_node
      outputs[tag] = pcollection_node
    step.outputs = outputs
    print 'YO outputs', outputs
    # if original_node:  # original_transform_node?
    #   self.transform_node_to_step[original_node] = step
    # if input_step:  # TODO: should this be main_input?
    #   step._add_input(input_step)
    #   input_step._add_output(step)

  def get_step_from_node(self, transform_node):
    return self.transform_node_to_step[transform_node]

  def __repr__(self):
    return 'StepGraph(steps=%s)' % self.steps


class PCollectionNode(object):
  def __init__(self, step, tag):
    self.step = step
    self.tag = tag
    self.consumers = []

  def add_consumer(self, consumer_step):
    self.consumers.append(consumer_step)

  def __repr__(self):
    return 'Step[%s.%s]' % (self.step.name, self.tag)



# class StepInfo(object):
#   def __init__(self, input_pcollection, output_pcollections):
#     self.input_pcollection = input_pcollection
#     self.output_pcollections = output_pcollections

class Step(object):
  def __init__(self, name):
    self.name = name
    self.inputs = []  # Should have one element except in case of Combine
    # self.side_input_steps
    self.outputs = {}

  def _add_input(self, input_step):
    assert isinstance(input_step, Step)
    self.inputs.append(input_step)

  def _add_output(self, output_step):  # add_consumer? what happens with named outputs? do we care?
    assert isinstance(output_step, Step)
    self.outputs.append(output_step)

  def __repr__(self):
    return 'Step(%s, coder: %s)' % (self.name, getattr(self, 'element_coder', None))

class ReadStep(Step):
  def __init__(self, name, source_bundle, element_coder):
    super(ReadStep, self).__init__(name)
    self.original_source_bundle = source_bundle
    self.element_coder = element_coder


class ParDoFnData(object):
  def __init__(self, fn, args, kwargs, si_tags_and_types, windowing):
    self.fn = fn
    self.args = args
    self.kwargs = kwargs
    self.si_tags_and_types = si_tags_and_types
    self.windowing = windowing

  def __repr__(self):
    return 'ParDoFnData(fn: %s, args: %s, kwargs: %s, si_tags_and_types: %s, windowing: %s)' % (
        self.fn, self.args, self.kwargs, self.si_tags_and_types, self.windowing
      )


class ParDoStep(Step):
  def __init__(self, name, pardo_fn_data, element_coder):
    super(ParDoStep, self).__init__(name)
    self.pardo_fn_data = pardo_fn_data
    self.element_coder = element_coder

class GroupByKeyStep(Step):
  def __init__(self, name, element_coder):
    super(GroupByKeyStep, self).__init__(name)
    self.element_coder = element_coder



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
    print 'split off', list(source_bundle.source.split(1))
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

  def apply_GroupByKey(self, transform, pcoll):
    return pvalue.PCollection(pcoll.pipeline)

  def run_GroupByKey(self, transform_node):
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)
    step = GroupByKeyStep(transform_node.full_label, element_coder)
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
    pardo_fn_data = ParDoFnData(*DataflowRunner._pardo_fn_data(
            transform_node,
            lambda side_input: self.side_input_labels[side_input]))  # TODO
    print 'output_tags', transform.output_tags  # TODO once we have multiple outputs or whatever
    print 'pardo_fn_data', pardo_fn_data
    print 'node', transform_node
    print 'input node', transform_node.inputs[0]
    step = ParDoStep(transform_node.full_label, pardo_fn_data, element_coder)
    print 'PARDO STEP', step

    self.step_graph.add_step(transform_node, step)

  def run(self, pipeline):
    # Visit the pipeline and build up the step graph.
    super(LaserRunner, self).run(pipeline)
    print 'COMPLETEd step graph', self.step_graph
    for step in self.step_graph.steps:
      print step.inputs, step.outputs




class CoordinatorInterface(Interface):

  @remote_method(int, str)
  def register_worker(host_id, worker_id):
    raise NotImplementedError()

  @remote_method(str, returns=str)
  def ping(self, body):
    raise NotImplementedError()

class LaserCoordinator(threading.Thread, CoordinatorInterface):

  def __init__(self):
    self.channel_manager = get_channel_manager()
    self.worker_channels = {}
    super(LaserCoordinator, self).__init__()

  def run(self):
    print 'RUN'
    while True:
      for worker_id in self.worker_channels:
        start_time = time.time()
        for i in range(100):
          self.worker_channels[worker_id].ping()
        end_time = time.time()
        print 'WORKER PING', worker_id, (end_time - start_time) / 100
      time.sleep(1)


  def register_worker(self, host_id, worker_id):
    print 'REGISTERED', host_id, worker_id
    # self.worker_channels[worker_id] = self.channel_manager.get_interface(HostDescriptor(host_id), 'worker', WorkerInterface)


  def ping(self, body):
    # print 'LaserCoordinator PINGED', body
    return 'PINGED_%r' % body

class WorkerInterface(Interface):

  @remote_method(returns=str)
  def ping(self):
    raise NotImplementedError()


class LaserWorker(WorkerInterface):
  def __init__(self, options):
    self.options = options

  def ping(self):
    return 'OK'

  def run(self):
    worker_id = self.options['id']
    set_channel_config(ChannelConfig.from_dict(self.options['channel_config']))
    manager = get_channel_manager()
    manager.register_interface('%s/worker' % worker_id, self)

    coordinator = manager.get_interface('master/coordinator', CoordinatorInterface)
    while True:
      try:
        print 'START REGISTER'
        coordinator.register_worker(self.options['id'], worker_id)
        print 'REGISTER OK'
        break
      except Exception as e:
        print 'e', e
        time.sleep(2)

    while True:
      print 'RESUTLED!!!', coordinator.ping('wtf')
      time.sleep(2)
    sys.exit(1)


class ComputeNodeManagerInterface(Interface):
  pass

class ComputeNodeManager(ComputeNodeManagerInterface):
  def supports_scaling(self):
    return False

  def _check_started(self):
    if not self.started:
      raise Exception('Node manager not started.')

class ComputeNodeHandle(object):
  def __init__(self, manager, name, core_count):
    self.manager = manager
    self.name = name
    self.core_count = core_count

  def get_node_interface(self):
    pass


class ComputeNodeInterface(Interface):
  pass

class ComputeNode(ComputeNodeInterface):
  def __init__(self, node_config):
    # TODO: node_config should be a class or something, not just a dict
    #self.config = node_config
    # TODO: id?
    self.name = node_config['name']
    self.channel_manager = get_channel_manager()
    self.node_manager_interface = self.channel_manager.get_interface()


  def start(self):
    self.node_manager_interface.report_node_started(self.name)


class InProcessComputeNodeManager(ComputeNodeManager):

  def __init__(self, num_nodes=1):
    self.num_nodes = num_nodes
    self.started = False


  def start(self):
    for i in range(self.num_nodes):
      pass
    self.started = True


  def get_nodes(self):
    self._check_started()
    return []




def spawn_worker(options):
  p = subprocess.Popen(['python', '-m', 'apache_beam.runners.laser.laser_runner', '--worker', json.dumps(options)])




def run(argv):
  if '--worker' in argv:
    worker = LaserWorker(json.loads(argv[-1]))
    worker.run()
    sys.exit(0)

  COORDINATOR_PORT = random.randint(20000,30000)
  set_channel_config(ChannelConfig(
    node_addresses=['master'],
    link_strategies=[
      LinkStrategy(
        LinkStrategyType.LISTEN,
        mode=LinkMode.TCP,
        address='localhost',
        port=COORDINATOR_PORT,
        # mode=LinkMode.UNIX,
        # address='./uds_socket3-%s' % COORDINATOR_PORT,
        )]))
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
        # mode=LinkMode.UNIX,
        # address='./uds_socket3-%s' % COORDINATOR_PORT,
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
  logging.getLogger().setLevel(logging.DEBUG)
  # run(sys.argv)
  from apache_beam import Pipeline
  from apache_beam import Create
  from apache_beam import DoFn
  p = Pipeline(runner=LaserRunner())
  # def fn(input):
  #   print input
  p | Create([1, 2, 3]) | beam.Map(lambda x: (x, '1')) | beam.GroupByKey()
  # | beam.Map(fn)
  p.run()

