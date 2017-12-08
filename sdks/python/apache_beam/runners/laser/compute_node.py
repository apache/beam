
import json
import random
import subprocess
import threading
import time

from apache_beam.runners.laser.channels import ChannelConfig
from apache_beam.runners.laser.channels import Interface
from apache_beam.runners.laser.channels import LinkMode
from apache_beam.runners.laser.channels import LinkStrategy
from apache_beam.runners.laser.channels import LinkStrategyType
from apache_beam.runners.laser.channels import remote_method
from apache_beam.runners.laser.channels import get_channel_manager
from apache_beam.runners.laser.worker import LaserWorker



class ComputeNodeManagerInterface(Interface):
  pass

class ComputeNodeManager(ComputeNodeManagerInterface):
  # def supports_scaling(self):
  #   return False

  def _check_started(self):
    if not self.started:
      raise Exception('Node manager not started.')
  @remote_method(str, returns=str)
  def report_node_started(self, node_name):
    raise NotImplementedError()

# class ComputeNodeHandle(object):
#   def __init__(self, manager, name, core_count):
#     self.manager = manager
#     self.name = name
#     self.core_count = core_count

#   def get_node_interface(self):
#     pass


class ComputeNodeStubInterface(Interface):

  @remote_method(str)
  def confirm(self, message):
    raise NotImplementedError

class ComputeNodeStub(ComputeNodeStubInterface):
  def __init__(self, node_config):
    # TODO: node_config should be a class or something, not just a dict
    self.config = node_config
    # TODO: id?
    self.name = node_config['name']
    self.channel_manager = get_channel_manager()
    self.channel_manager.register_interface('%s/stub' % self.name, self)
    self.node_manager = self.channel_manager.get_interface('master/node_manager', ComputeNodeManagerInterface)
    self.worker = None


  def start(self):
    time.sleep(0.1)  # TODO: wait for node manager / link to be ready.
    self.node_manager.report_node_started(self.name)

  # REMOTE METHOD
  def start_worker(self):
    # TODO: maybe in the future this will be another process
    self.worker = LaserWorker(self.config)
    self.worker.start()
    return 'wtfwtfwtf'


  def confirm(self, message):
    print 'CONFIRMed', self.name, message


class InProcessComputeNodeManager(ComputeNodeManager):

  def __init__(self, num_nodes=1):
    self.num_nodes = num_nodes
    self.started = False
    self.channel_manager = get_channel_manager()
    self.channel_manager.register_interface('master/node_manager', self)

    self.node_stubs = {}


  def start(self):
    for i in range(self.num_nodes):
      worker_name = 'worker%d' % i
      self.channel_manager.register_node_address(worker_name)
      node_stub = ComputeNodeStub({'name': worker_name})
      threading.Thread(target=node_stub.start).start()
      self.node_stubs[worker_name] = node_stub
    self.started = True

  def report_node_started(self, node_name):
    # TODO: assert node name is correct
    # TODO: what happens if node restarts?
    print 'NODE STARTED', node_name
    node_stub = self.channel_manager.get_interface('%s/stub' % node_name, ComputeNodeStubInterface)
    node_stub.confirm('yay' + str(self))
    return 'HI[%s]' % node_name


  def get_nodes(self):
    self._check_started()
    return list(self.node_stubs.values())


def spawn_worker(options):
  p = subprocess.Popen(['python', '-m', 'apache_beam.runners.laser.laser_runner', '--worker', json.dumps(options)])

class MultiProcessComputeNodeManager(ComputeNodeManager):
  def __init__(self, num_nodes=8):
    self.num_nodes = num_nodes
    self.started = False
    self.channel_manager = get_channel_manager()
    self.channel_manager.register_interface('master/node_manager', self)

    self.node_stubs = {}

  def start(self):
    coordinator_port = random.randint(20000,30000)
    self.channel_manager.add_link_strategy(
        LinkStrategy(
            LinkStrategyType.LISTEN,
            mode=LinkMode.TCP,
            address='localhost',
            port=coordinator_port,
        ))
    for i in range(self.num_nodes):
      worker_name = 'worker%d' % i
      print 'SPAWN', worker_name
      spawn_worker({
          'name': worker_name,
          'channel_config': ChannelConfig(
              node_addresses=[worker_name],
              link_strategies=[
                LinkStrategy(
                  LinkStrategyType.CONNECT,
                  mode=LinkMode.TCP,
                  address='localhost',
                  port=coordinator_port,
                  )]).to_dict(),
      })
    time.sleep(2)  # HACK
    print 'DONE STARTING'
    self.started = True

  def report_node_started(self, node_name):  # TODO: HACK / C+P
    # TODO: assert node name is correct
    # TODO: what happens if node restarts?
    print 'NODE STARTED', node_name
    node_stub = self.channel_manager.get_interface('%s/stub' % node_name, ComputeNodeStubInterface)
    node_stub.confirm('yay' + str(self))
    return 'HI[%s]' % node_name


  def get_nodes(self):
    self._check_started()
    return list(self.node_stubs.values())
