

import copy
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


class InterfaceNotReadyException(Exception):
  pass

class ChannelManager(threading.Thread):
  def __init__(self):
    self.node_addresses = set() # node_address -> node_name?
    self.anycast_aliases = {}
    self.address_generation = 0
    self.registered_interfaces = {}
    self.links = []
    self.addresses_to_link_indices = {}
    self.links_to_indices = {}
    self.links_to_generations = {}

    self.lock = threading.Lock()
    super(ChannelManager, self).__init__()
    self.daemon = True

  def register_node_address(self, node_address):
    with self.lock:
      self.node_addresses.add(node_address)
      self.address_generation += 1

  def register_anycast_alias(self, node_address, anycast_address):
    # TODO: allow multiple alias targets.
    with self.lock:
      self.node_addresses.add(anycast_address)
      self.anycast_aliases[anycast_address] = node_address
      self.address_generation += 1

  def register_interface(self, interface_address, interface):
    assert isinstance(interface, Interface)
    print 'REGISTER_INTERFACE', interface_address, interface
    node_address, interface_name = interface_address.rsplit('/', 1)
    with self.lock:
      assert node_address in self.node_addresses
      self.registered_interfaces[interface_address] = interface
  
  def _get_remote_interface(self, interface_address, interface_cls):
    class RemoteInterface(interface_cls):
      pass
    interface_obj = RemoteInterface()
    for name in dir(interface_obj):
      prop = getattr(interface_obj, name)
      if isinstance(prop, _RemoteMethodStub):
        def remote_call(stub):
          def _inner(*args):
            return self._send_call(interface_address, stub.name, args)
          return _inner
        setattr(interface_obj, name, remote_call(prop))
    return interface_obj

  def _send_call(self, interface_address, method_name, args):
    payload = pickle.dumps((interface_address, method_name, args))
    node_address, interface_name = interface_address.rsplit('/', 1)
    with self.lock:
      if node_address not in self.addresses_to_link_indices:
        raise InterfaceNotReadyException()
      link_index = self.addresses_to_link_indices[node_address]
      link = self.links[link_index]
    result = link.send_message(payload)
    print 'send_call(%s, %s) returned' % (interface_address, method_name)
    return result

  def get_interface(self, interface_address, interface_cls):
    node_address, interface_name = interface_address.rsplit('/', 1)
    with self.lock:
      if node_address in self.anycast_aliases:
        node_address = self.anycast_aliases[node_address]
        assert node_address in self.node_addresses
        interface_address = '%s/%s' % (node_address, interface_name)
      if node_address in self.node_addresses:
        if not interface_address in self.registered_interfaces:
          raise InterfaceNotReadyException()
        # This directly returns the bound interface class if the interface is
        # local.
        return self.registered_interfaces[interface_address]
    return self._get_remote_interface(interface_address, interface_cls)

  def _call_local(self, interface_name, method_stub_name, args):
    # print self.host_descriptor, 'CALL_LOCAL', method_stub_name, args
    with self.lock:
      interface = self.registered_interfaces[interface_name]
    return getattr(interface, method_stub_name)(*args)

  def register_link(self, link):
    with self.lock:
      self.links.append(link)
      self.links_to_indices[link] = len(self.links) - 1

  def receive_address_info(self, link, node_addresses, anycast_aliases):
    with self.lock:
      index = self.links_to_indices[link]
      for node_address in node_addresses:
        self.addresses_to_link_indices[str(node_address)] = index # TODO: don't use json
      for from_address, unused_to_address in anycast_aliases.iteritems():
        self.addresses_to_link_indices[str(from_address)] = index # TODO: don't use json

  def run(self):
    # The manager thread does periodic tasks.  Each link is responsible for handling incomming
    # traffic and calling the manager as necessary.
    # Right now, 
    while True:
      links_to_update = []
      with self.lock:
        for link in self.links:
          if self.links_to_generations.get(link, -1) < self.address_generation:
            links_to_update.append(link)
            self.links_to_generations[link] = self.address_generation
        if links_to_update:
          node_addresses = copy.copy(self.node_addresses)
          anycast_aliases = copy.copy(self.anycast_aliases)

      for link in links_to_update:
        print 'SEND UPDATE', link, node_addresses, anycast_aliases
        link.send_address_info(node_addresses, anycast_aliases)
      time.sleep(1)

  def add_link_strategy(self, link_strategy):
    if link_strategy.strategy_type == LinkStrategyType.LISTEN:
      listener = LinkListener(self, link_strategy)
      listener.start()
    elif link_strategy.strategy_type == LinkStrategyType.CONNECT:
      connecter = LinkConnecter(self, link_strategy)
      connecter.start()
    else:
      raise ValueError('Invalid link strategy type: %s.' % link_strategy.strategy_type)
    


def get_remote_interface(host_descriptor, channel_manager, interface_name, interface_cls):
  class RemoteInterface(interface_cls):
    pass
  interface_obj = RemoteInterface()
  # print interface_cls, 'dict', dir(interface_obj), interface_obj.ping
  for name in dir(interface_obj):
    prop = getattr(interface_obj, name)
    if isinstance(prop, _RemoteMethodStub):
      def _remote_call(stub):
        def _inner(*args):
          # print host_descriptor, 'REMOTE CALL', stub, args
          return channel_manager._send_call(host_descriptor, interface_name, stub, args)
        return _inner
      setattr(interface_obj, name, _remote_call(prop))
  # print 'REMOTE OBJ', interface_obj
  return interface_obj


TAG_MESSAGE = 1
TAG_MESSAGE_ACK = 2
TAG_MESSAGE_RESULT = 3
TAG_ADDRESS_INFO = 4

class Channel(object):  # TODO: basechannel?
  def __init__(self, channel_manager, pipe):
    self.channel_manager = channel_manager
    self.pipe = pipe
    self.seq = 0
    self.lock = threading.Lock()
    self.ack_conds = {}
    self.result_conds = {}
    self.results = {}

  def start(self):
    rt = threading.Thread(target=self.recv_thread)
    rt.daemon = True
    rt.start()

  def send_address_info(self, node_addresses, anycast_aliases):
    serialized = json.dumps({
        'node_addresses': list(node_addresses),
        'anycast_aliases': anycast_aliases
      })
    with self.lock:
      self.pipe.send(struct.pack('<ii', TAG_ADDRESS_INFO, len(serialized)))
      self.pipe.send(serialized)
    # TODO: should we wait for ack?  nah might be unnecessary

  def send_message(self, message_bytes, wait_for_ack=True, wait_for_result=True):
    assert isinstance(message_bytes, str)
    message_id = self.seq
    # TODO: long longs?
    start_time = time.time()
    with self.lock:
      self.seq += 1
      self.pipe.send(struct.pack('<iii', TAG_MESSAGE, message_id, len(message_bytes)))
      self.pipe.send(message_bytes)
      elapsed_time = time.time() - start_time
      print 'sent %d bytes in %fs' % (len(message_bytes), elapsed_time)
      if wait_for_ack:
        self.ack_conds[message_id] = threading.Condition(self.lock)
        self.ack_conds[message_id].wait()
      if wait_for_result:
        # print 'WAIT FOR RESULT...'
        if message_id not in self.results:
          self.result_conds[message_id] = threading.Condition(self.lock)
          self.result_conds[message_id].wait()
          # print 'GOT RESULT...', self.results[message_id]
          elapsed_time = time.time() - start_time
          print 'got result in %fs' % (elapsed_time)
        return self.results[message_id]
    # return message_id

  def recv_thread(self):
    while True:
      # print 'RECV TAG'
      a = self.pipe.recv(4)
      print 'RECV', repr(a), 'ME', get_channel_manager().node_addresses
      tag, = struct.unpack('<i', a)
      if tag == TAG_MESSAGE:
        # print 'RECEIVED MESSAGE'
        message_id, = struct.unpack('<i', self.pipe.recv(4))
        message_len, = struct.unpack('<i', self.pipe.recv(4))
        # print 'MESSAGE LEN', message_len
        message_bytes = self.pipe.recv(message_len)
        while len(message_bytes) < message_len:
          message_bytes += self.pipe.recv(message_len - len(message_bytes))
        with self.lock:
          self.pipe.send(struct.pack('<ii', TAG_MESSAGE_ACK, message_id))
        return_val = self._process_received_message(message_id, message_bytes)
        return_bytes = pickle.dumps(return_val)
        with self.lock:
          self.pipe.send(struct.pack('<iii', TAG_MESSAGE_RESULT, message_id, len(return_bytes)))
          self.pipe.send(return_bytes)
      elif tag == TAG_MESSAGE_ACK:
        message_id, = struct.unpack('<i', self.pipe.recv(4))
        # print 'RECEIVED MESSAGE ACK', message_id
        with self.lock:
          cond = self.ack_conds.get(message_id)
          if cond:
            cond.notify()
      elif tag == TAG_MESSAGE_RESULT:
        message_id, = struct.unpack('<i', self.pipe.recv(4))
        message_len, = struct.unpack('<i', self.pipe.recv(4))
        # print 'RECEIVED MESSAGE RESULT', message_id
        message_bytes = ''
        while len(message_bytes) < message_len:
          message_bytes += self.pipe.recv(message_len - len(message_bytes))
        unpickled = pickle.loads(message_bytes)
        with self.lock:
          self.results[message_id] = unpickled
          cond = self.result_conds.get(message_id)
          if cond:
            cond.notify()
          else:
            print 'WARNING: NO RESUTL COND', message_id
      elif tag == TAG_ADDRESS_INFO:
        # message_id, = struct.unpack('<i', self.pipe.recv(4))
        serialized_len, = struct.unpack('<i', self.pipe.recv(4))
        serialized = self.pipe.recv(serialized_len)
        print 'SERIALIZED', serialized
        data = json.loads(serialized)
        self.channel_manager.receive_address_info(self, data['node_addresses'], data['anycast_aliases'])
      else:
        # print repr((TAG_MESSAGE, TAG_MESSAGE_ACK)), tag, tag == TAG_MESSAGE
        raise Exception('UNKNOWN TAG %r' % tag)



  def _process_received_message(self, message_id, message_bytes):
    # print 'RECCEIVED', message_id, repr(message_bytes)
    interface_name, method_stub_name, args = pickle.loads(message_bytes)
    # print 'YO', method_stub_name, args
    return self.channel_manager._call_local(interface_name, method_stub_name, args)


class LoopbackPipe(object):
  def __init__(self):
    self.lock = threading.Lock()
    self.new_data_cond = threading.Condition(self.lock)
    self.buffer = ''

  def send(self, body):
    # print 'SEND', repr(body)
    with self.lock:
      self.buffer += body
      self.new_data_cond.notify()

  def recv(self, max_len):
    # print 'RECV WAIT', max_len
    with self.lock:
      while len(self.buffer) == 0:
        self.new_data_cond.wait()
      body = self.buffer[0:max_len]
      self.buffer = self.buffer[max_len:]
    # print 'RECV\'d', len(body), repr(body)
    return body



def loopback(s2):
  while True:
    a = s2.recv(2048)
    if a:
      s2.send(a)


class LinkMode:
  TCP = 'tcp'
  UNIX = 'unix'

class LinkStrategyType(object):
  LISTEN = 'listen'
  CONNECT = 'connect'

class LinkStrategy(object):

  def __init__(self, strategy_type, mode=LinkMode.TCP, address='localhost', port=-1):
    self.strategy_type = strategy_type
    self.mode = mode
    self.address = address
    self.port = port

class ChannelConfig(object):
  def __init__(self, node_addresses=None, anycast_aliases=None, link_strategies=None):
    self.node_addresses = node_addresses or []
    self.anycast_aliases = anycast_aliases or {}
    self.link_strategies = link_strategies or []

  @staticmethod
  def from_dict(data):
    link_strategies = pickle.loads(str(data['link_strategies']))
    return ChannelConfig(
      node_addresses=data['node_addresses'],
      anycast_aliases=data['anycast_aliases'],
      link_strategies=link_strategies,
      )

  def to_dict(self):
    return {
      'node_addresses': self.node_addresses,
      'anycast_aliases': self.anycast_aliases,
      'link_strategies': pickle.dumps(self.link_strategies),  # HACK
    }

def set_channel_config(config):
  assert isinstance(config, ChannelConfig)
  globals()['_channel_config'] = config

def get_channel_config():
  if '_channel_config' in globals():
    return globals()['_channel_config']
  return ChannelConfig()


def _initialize_pipe(pipe, my_host_id):
  pipe.send(struct.pack('<i', my_host_id))
  other_host_id, = struct.unpack('<i', pipe.recv(4))
  # print 'INITAILIZED: my_host_id', my_host_id, 'other', other_host_id
  return other_host_id


def get_channel_manager():
  if '_channel_manager' in globals():
    # TODO: there is probably some initialization race condition here if not called while single-threaded...
    return globals()['_channel_manager']
  # construct channel_manager
  config = get_channel_config()
  manager = ChannelManager()
  for node_address in config.node_addresses:
    manager.register_node_address(node_address)
  for alias_from, alias_to in config.anycast_aliases.iteritems():
    manager.register_anycast_alias(alias_to, alias_from)
  for link_strategy in config.link_strategies:
    print 'GOT LINK STRATEGY', link_strategy
    manager.add_link_strategy(link_strategy)
  globals()['_channel_manager'] = manager
  manager.start()
  return manager


class Interface(object):
  pass


class _RemoteMethodStub(object):
  def __init__(self, name, arg_types, return_type=None):
    self.name = name
    self.arg_types = arg_types
    self.return_type = return_type

def remote_method(*arg_types, **kvargs):
  return_type = kvargs.get('returns')
  def _inner(method):
    return _RemoteMethodStub(method.__name__, arg_types, return_type)
  return _inner

class LinkListener(threading.Thread):
  def __init__(self, channel_manager, link_strategy):
    self.channel_manager = channel_manager
    self.link_strategy = link_strategy
    super(LinkListener, self).__init__()
    self.daemon = True

  def run(self):
    print 'LinkListener STARTED'
    if self.link_strategy.mode == LinkMode.TCP:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.bind((self.link_strategy.address, self.link_strategy.port))
    elif self.link_strategy.mode == LinkMode.UNIX:
      s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      s.bind(self.link_strategy.address)
    else:
      raise ValueError('Invalid mode: %r.' % self.link_strategy.mode)
    s.listen(1)  # Number of queued connections. TODO: what does this actually do?
    while True:
      conn, addr = s.accept()
      # print 'accepted', conn, addr
      # other_host_id = _initialize_pipe(conn, self.channel_manager.host_descriptor.host_id)
      # print 'other_host_id', other_host_id
      # print 'LinkListener got new', conn, addr
      channel = Channel(self.channel_manager, conn)
      # print 'registered new channel for host id', other_host_id
      self.channel_manager.register_link(channel)
      channel.start()


class LinkConnecter(threading.Thread):
  def __init__(self, channel_manager, link_strategy):
    self.channel_manager = channel_manager
    self.link_strategy = link_strategy
    super(LinkConnecter, self).__init__()
    self.daemon = True


  def run(self):
    print 'LinkConnecter STARTED'
    if self.link_strategy.mode == LinkMode.TCP:
      pipe = socket.create_connection((self.link_strategy.address, self.link_strategy.port))
    elif self.link_strategy.mode == LinkMode.UNIX:
      pipe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
      pipe.connect(self.link_strategy.address)
    else:
      raise ValueError('Invalid mode: %r.' % self.link_strategy.mode)
    # perform initialization sequence:
    # other_host_id = _initialize_pipe(pipe, host_descriptor.host_id)
    channel = Channel(self.channel_manager, pipe)
    self.channel_manager.register_link(channel)
    channel.start()

## END RPC layer stuff
